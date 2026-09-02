#!/usr/bin/env python3
"""Operator-only signed route publisher. Never invoke from Milk Man."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile
import urllib.parse
import uuid


UTC_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")


def fail(message: str) -> "NoReturn":
    raise SystemExit(message)


def required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        fail(f"{name} is required")
    return value


def canonical(value: dict) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def candidate_base_url(name: str, raw: str | None) -> str | None:
    if raw is None:
        return None
    parsed = urllib.parse.urlsplit(raw)
    if (
        parsed.scheme not in ("http", "https")
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path.rstrip("/").endswith("/v1")
    ):
        fail(f"{name} must be an HTTP(S) provider base before the /v1 endpoint")
    return raw.rstrip("/")


def timestamp(raw: str) -> str:
    if not UTC_TIMESTAMP.fullmatch(raw):
        fail("timestamps must be UTC seconds in YYYY-MM-DDTHH:MM:SSZ form")
    try:
        dt.datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as error:
        fail(f"invalid timestamp: {error}")
    return raw


def now() -> str:
    return (
        dt.datetime.now(dt.timezone.utc)
        .replace(microsecond=0)
        .strftime("%Y-%m-%dT%H:%M:%SZ")
    )


def sign(key: Path, message: bytes) -> str:
    # macOS OpenSSL rejects Ed25519 one-shot signing from an unsized stdin.
    with tempfile.NamedTemporaryFile() as source:
        source.write(message)
        source.flush()
        result = subprocess.run(
            [
                "openssl",
                "pkeyutl",
                "-sign",
                "-rawin",
                "-inkey",
                str(key),
                "-in",
                source.name,
            ],
            stdout=subprocess.PIPE,
            check=True,
        )
    if len(result.stdout) != 64:
        fail("OpenSSL returned an invalid Ed25519 signature")
    return base64.b64encode(result.stdout).decode()


def aws_environment() -> dict[str, str]:
    environment = {
        **os.environ,
        "AWS_ACCESS_KEY_ID": required("MILK_STORE_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": required("MILK_STORE_SECRET_ACCESS_KEY"),
        "AWS_DEFAULT_REGION": required("MILK_STORE_REGION"),
        "AWS_EC2_METADATA_DISABLED": "true",
    }
    session = os.environ.get("MILK_STORE_SESSION_TOKEN")
    if session:
        environment["AWS_SESSION_TOKEN"] = session
    return environment


def aws_args() -> list[str]:
    return [
        "aws",
        "s3api",
        "--endpoint-url",
        required("MILK_STORE_ENDPOINT"),
        "--region",
        required("MILK_STORE_REGION"),
    ]


def current_pointer(
    bucket: str, key: str, scope_id: uuid.UUID, environment: dict[str, str]
) -> tuple[int, str] | None:
    head = subprocess.run(
        [*aws_args(), "head-object", "--bucket", bucket, "--key", key, "--output", "json"],
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if head.returncode != 0:
        if any(marker in head.stderr for marker in ("404", "NoSuchKey", "Not Found")):
            return None
        fail(f"failed to inspect current route pointer: {head.stderr.strip()}")
    metadata = json.loads(head.stdout)
    etag = metadata.get("ETag")
    if not isinstance(etag, str) or not etag:
        fail("current route pointer has no ETag")
    with tempfile.TemporaryDirectory() as directory:
        destination = Path(directory) / "current.json"
        subprocess.run(
            [*aws_args(), "get-object", "--bucket", bucket, "--key", key, str(destination)],
            env=environment,
            stdout=subprocess.DEVNULL,
            check=True,
        )
        existing = json.loads(destination.read_bytes())
    revision = existing.get("revision")
    if (
        existing.get("schema_version") != "milk.route-pointer.v2"
        or existing.get("scope_id") != str(scope_id)
        or isinstance(revision, bool)
        or not isinstance(revision, int)
        or revision < 1
    ):
        fail("current route pointer has invalid identity or revision")
    return revision, etag


def put(
    bucket: str,
    key: str,
    body: bytes,
    environment: dict[str, str],
    condition: tuple[str, str],
) -> None:
    with tempfile.NamedTemporaryFile() as source:
        source.write(body)
        source.flush()
        subprocess.run(
            [
                *aws_args(),
                "put-object",
                "--bucket",
                bucket,
                "--key",
                key,
                "--body",
                source.name,
                "--content-type",
                "application/json",
                condition[0],
                condition[1],
            ],
            env=environment,
            stdout=subprocess.DEVNULL,
            check=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--signing-key", required=True, type=Path)
    parser.add_argument("--scope-id", required=True, type=uuid.UUID)
    parser.add_argument("--revision", required=True, type=int)
    parser.add_argument("--candidate-bps", required=True, type=int)
    parser.add_argument("--candidate-chat-base-url")
    parser.add_argument("--candidate-responses-base-url")
    parser.add_argument("--candidate-artifact-sha256")
    parser.add_argument("--expires-at", required=True, type=timestamp)
    parser.add_argument("--valid-from", type=timestamp, default=now())
    parser.add_argument("--route-id", type=uuid.UUID, default=None)
    args = parser.parse_args()

    if args.scope_id.int == 0 or args.revision < 1:
        fail("scope and revision must be nonzero")
    if not 0 <= args.candidate_bps <= 10_000:
        fail("candidate basis points must be in 0..=10000")
    chat_base_url = candidate_base_url(
        "candidate Chat base URL", args.candidate_chat_base_url
    )
    responses_base_url = candidate_base_url(
        "candidate Responses base URL", args.candidate_responses_base_url
    )
    if args.candidate_bps:
        if chat_base_url is None and responses_base_url is None:
            fail("an active candidate requires at least one native protocol base URL")
        if not args.candidate_artifact_sha256 or not re.fullmatch(
            r"[0-9a-f]{64}", args.candidate_artifact_sha256
        ):
            fail("an active candidate requires a lowercase SHA-256 artifact digest")
    elif (
        chat_base_url is not None
        or responses_base_url is not None
        or args.candidate_artifact_sha256 is not None
    ):
        fail("a zero route must omit candidate protocol URLs and artifact digest")
    if args.valid_from >= args.expires_at:
        fail("route expiry must follow its start")
    key_mode = stat.S_IMODE(args.signing_key.stat().st_mode)
    if key_mode & 0o077:
        fail("signing key must not be accessible by group or other users")

    route_id = args.route_id or uuid.uuid4()
    if route_id.int == 0:
        fail("route ID must be nonzero")
    candidate_protocols = {}
    if args.candidate_bps:
        for protocol, base_url in (
            ("chat_completions", chat_base_url),
            ("responses", responses_base_url),
        ):
            if base_url is not None:
                candidate_protocols[protocol] = hashlib.sha256(
                    canonical(
                        {
                            "artifact_sha256": args.candidate_artifact_sha256,
                            "base_url": base_url,
                            "protocol": protocol,
                        }
                    )
                ).hexdigest()
    route_unsigned = {
        "schema_version": "milk.route.v3",
        "scope_id": str(args.scope_id),
        "route_id": str(route_id),
        "revision": args.revision,
        "valid_from": args.valid_from,
        "expires_at": args.expires_at,
        "baseline": "baseline",
        "candidate": (
            {
                "target": "candidate-a",
                "artifact_sha256": args.candidate_artifact_sha256,
                "basis_points": args.candidate_bps,
                "protocols": candidate_protocols,
            }
            if args.candidate_bps
            else None
        ),
    }
    route = canonical({**route_unsigned, "signature": sign(args.signing_key, canonical(route_unsigned))})
    route_sha256 = hashlib.sha256(route).hexdigest()
    pointer_unsigned = {
        "schema_version": "milk.route-pointer.v2",
        "scope_id": str(args.scope_id),
        "route_id": str(route_id),
        "revision": args.revision,
        "route_sha256": route_sha256,
        "published_at": now(),
    }
    pointer = canonical(
        {**pointer_unsigned, "signature": sign(args.signing_key, canonical(pointer_unsigned))}
    )

    bucket = required("MILK_STORE_BUCKET")
    environment = aws_environment()
    prefix = f"milk/v2/scopes/{args.scope_id}/r"
    pointer_key = f"{prefix}/current.json"
    current = current_pointer(bucket, pointer_key, args.scope_id, environment)
    if current is not None and args.revision <= current[0]:
        fail(f"revision must exceed current revision {current[0]}")

    put(
        bucket,
        f"{prefix}/{route_id}.json",
        route,
        environment,
        ("--if-none-match", "*"),
    )
    condition = ("--if-match", current[1]) if current else ("--if-none-match", "*")
    put(bucket, pointer_key, pointer, environment, condition)
    print(
        json.dumps(
            {
                "scope_id": str(args.scope_id),
                "route_id": str(route_id),
                "revision": args.revision,
                "candidate_basis_points": args.candidate_bps,
                "candidate_artifact_sha256": args.candidate_artifact_sha256,
                "candidate_protocols": candidate_protocols,
                "route_sha256": route_sha256,
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
