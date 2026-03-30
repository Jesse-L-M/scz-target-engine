from __future__ import annotations

import json
import re


INTEGER_PATTERN = re.compile(r"^-?\d+$")
SLUG_PATTERN = re.compile(r"[^a-z0-9]+")


def clean_text(value: str | None) -> str:
    return (value or "").strip()


def split_target_symbols(target: str) -> tuple[str, ...]:
    return tuple(
        token.strip().upper()
        for token in target.split("/")
        if token.strip()
    )


def parse_string_list(value: str | None) -> tuple[str, ...]:
    cleaned = clean_text(value)
    if not cleaned:
        return ()
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        return (cleaned,)
    if parsed is None:
        return ()
    if isinstance(parsed, list):
        return tuple(str(item) for item in parsed)
    return (str(parsed),)


def encode_string_list(values: tuple[str, ...] | list[str]) -> str:
    return json.dumps(list(values), ensure_ascii=True)


def parse_int(value: str | None, *, default: int) -> int:
    cleaned = clean_text(value)
    if not cleaned:
        return default
    if not INTEGER_PATTERN.fullmatch(cleaned):
        raise ValueError(f"expected integer value, got {cleaned!r}")
    return int(cleaned)


def slugify(value: str) -> str:
    return SLUG_PATTERN.sub("-", clean_text(value).lower()).strip("-")
