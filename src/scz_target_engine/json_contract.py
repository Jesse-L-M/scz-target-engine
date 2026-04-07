from __future__ import annotations


def require_json_mapping(value: object, context: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{context} must be a JSON object")
    return value


def require_json_list(value: object, context: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{context} must be a JSON array")
    return value


def require_json_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    return value


def require_json_text(value: object, field_name: str) -> str:
    text = require_json_string(value, field_name)
    if not text.strip():
        raise ValueError(f"{field_name} is required")
    return text


def require_optional_json_string(value: object, field_name: str) -> str:
    if value is None:
        return ""
    return require_json_string(value, field_name)


def require_optional_json_text(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return require_json_text(value, field_name)


def require_json_bool(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return value


def require_json_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


def require_optional_json_int(value: object, field_name: str) -> int | None:
    if value is None:
        return None
    return require_json_int(value, field_name)


def require_json_float(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a float")
    return float(value)


def require_optional_json_float(value: object, field_name: str) -> float | None:
    if value is None:
        return None
    return require_json_float(value, field_name)
