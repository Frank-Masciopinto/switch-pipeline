"""Opaque page tokens: clients pass back ``next_cursor`` without interpreting it."""

import base64
import binascii
import json


class InvalidCursorError(ValueError):
    pass


def encode_cursor(sequence: int) -> str:
    return base64.urlsafe_b64encode(json.dumps({"before": sequence}).encode()).decode()


def decode_cursor(token: str) -> int:
    try:
        value = json.loads(base64.urlsafe_b64decode(token.encode()))["before"]
    except (binascii.Error, ValueError, KeyError, TypeError) as exc:
        raise InvalidCursorError("malformed pagination cursor") from exc
    if not isinstance(value, int) or isinstance(value, bool):
        raise InvalidCursorError("malformed pagination cursor")
    return value
