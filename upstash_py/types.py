from typing import TypedDict


class UpstashResponse(TypedDict):
    result: str
    error: str
