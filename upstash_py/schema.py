from typing import TypedDict


class UpstashResponse(TypedDict):
    result: str | int | dict | None
    error: str


RESTResponse = str | int | list | None
