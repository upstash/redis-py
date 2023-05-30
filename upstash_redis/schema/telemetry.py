from typing import TypedDict


class TelemetryData(TypedDict, total=False):  # Allow omitting properties.
    runtime: str
    sdk: str
    platform: str
