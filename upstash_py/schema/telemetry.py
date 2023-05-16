from typing import TypedDict
from typing import NotRequired


class TelemetryData(TypedDict):
    runtime: str
    sdk: str
    platform: NotRequired[str]  # For now this will be omitted.
