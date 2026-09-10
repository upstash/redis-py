"""Type definitions and helpers for vector index commands (VECTOR.*)."""

import base64
import dataclasses
import enum
from typing import Any, Iterable, List, Optional, Union


class VectorMetric(str, enum.Enum):
    """Similarity metric of a vector index. Fixed when the index is created."""

    COSINE = "COSINE"
    EUCLIDEAN = "EUCLIDEAN"
    DOT = "DOT"


class VectorQueryProfile(str, enum.Enum):
    """Recall and latency trade-off of a vector query. Defaults to BALANCED."""

    FAST = "FAST"
    BALANCED = "BALANCED"
    PRECISE = "PRECISE"


"""
A vector to add or query with:

- a sequence of numbers (list, tuple, numpy array, ...), sent as ``VALUES <n> ...``
- ``bytes``/``bytearray``/``memoryview`` holding little-endian 32-bit floats
  (e.g. ``numpy.asarray(v, dtype="<f4").tobytes()``), sent base64-encoded as ``BASE64-FP32``
- ``str``: an already base64-encoded little-endian FP32 blob (e.g. an embedding API's
  ``encoding_format="base64"`` output), sent as ``BASE64-FP32``
"""
VectorT = Union[Iterable[float], bytes, bytearray, memoryview, str]


@dataclasses.dataclass
class VectorQueryResult:
    """A nearest-neighbour match. Scores are normalized to 0..1, higher is closer."""

    id: str
    score: float


@dataclasses.dataclass
class VectorIndexInfo:
    """Configuration of a vector index."""

    dimension: int
    metric: VectorMetric


def serialize_vector(vector: VectorT) -> List[Any]:
    """Converts a vector into the trailing arguments of VECTOR.ADD / VECTOR.QUERY."""
    if isinstance(vector, str):
        return ["BASE64-FP32", vector]

    if isinstance(vector, (bytes, bytearray, memoryview)):
        blob = bytes(vector)
        if len(blob) == 0 or len(blob) % 4 != 0:
            raise ValueError(
                "An FP32 vector blob must be a non-empty multiple of 4 bytes long."
            )
        return ["BASE64-FP32", base64.b64encode(blob).decode("ascii")]

    values = [float(value) for value in vector]
    return ["VALUES", len(values), *values]


def deserialize_vector(raw: Optional[List[Any]]) -> Optional[List[float]]:
    if raw is None:
        return None
    return [float(value) for value in raw]


def deserialize_vector_query_response(raw: List[Any]) -> List[VectorQueryResult]:
    return [VectorQueryResult(id=str(item[0]), score=float(item[1])) for item in raw]


def deserialize_vector_info_response(raw: Any) -> Optional[VectorIndexInfo]:
    if raw is None:
        return None

    if isinstance(raw, dict):
        fields = raw
    else:
        fields = {raw[i]: raw[i + 1] for i in range(0, len(raw) - 1, 2)}

    return VectorIndexInfo(
        dimension=int(fields["dimension"]),
        metric=VectorMetric(str(fields["metric"]).upper()),
    )
