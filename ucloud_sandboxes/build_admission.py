"""Backward-compatible builder admission hints carried in heartbeat labels."""

from collections.abc import Mapping


BUILD_ADMISSION_CAPACITY_LABEL = "ucloud.image-build-admission-capacity"


def build_admission_capacity(labels: Mapping[str, str]) -> int:
    """Return the current total admission ceiling, or the legacy four slots.

    A present but malformed hint closes new admission. Zero is a valid
    temporary closure; existing-build retries do not require another slot.
    This is a scheduling hint, not a reservation: the node must enforce its
    own limit atomically when admitting a new build.
    """

    if BUILD_ADMISSION_CAPACITY_LABEL not in labels:
        return 4
    value = labels[BUILD_ADMISSION_CAPACITY_LABEL]
    # Bound parsing independently of Python's integer-string limit and accept
    # only canonical ASCII decimal, as emitted by str(node_capacity).
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 10
        or not value.isascii()
        or not value.isdecimal()
        or (len(value) > 1 and value.startswith("0"))
    ):
        return 0
    capacity = int(value)
    return capacity if capacity <= (1 << 31) - 1 else 0
