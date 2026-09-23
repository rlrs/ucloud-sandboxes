"""Expiring rollout observations, never permission to change execution state."""

import math
import re

METADATA_KEY = "_ucloud_resource_phase"
PHASES = frozenset({"model_wait", "tool", "rollout_complete", "training_pause", "training_resume"})
MAX_TTL_SECONDS = 3600.0


def phase_update(payload):
    """Normalize one retryable update before touching registration authority."""
    allowed = {"sequence", "phase", "ttl_seconds", "expected_remaining_wait_seconds"}
    if not isinstance(payload, dict) or set(payload) - allowed:
        raise ValueError("invalid resource phase fields")
    sequence = payload.get("sequence")
    if type(sequence) is not int or not 0 < sequence < 2**63:
        raise ValueError("resource phase sequence must be a positive 63-bit integer")
    phase = payload.get("phase")
    if not isinstance(phase, str) or phase not in PHASES:
        raise ValueError("unsupported resource phase")
    ttl = payload.get("ttl_seconds", 60.0)
    if type(ttl) not in (int, float) or not math.isfinite(ttl) or not 0 < ttl <= MAX_TTL_SECONDS:
        raise ValueError("resource phase ttl_seconds must be in (0, 3600]")
    value = {"sequence": sequence, "phase": phase, "ttl_seconds": float(ttl)}
    wait = payload.get("expected_remaining_wait_seconds")
    if wait is not None:
        if (phase != "model_wait" or type(wait) not in (int, float)
                or not math.isfinite(wait) or not 0 <= wait <= MAX_TTL_SECONDS):
            raise ValueError("expected remaining wait requires model_wait and seconds in [0, 3600]")
        value["expected_remaining_wait_seconds"] = float(wait)
    return value


def current_phase(metadata, *, now):
    """Expiry removes usefulness, not the durable sequence/idempotency fence."""
    value = metadata.get(METADATA_KEY) if isinstance(metadata, dict) else None
    if (not isinstance(value, dict) or value.get("expires_at", 0) <= now
            or value.get("observed_at", now) > now):
        return None
    result = dict(value)
    result["evaluated_at"] = now
    if "expected_remaining_wait_seconds" in result:
        result["expected_remaining_wait_seconds"] = max(
            0.0, result["expected_remaining_wait_seconds"] - max(0.0, now - result["observed_at"])
        )
    return result


def transport_phase(payload):
    """One strict wire shape for optional relay-to-worker scheduling advice."""
    if not isinstance(payload, dict):
        raise ValueError("resource phase must be an object")
    clocks = {"observed_at", "expires_at", "evaluated_at"}
    extras = clocks | {"registration_incarnation"}
    if not extras <= set(payload):
        raise ValueError("resource phase transport timestamps and incarnation are required")
    value = phase_update({key: value for key, value in payload.items() if key not in extras})
    for key in clocks:
        timestamp = payload[key]
        if type(timestamp) not in (int, float) or not math.isfinite(timestamp) or timestamp < 0:
            raise ValueError("resource phase timestamps must be finite nonnegative numbers")
        value[key] = float(timestamp)
    if not (value["observed_at"] <= value["evaluated_at"] < value["expires_at"]
            and value["expires_at"] - value["observed_at"] <= value["ttl_seconds"] + 0.000001):
        raise ValueError("resource phase transport lifetime is inconsistent")
    incarnation = payload["registration_incarnation"]
    if not isinstance(incarnation, str) or re.fullmatch("[0-9a-f]{64}", incarnation) is None:
        raise ValueError("resource phase registration incarnation is invalid")
    value["registration_incarnation"] = incarnation
    return value
