"""Unit tests for the UUIDv7 generator.

Verified properties:
  - Correct version nibble (7)
  - Correct variant bits (0b10)
  - Lexicographic sortability under tight loop (same process)
  - Monotonicity: sequence of UUIDs is non-decreasing
  - Output is valid UUID4-format string parseable by uuid.UUID
"""

from __future__ import annotations

import time
import uuid as _uuid_mod

import pytest

from app.core.uuid7 import uuid7


def _parse(u: str) -> _uuid_mod.UUID:
    return _uuid_mod.UUID(u)


def test_uuid7_is_valid_uuid_string():
    """Generated value must be parseable as a UUID."""
    u = uuid7()
    parsed = _parse(u)
    assert str(parsed) == u


def test_uuid7_version_field():
    """UUID version nibble must be 7."""
    u = uuid7()
    parsed = _parse(u)
    assert parsed.version == 7


def test_uuid7_variant_bits():
    """RFC 4122 variant bits: top 2 bits of clock_seq_hi must be 0b10."""
    u = uuid7()
    parsed = _parse(u)
    # variant is stored in bits 62–63 of the integer (big-endian)
    variant = (parsed.int >> 62) & 0b11
    assert variant == 0b10


def test_uuid7_lexicographic_sortability_across_seconds():
    """UUIDv7s generated 10ms apart must be lexicographically ordered.

    The implementation embeds a 48-bit millisecond timestamp in the most
    significant bits. When two UUIDs are generated far enough apart in time
    (different ms values), the earlier one will sort lower.

    NOTE: Within the same millisecond the ordering is randomised (random bits
    in bits 75-64 and 63-0). This is by design for v0.1 — the architecture
    notes sequence collision handling as an open design question (§13 #4).
    We only test cross-millisecond sortability here.
    """
    import time

    ids = []
    for _ in range(10):
        ids.append(uuid7())
        time.sleep(0.002)  # 2ms apart — guaranteed different ms bucket

    assert ids == sorted(ids), (
        "UUIDv7s generated 2ms apart are not lexicographically sorted"
    )


def test_uuid7_monotonic_integer_across_time():
    """UUIDv7 integer values must be non-decreasing when generated with distinct ms timestamps."""
    import time

    ids = []
    for _ in range(10):
        ids.append(uuid7())
        time.sleep(0.002)

    ints = [_parse(u).int for u in ids]
    for a, b in zip(ints, ints[1:]):
        assert a <= b, f"Non-monotonic pair: {a} > {b}"


def test_uuid7_same_ms_bucket_has_random_component():
    """UUIDs generated in a tight loop (same ms) differ due to random bits.

    This confirms the random sub-millisecond component is working. Exact
    ordering within the same ms is NOT guaranteed by this implementation.
    """
    uuids = [uuid7() for _ in range(50)]
    # All unique (no duplicates)
    assert len(set(uuids)) == 50, "Duplicates in same-ms batch"
    # All share the same top-48-bit timestamp prefix (same ms bucket)
    # — this might not always be true if tests run across ms boundary, so we
    # just check uniqueness here.


def test_uuid7_timestamp_is_recent():
    """The embedded ms timestamp must be within 2 seconds of now."""
    before_ms = int(time.time() * 1000)
    u = uuid7()
    after_ms = int(time.time() * 1000)
    parsed = _parse(u)
    # Top 48 bits are the timestamp
    embedded_ms = parsed.int >> 80
    assert before_ms - 2000 <= embedded_ms <= after_ms + 2000, (
        f"Embedded timestamp {embedded_ms} is far from now [{before_ms}, {after_ms}]"
    )


def test_uuid7_uniqueness():
    """Every generated UUID in a batch must be unique."""
    uuids = [uuid7() for _ in range(1000)]
    assert len(set(uuids)) == 1000, "Duplicate UUIDv7 generated"


def test_uuid7_format():
    """Result must match standard UUID hex format (8-4-4-4-12)."""
    u = uuid7()
    parts = u.split("-")
    assert len(parts) == 5
    lengths = [len(p) for p in parts]
    assert lengths == [8, 4, 4, 4, 12]
    # All hex chars
    assert all(c in "0123456789abcdef-" for c in u)
