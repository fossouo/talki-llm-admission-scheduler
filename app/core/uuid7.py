"""UUIDv7 helper — timestamp-based, lexicographically sortable UUID.

UUIDv7 encodes a 48-bit Unix millisecond timestamp in the most-significant
bits, making it suitable as a job identifier that sorts chronologically.

Reference: https://www.ietf.org/archive/id/draft-peabody-dispatch-new-uuid-format-04.txt
"""

from __future__ import annotations

import os
import time
import uuid


def uuid7() -> str:
    """Generate a UUIDv7 string (timestamp-based, sortable).

    Layout (128 bits):
      48 bits — Unix epoch milliseconds
       4 bits — version (0b0111 = 7)
      12 bits — random sub-millisecond sequence
       2 bits — variant (0b10)
      62 bits — random

    The result is returned as a canonical lowercase UUID string.
    """
    ms = int(time.time() * 1000) & 0xFFFF_FFFF_FFFF  # 48-bit epoch-ms
    rand_bytes = int.from_bytes(os.urandom(10), "big")

    # Build the 128-bit integer
    # Bits 127..80: ms (48 bits)
    # Bits 79..76:  version = 7
    # Bits 75..64:  12 random bits
    # Bits 63..62:  variant = 0b10
    # Bits 61..0:   62 random bits
    rand_a = (rand_bytes >> 62) & 0xFFF  # 12 bits
    rand_b = rand_bytes & 0x3FFF_FFFF_FFFF_FFFF  # 62 bits

    value = (
        (ms << 80)
        | (0x7 << 76)
        | (rand_a << 64)
        | (0b10 << 62)
        | rand_b
    )

    return str(uuid.UUID(int=value))
