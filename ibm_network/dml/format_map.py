"""Single canonical translation table from Ab Initio date/time format tokens to
Spark format tokens (per Phase 3 prompt rule 5).

Ab Initio uses Oracle-style format tokens (YYYY, MI, SS, HH24, HH12). Spark uses
Java's `java.time.format.DateTimeFormatter` style (yyyy, mm, ss, HH, hh).

Order matters: longer / more-specific tokens must be replaced first so that
``HH24`` doesn't get rewritten to ``HH`` while a still-pending ``HH12`` waits
for its turn.
"""

from __future__ import annotations

# (ab_initio_token, spark_token) — applied in order, longest-prefix-first.
_TOKENS: tuple[tuple[str, str], ...] = (
    ("YYYY", "yyyy"),
    ("HH24", "HH"),
    ("HH12", "hh"),
    ("DD", "dd"),
    ("MI", "mm"),
    ("SS", "ss"),
    # MM (month) and HH (hour) keep their case in Spark — left untouched.
)


def to_spark_format(ab_initio_format: str) -> str:
    """Translate a single Ab Initio format string to its Spark equivalent."""
    out = ab_initio_format
    for src, dst in _TOKENS:
        out = out.replace(src, dst)
    return out
