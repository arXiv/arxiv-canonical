"""Helpers for domain classes."""

from datetime import datetime
from pytz import UTC


def now() -> datetime:
    """Get a timezone-aware datetime localized to UTC."""
    return datetime.now(UTC)


