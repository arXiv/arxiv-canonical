"""Provide license-related domain concepts and logic."""

from typing import NamedTuple


class License(NamedTuple):
    """License under which the e-print was provided to arXiv."""

    href: str