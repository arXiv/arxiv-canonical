"""Provides the high-level API for the daily preservation record."""

from .core import IPreservationAPI


class PreservationAPI(IPreservationAPI):
    """Implementation of the high-level API for the preservation record."""