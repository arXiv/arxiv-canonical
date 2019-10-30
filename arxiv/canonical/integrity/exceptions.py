

class ValidationError(Exception):
    """A data consistency problem was encountered."""


class ChecksumError(ValidationError):
    """An unexpected checksum value was encountered."""
