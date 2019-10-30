class ConsistencyError(Exception):
    """Operation was attempted that would violate consistency of the record."""


class NoSuchResource(Exception):
    """Operation was attempted on a non-existant resource."""