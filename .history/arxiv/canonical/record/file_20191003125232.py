from .core import RecordEntry, D


class RecordFile(RecordEntry[D.CanonicalFile]):
    """An entry that is handled as an otherwise-uninterpreted file."""