
from typing import Optional, Type
from .core import IntegrityEntryBase, R, _Self, calculate_checksum


class IntegrityEntry(IntegrityEntryBase[R.RecordEntry]):
    """Integrity concept for a single entry in the record."""

    record_type = R.RecordEntry

    @classmethod
    def from_record(cls: Type[_Self], record: R.RecordEntry,
                    checksum: Optional[str] = None,
                    calculate_new_checksum: bool = True) -> _Self:
        """Generate an :class:`.IntegrityEntry` from a :class:`.RecordEntry."""
        if calculate_new_checksum:
            checksum = calculate_checksum(record.stream)
        return cls(name=record.key, record=record, checksum=checksum)

    # This is redefined since the entry has no manifest; the record entry is
    # used instead.
    def calculate_checksum(self) -> str:
        return calculate_checksum(self.record.stream)
