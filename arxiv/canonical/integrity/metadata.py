
from typing import Optional, Type
from .core import IntegrityEntryBase, R, _Self, calculate_checksum


class IntegrityMetadata(IntegrityEntryBase[R.RecordMetadata]):
    """Integrity entry for a metadata bitstream in the record."""

    record_type = R.RecordMetadata

    @classmethod
    def from_record(cls: Type[_Self], record: R.RecordMetadata,
                    checksum: Optional[str] = None,
                    calculate_new_checksum: bool = True) -> _Self:
        if calculate_new_checksum:
            checksum = calculate_checksum(record.stream)
        return cls(name=record.key, record=record, checksum=checksum)

    # This is redefined since the entry has no manifest; the record entry is
    # used instead.
    def calculate_checksum(self) -> str:
        return calculate_checksum(self.record.stream)