

from .core import (Base, D, R, I, ICanonicalStorage, ICanonicalSource, _Self)


class RegisterFile(Base[str,
                        D.CanonicalFile,
                        R.RecordFile,
                        I.IntegrityEntry,
                        None,
                        None]):

    domain_type = D.CanonicalFile
    record_type = R.RecordFile
    integrity_type = I.IntegrityEntry
    member_type = type(None)

    def save(self, s: ICanonicalStorage) -> str:
        """
        Save this file.

        Overrides the base method since this is a terminal record, not a
        collection.
        """
        s.store_entry(self.integrity)
        self.integrity.update_checksum()
        return self.integrity.checksum

    def delete(self, s: ICanonicalStorage) -> None:
        raise NotImplementedError('not yet; do this please')