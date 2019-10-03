from .core import Base, D, R, I, ICanonicalStorage

class RegisterMetadata(Base[str,
                            D.Version,
                            R.RecordMetadata,
                            I.IntegrityMetadata,
                            None,
                            None]):

    domain_type = D.Version
    record_type = R.RecordMetadata
    integrity_type = I.IntegrityMetadata
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