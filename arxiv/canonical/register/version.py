from datetime import date
from functools import partial
from typing import Dict, Optional, Sequence, Set, Type, Union

from .core import (Base, D, R, I, ICanonicalStorage, ICanonicalSource, _Self,
                   dereference)
from .file import RegisterFile
from .metadata import RegisterMetadata


class RegisterVersion(Base[D.VersionedIdentifier,
                           D.Version,
                           R.RecordVersion,
                           I.IntegrityVersion,
                           str,
                           Union[RegisterFile, RegisterMetadata]]):
    domain_type = D.Version
    record_type = R.RecordVersion
    integrity_type = I.IntegrityVersion
    member_type = RegisterFile

    @classmethod
    def create(cls, s: ICanonicalStorage, sources: Sequence[ICanonicalSource],
               d: D.Version, save_members: bool = True) -> 'RegisterVersion':
        r = R.RecordVersion.from_domain(d, partial(dereference, sources))
        i = I.IntegrityVersion.from_record(r, calculate_new_checksum=True)
        members = RegisterVersion._get_v_members(s, i, save_members)
        return cls(r.name, domain=d, record=r, integrity=i, members=members)

    @classmethod
    def load(cls: Type[_Self], s: ICanonicalStorage,
             sources: Sequence[ICanonicalSource],
             identifier: D.VersionedIdentifier,
             checksum: Optional[str] = None) -> _Self:
        """
        Load an e-print :class:`.Version` from s.

        This method is overridden since it uses a different member mapping
        struct than higher-level collection types.
        """
        # Most of the data needed to reconstitute the Version is in the
        # metadata record.
        key = R.RecordMetadata.make_key(identifier)
        stream, _ = s.load_entry(key)
        d = R.RecordMetadata.to_domain(stream)   # self.load_deferred
        _r = R.RecordMetadata(key=key, stream=stream, domain=d)

        # The manifest provides pre-calculated checksums for version members
        # (source, render, other formats, etc).
        manifest \
            = s.load_manifest(R.RecordVersion.make_manifest_key(identifier))
        r = R.RecordVersion.from_domain(d, partial(dereference, sources),
                                        metadata=_r)
        i = I.IntegrityVersion.from_record(
            r,
            checksum=checksum,
            calculate_new_checksum=bool(checksum is None),
            manifest=manifest
        )
        # This just makes references to the members based on what is already
        # loaded in the IntegrityVersion.
        members = RegisterVersion._get_v_members(s, i, False)

        return cls(r.name, domain=d, record=r, integrity=i,
                   members=members)

    @classmethod
    def _get_v_members(cls, s: ICanonicalStorage,
                       integrity: I.IntegrityVersion,
                       save_members: bool = True) \
            -> Dict[str, Union[RegisterFile, RegisterMetadata]]:
        """
        Describe members of this version.

        This is a little different from the base ``_get_members()`` method,
        in that we are working from an Integrity object rather than a manifest
        alone.
        """
        members: Dict[str, Union[RegisterFile, RegisterMetadata]] = {}
        member: Union[RegisterFile, RegisterMetadata]
        for i_member in integrity.iter_members():
            if isinstance(i_member.record, R.RecordFile):
                assert isinstance(i_member, I.IntegrityEntry)
                assert isinstance(i_member.record.domain, D.CanonicalFile)
                member = RegisterFile(i_member.name,
                                      domain=i_member.record.domain,
                                      record=i_member.record,
                                      integrity=i_member)
            elif isinstance(i_member.record, R.RecordMetadata):
                assert isinstance(i_member.record.domain, D.Version)
                assert isinstance(i_member, I.IntegrityMetadata)
                member = RegisterMetadata(i_member.name,
                                          domain=i_member.record.domain,
                                          record=i_member.record,
                                          integrity=i_member)
            if save_members:
                member.save(s)
            members[member.name] = member
        return members

    @property
    def member_names(self) -> Set[str]:
        assert self.members is not None
        return set([name for name in self.members])

    @property
    def number_of_events(self) -> int:
        return 0

    @property
    def number_of_versions(self) -> int:
        return 1

    def update(self, s: ICanonicalStorage, sources: Sequence[ICanonicalSource],
               version: D.Version) -> None:
        """
        Update a version in place.

        Removes any members (files) not in the passed ``Version``, and retains
        and ignores members without any content (assumes that this is a partial
        update). Saves any new/changed members, and updates the manifest.
        """
        new_version = self.create(s, sources, version, save_members=False)
        # assert self.members is not None and new_version.members is not None
        to_remove = self.member_names - new_version.member_names

        to_add = [name for name in new_version.members
                  # Ignore any members without content, as this may be a
                  # partial update only.
                  if new_version.members[name].domain is not None
                    # Select members not already present, or...
                    and (name not in self.members
                         # ...that appear to have changed.
                         or self.members[name].integrity.checksum
                            != new_version.members[name].integrity.checksum)]
        for name in to_remove:
            self.members[name].delete(s)
            del self.members[name]
        altered = set()
        for name in to_add:
            self.members[name] = new_version.members[name]
            altered.add(self.members[name])
        self.save_members(s, altered)   # Updates our manifest.


