from datetime import date
from typing import Optional, NamedTuple, Mapping

from .base import CanonicalBase
from .identifier import Identifier, VersionedIdentifier
from .version import Version


class EPrint(CanonicalBase):
    def __init__(self, identifier: Optional[Identifier],
                 versions: Mapping[VersionedIdentifier, Version]) -> None:
        self.identifier = identifier
        self.versions = versions

    @property
    def announced_date(self) -> Optional[date]:
        idents = [v for v in self.versions]
        return self.versions[idents[0]].announced_date

    @property
    def is_withdrawn(self) -> bool:
        idents = [v for v in self.versions]
        return self.versions[idents[-1]].is_withdrawn

    @property
    def size_kilobytes(self) -> int:
        idents = [v for v in self.versions]
        return self.versions[idents[-1]].size_kilobytes