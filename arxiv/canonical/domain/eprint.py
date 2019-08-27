from datetime import date
from typing import Optional, NamedTuple, Sequence

from .event import Event
from .identifier import Identifier
from .version import Version


class EPrint(NamedTuple):
    identifier: Optional[Identifier]
    versions: Sequence[Version]

    @property
    def announced_date(self) -> Optional[date]:
        return self.versions[0].announced_date

    @property
    def is_withdrawn(self) -> bool:
        return self.versions[-1].is_withdrawn

    @property
    def size_kilobytes(self) -> int:
        return self.versions[-1].size_kilobytes