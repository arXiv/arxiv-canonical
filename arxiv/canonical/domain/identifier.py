"""Provides the concept of an arXiv identifier."""

from arxiv import identifier


class Identifier(str):
    """
    An arXiv e-print identifier.
    
    Supports both old-style (``archive.category/YYMMNNN``) and new-style 
    (``YYMM.NNNNN``) identifiers.
    """

    def __init__(self, value: str) -> None:
        if identifier.STANDARD.match(value.__str__()):
            self.is_old_style = False
        elif identifier.OLD_STYLE.match(value.__str__()):
            self.is_old_style = True
        else:
            raise ValueError('Not a valid arXiv ID')

    @classmethod
    def from_parts(cls, year: int, month: int, inc: int) -> 'Identifier':
        """Generate a new-style identifier from its parts."""
        prefix = f'{str(year)[-2:]}{str(month).zfill(2)}'
        return cls(f'{prefix}.{str(inc).zfill(5)}')

    @property
    def incremental_part(self) -> int:
        """The part of the identifier that is incremental."""
        if self.is_old_style:
            return int(self.split('/', 1)[1][4:])
        return int(self.split('.', 1)[1])

    @property
    def year(self) -> int:
        if self.is_old_style:
            yy = int(self.split('/', 1)[1][0:2])
        else:
            yy = int(self[:2])
        if yy > 90:
            return 1900 + yy
        return 2000 + yy

    @property
    def month(self) -> int:
        if self.is_old_style:
            return int(self.split('/', 1)[1][2:4])
        return int(self[2:4])


class VersionedIdentifier(str):
    def __init__(self, value: str) -> None:
        id_part, version_part = self.split('v', 1)
        self.arxiv_id = Identifier(id_part)
        self.version = int(version_part)

    @classmethod
    def from_parts(cls, arxiv_id: Identifier, version: int) \
            -> 'VersionedIdentifier':
        """Generate a new-style versioned identifier from its parts."""
        return cls(f'{arxiv_id}v{version}')

    @property
    def incremental_part(self) -> int:
        """The part of the identifier that is incremental."""
        return self.arxiv_id.incremental_part

    @property
    def year(self) -> int:
        return self.arxiv_id.year

    @property
    def month(self) -> int:
        return self.arxiv_id.month
