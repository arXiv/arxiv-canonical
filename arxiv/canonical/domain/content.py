from enum import Enum
from typing import List, Optional

from .identifier import VersionedIdentifier


class SourceFileType(Enum):
    """Source file types are represented by single-character codes."""

    Ignore = 'I'
    """All files auto ignore. No paper available."""

    SourceEncrypted = 'S'
    """Source is encrypted and should not be made available."""

    PostscriptOnly = 'P'
    """
    Multi-file PS submission.

    It is not necessary to indicate P with single file PS since in this case
    the source file has .ps.gz extension.
    """

    PDFLaTeX = 'D'
    """A TeX submission that must be processed with PDFlatex."""

    HTML = 'H'
    """Multi-file HTML submission."""

    Ancillary = 'A'
    """Submission includes ancillary files in the /anc directory."""

    DCPilot = 'B'
    """Submission has associated data in the DC pilot system."""

    DOCX = 'X'
    """Submission in Microsoft DOCX (Office Open XML) format."""

    ODF = 'O'
    """Submission in Open Document Format."""

    PDFOnly = 'F'
    """PDF-only with .tar.gz package (likely because of anc files)."""


class SourceType(str):
    def __init__(self, value: str) -> None:
        self._types = [SourceFileType(v) for v in list(value.upper())]

    @property
    def has_docx(self) -> bool:
        return bool(SourceFileType.DOCX in self._types)

    @property
    def has_encrypted_source(self) -> bool:
        return bool(SourceFileType.SourceEncrypted in self._types)

    @property
    def has_html(self) -> bool:
        return bool(SourceFileType.HTML in self._types)

    @property
    def has_ignore(self) -> bool:
        return bool(SourceFileType.Ignore in self._types)

    @property
    def has_odf(self) -> bool:
        return bool(SourceFileType.ODF in self._types)

    @property
    def has_pdf_only(self) -> bool:
        return bool(SourceFileType.PDFOnly in self._types)

    @property
    def has_pdflatex(self) -> bool:
        return bool(SourceFileType.PDFLaTeX in self._types)

    @property
    def has_ps_only(self) -> bool:
        return bool(SourceFileType.PostscriptOnly in self._types)

    @property
    def available_formats(self) -> List['ContentType']:
        """
        List the available dissemination formats for this source type.

        Depending on the original source type, we may not be able to provide
        all supported formats.

        This does not include the source format. Note also that this does
        **not** enforce rules about what should be displayed as an option
        or provided to end users.
        """
        formats = []
        if self.has_ignore and not self.has_encrypted_source:
            pass
        elif self.has_ps_only:
            formats.extend([ContentType.pdf, ContentType.ps])
        elif self.has_pdflatex:
            formats.append(ContentType.pdf)
        elif self.has_pdf_only:
            formats.append(ContentType.pdf)
        elif self.has_html:
            formats.append(ContentType.html)
        elif self.has_docx or self.has_odf:
            formats.append(ContentType.pdf)
        else:
            formats.extend([
                ContentType.pdf,
                ContentType.ps,
                ContentType.dvi,
            ])
        return formats


class ContentType(Enum):
    pdf = 'pdf'
    tar = 'tar'
    json = 'json'
    abs = 'abs'
    html = 'html'
    dvi = 'dvi'
    ps = 'ps'
    tex = 'tex'

    @property
    def mime_type(self) -> str:
        return _mime_types[self]

    @property
    def ext(self) -> str:
        return _extensions[self]

    @classmethod
    def from_filename(cls, filename: str) -> 'ContentType':
        for ctype, ext in _extensions.items():
            if filename.endswith(ext) or filename.endswith(f'{ext}.gz'):
                return ctype
        raise ValueError(f'Unrecognized extension: {filename}')

    @classmethod
    def from_mimetype(cls, mime: str) -> 'ContentType':
        return {v: k for k, v in _mime_types.items()}[mime]

    def make_filename(self, identifier: VersionedIdentifier,
                      is_gzipped: bool = False) -> str:
        """Make a filename for this content type based on an identifier."""
        if identifier.is_old_style:
            fn = f'{identifier.numeric_part}v{identifier.version}.{self.ext}'
        else:
            fn = f'{identifier}.{self.ext}'
        if is_gzipped:
            fn = f'{fn}.gz'
        return fn



_mime_types = {
    ContentType.pdf: 'application/pdf',
    ContentType.tar: 'application/x-tar',
    ContentType.json: 'application/json',
    ContentType.abs: 'text/plain',
    ContentType.html: 'text/html',
    ContentType.dvi: 'application/x-dvi',
    ContentType.ps: 'application/postscript',
    ContentType.tex: 'application/x-tex',
}

_extensions = {
    ContentType.pdf: 'pdf',
    ContentType.tar: 'tar',
    ContentType.json: 'json',
    ContentType.abs: 'abs',
    ContentType.html: 'html',
    ContentType.dvi: 'dvi',
    ContentType.ps: 'ps',
    ContentType.tex: 'tex'
}


DISSEMINATION_FORMATS_BY_SOURCE_EXT = [
    ('.tar.gz', None),
    ('.tar', None),
    ('.dvi.gz', None),
    ('.dvi', None),
    ('.pdf', [ContentType.pdf]),
    ('.ps.gz', [ContentType.pdf, ContentType.ps]),
    ('.ps', [ContentType.pdf, ContentType.ps]),
    ('.html.gz', [ContentType.html]),
    ('.html', [ContentType.html]),
    ('.gz', None),
]
"""
Dissemination formats that can be inferred from source file extension.

.. note::
    This is largely to support format discovery in classic. In the NG
    canonical record, this should all be explicit.
"""


def available_formats_by_ext(filename: str) -> Optional[List[ContentType]]:
    """
    Attempt to determine the available dissemination formats by file extension.

    It sometimes (but not always) possible to infer the available dissemination
    formats based on the filename extension of the source package.

    .. note::
        This is largely to support format discovery in classic. In the NG
        canonical record, this should all be explicit.

    """
    for ext, formats in DISSEMINATION_FORMATS_BY_SOURCE_EXT:
        if filename.endswith(ext):
            return formats
    return None


def list_source_extensions() -> List[str]:
    return [ext for ext, _ in DISSEMINATION_FORMATS_BY_SOURCE_EXT]