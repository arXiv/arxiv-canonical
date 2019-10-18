from enum import Enum


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


class ContentType(Enum):
    pdf = 'pdf'
    targz = 'targz'
    json = 'json'
    abs = 'abs'
    html = 'html'
    dvi = 'dvi'
    ps = 'ps'

    @property
    def mime_type(self) -> str:
        return _mime_types[self]

    @property
    def ext(self) -> str:
        return _extensions[self]

    @classmethod
    def from_mimetype(cls, mime: str) -> 'ContentType':
        return {v: k for k, v in _mime_types.items()}[mime]


_mime_types = {
    ContentType.pdf: 'application/pdf',
    ContentType.targz: 'application/gzip',
    ContentType.json: 'application/json',
    ContentType.abs: 'text/plain',
    ContentType.html: 'text/html',
    ContentType.dvi: 'application/x-dvi',
    ContentType.ps: 'application/postscript',
}

_extensions = {
    ContentType.pdf: 'pdf',
    ContentType.targz: 'tar.gz',
    ContentType.json: 'json',
    ContentType.abs: 'abs',
    ContentType.html: 'html',
    ContentType.dvi: 'x-dvi',
    ContentType.ps: 'ps',
}