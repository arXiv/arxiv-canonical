"""Parse fields from a single arXiv abstract (.abs) file."""

import os
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple, NamedTuple
from functools import wraps
from dateutil import parser
from pytz import timezone
from datetime import datetime, date
from dateutil.tz import tzutc, gettz

from .. import domain as D

EASTERN = gettz('US/Eastern')

RE_ABS_COMPONENTS = re.compile(r'^\\\\\n', re.MULTILINE)
RE_FROM_FIELD = re.compile(
    r'(?P<from>From:\s*)(?P<name>[^<]+)?\s+(<(?P<email>.*)>)?')
RE_DATE_COMPONENTS = re.compile(
    r'^Date\s*(?::|\(revised\s*(?P<version>.*?)\):)\s*(?P<date>.*?)'
    r'(?:\s+\((?P<size_kilobytes>\d+)kb,?(?P<source_type>.*)\))?$')
RE_FIELD_COMPONENTS = re.compile(
    r'^(?P<field>[-a-z\)\(]+\s*):\s*(?P<value>.*)', re.IGNORECASE)
RE_ARXIV_ID_FROM_PREHISTORY = re.compile(
    r'(Paper:\s+|arXiv:)(?P<arxiv_id>\S+)')

NAMED_FIELDS = ['Title', 'Authors', 'Categories', 'Comments', 'Proxy',
                'Report-no', 'ACM-class', 'MSC-class', 'Journal-ref',
                'DOI', 'License']
"""
Fields that may be parsed from the key-value pairs in second
major component of .abs string. Field names are not normalized.
"""

REQUIRED_FIELDS = ['title', 'authors', 'abstract']
"""
Required parsed fields with normalized field names.

Note the absense of 'categories' as a required field. A subset of version-
affixed .abs files with the old identifiers predate the introduction of
categories and therefore do not have a "Categories:" line; only the (higher-
level) archive and group can be be inferred, and this must be done via the
identifier itself.

The latest versions of these papers should always have the "Categories:" line.
"""

# arXiv ID format used from 1991 to 2007-03
RE_ARXIV_OLD_ID = re.compile(
    r'^(?P<archive>[a-z]{1,}(\-[a-z]{2,})?)(\.([a-zA-Z\-]{2,}))?\/'
    r'(?P<yymm>(?P<yy>\d\d)(?P<mm>\d\d))(?P<num>\d\d\d)'
    r'(v(?P<version>[1-9]\d*))?([#\/].*)?$')

# arXiv ID format used from 2007-04 to present
RE_ARXIV_NEW_ID = re.compile(
    r'^(?P<yymm>(?P<yy>\d\d)(?P<mm>\d\d))\.(?P<num>\d{4,5})'
    r'(v(?P<version>[1-9]\d*))?([#\/].*)?$'
)

ASSUMED_LICENSE = D.License(
    href='http://arxiv.org/licenses/nonexclusive-distrib/1.0/'
)


class AbsRef(NamedTuple):
    identifier: D.VersionedIdentifier
    submitted_date: datetime
    announced_month: str
    source_type: str
    size_kilobytes: int


class AbsData(NamedTuple):
    identifier: D.VersionedIdentifier
    submitter: Optional[D.Person]
    submitted_date: datetime
    announced_month: str
    updated_date: datetime
    license: D.License
    primary_classification: D.Category
    title: str
    abstract: str
    authors: str
    source_type: str
    size_kilobytes: int
    submission_type: D.EventType
    secondary_classification: List[D.Category]
    journal_ref: Optional[str] = None
    report_num: Optional[str] = None
    doi: Optional[str] = None
    msc_class: Optional[str] = None
    acm_class: Optional[str] = None
    proxy: Optional[str] = None
    comments: str = ''
    previous_versions: Optional[List[AbsRef]] = None


# TODO: implement this!
def get_path(base_path: str, identifier: D.VersionedIdentifier) -> str:
    # Needs to handle both new-style and old-style identifiers.
    # return os.path.join(base_path, )
    return ""


# TODO: implement this!
def parse_versions(base_path: str, identifier: D.Identifier) \
        -> Iterable[AbsData]:
    # Needs to handle both new-style and old-style identifiers.
    return [parse(get_path(base_path, v))
            for v in list_versions(base_path, identifier)]


def list_all(base_path: str, from_id: Optional[D.Identifier] = None,
             to_id: Optional[D.Identifier] = None) -> Iterable[D.Identifier]:
    """List all of the identifiers for which we have abs files."""
    return []


def list_versions(base_path: str, identifier: D.Identifier) \
        -> Iterable[D.VersionedIdentifier]:
    """List all of the versions for an identifier from abs files."""
    return []


def get_source_path(base_path: str, identifier: D.VersionedIdentifier) \
        -> D.URI:
    ...


def get_render_path(base_path: str, identifier: D.VersionedIdentifier) \
        -> D.URI:
    # If a render exists (ps_cache), return a file:// uri; otherwise
    # return an https:// uri.
    ...


def get_source(base_path: str, identifier: D.VersionedIdentifier) \
        -> D.CanonicalFile:
    ...

def get_render(base_path: str, identifier: D.VersionedIdentifier) \
        -> D.CanonicalFile:
    ...


def parse(path: str) -> AbsData:
    with open(path, mode='r', encoding='latin-1') as f:
        raw = f.read()

    # The best we can do to infer when the last update was made was to examine
    # the modification time of the abs file itself.
    mtime = os.path.getmtime(path)
    modified = datetime.fromtimestamp(mtime, tz=EASTERN).astimezone(tz=tzutc())

    # There are two main components to an .abs file that contain data,
    # but the split must always return four components.
    components = RE_ABS_COMPONENTS.split(raw)
    if not len(components) == 4:
        raise IOError('Unexpected number of components parsed from .abs.')

    # Everything else is in the second main component.
    prehistory, misc_fields = re.split(r'\n\n', components[1])

    fields: Dict[str, Any] = _parse_metadata(key_value_block=misc_fields)
    # Abstract is the first main component.
    fields['abstract'] = components[2]

    id_match = RE_ARXIV_ID_FROM_PREHISTORY.match(prehistory)
    if not id_match:
        raise IOError('Could not extract arXiv ID from prehistory component.')

    arxiv_id = id_match.group('arxiv_id')
    prehistory = re.sub(r'^.*\n', '', prehistory)
    parsed_version_entries = re.split(r'\n', prehistory)

    # Submitter data.
    from_match = RE_FROM_FIELD.match(parsed_version_entries.pop(0))
    if not from_match:
        raise IOError('Could not extract submitter data.')

    name = from_match.group('name')
    if name is not None:
        name = name.rstrip()

    # Get the version history for this particular version of the document.
    if not len(parsed_version_entries) >= 1:
        raise IOError('At least one version entry expected.')

    versions = _parse_versions(arxiv_id=arxiv_id,
                               version_entry_list=parsed_version_entries)

    secondary_classification: List[str] = []
    if 'categories' in fields and fields['categories']:
        classifications = fields['categories'].split()
        primary_classification = classifications[0]
        secondary_classification = classifications[1:]
    else:
        match = RE_ARXIV_OLD_ID.match(arxiv_id)
        if not match:
            raise IOError('Could not determine primary classification')
        primary_classification = match.group('archive')

    if 'license' in fields:
        license = D.License(fields['license'])
    else:
        license = ASSUMED_LICENSE

    if versions[-1].identifier.version == 1:
        submission_type = D.EventType.NEW
    elif versions[-1].size_kilobytes == 0:
        submission_type = D.EventType.WITHDRAWN
    else:
        submission_type = D.EventType.REPLACED

    return AbsData(
        identifier=versions[-1].identifier,
        submitter=D.Person(full_name=name) if name else None,
        submitted_date=versions[-1].submitted_date,
        announced_month=versions[-1].announced_month,
        updated_date=modified,
        license=license,
        primary_classification=primary_classification,
        title=fields['title'],
        abstract=fields['abstract'],
        authors=fields['authors'],
        source_type=versions[-1].source_type,
        size_kilobytes=versions[-1].size_kilobytes,
        submission_type=submission_type,
        secondary_classification=secondary_classification,
        journal_ref=fields.get('journal_ref'),
        report_num=fields.get('report_num'),
        doi=fields.get('doi'),
        msc_class=fields.get('msc_class'),
        acm_class=fields.get('acm_class'),
        proxy=fields.get('proxy'),
        comments=fields.get('comments', ''),
        previous_versions=versions[:-1],
    )


def _parse_metadata(key_value_block: str) -> Dict[str, str]:
    """Parse the key-value block from the arXiv .abs string."""
    key_value_block = key_value_block.lstrip()
    field_lines = re.split(r'\n', key_value_block)
    field_name = 'unknown'
    fields_builder: Dict[str, str] = {}
    for field_line in field_lines:
        field_match = RE_FIELD_COMPONENTS.match(field_line)
        if field_match and field_match.group('field') in NAMED_FIELDS:
            field_name = field_match.group('field').lower().replace('-', '_')
            field_name = re.sub(r'_no$', '_num', field_name)
            fields_builder[field_name] = field_match.group('value').rstrip()
        elif field_name != 'unknown':
            # we have a line with leading spaces
            fields_builder[field_name] += re.sub(r'^\s+', ' ', field_line)
    return fields_builder


def _parse_announced(arxiv_id: str) -> str:
    match = RE_ARXIV_OLD_ID.match(arxiv_id)
    if not match:
        match = RE_ARXIV_NEW_ID.match(arxiv_id)
    if not match:
        raise ValueError('Not a valid arXiv ID')
    yy = int(match.group('yy'))
    mm = int(match.group('mm'))
    year = f'19{yy}' if yy > 90 else f'20{yy}'
    return f'{year}-{mm}'


def _parse_versions(arxiv_id: str, version_entry_list: List) -> List[AbsRef]:
    """Parse the version entries from the arXiv .abs file."""
    version_entries: List[AbsRef] = list()
    for parsed_version_entry in version_entry_list:
        date_match = RE_DATE_COMPONENTS.match(parsed_version_entry)
        if not date_match:
            raise IOError('Could not extract date components from date line.')
        try:
            sd = date_match.group('date')
            submitted_date = parser.parse(date_match.group('date'))
        except (ValueError, TypeError):
            raise IOError(f'Could not parse submitted date {sd} as datetime')

        source_type = date_match.group('source_type')
        size_kilobytes = int(date_match.group('size_kilobytes'))
        V = len(version_entries) + 1
        identifier = \
            D.VersionedIdentifier(f'{D.Identifier(arxiv_id)}v{V}')
        version_entries.append(
            AbsRef(
                identifier=identifier,
                submitted_date=submitted_date,
                announced_month=_parse_announced(arxiv_id),
                source_type=source_type,
                size_kilobytes=size_kilobytes
            )
        )

    return version_entries
