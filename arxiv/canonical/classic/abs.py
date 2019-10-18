"""Parse fields from a single arXiv abstract (.abs) file."""

import os
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, \
    NamedTuple
from functools import wraps
from dateutil import parser
from pytz import timezone
from datetime import datetime, date
from dateutil.tz import tzutc, gettz

from .. import domain as D

AnyIdentifier = Union[D.VersionedIdentifier, D.Identifier]

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
    source_type: D.SourceType
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
    size_kilobytes: int
    submission_type: D.EventType
    secondary_classification: List[D.Category]
    source_type: Optional[D.SourceType] = None
    journal_ref: Optional[str] = None
    report_num: Optional[str] = None
    doi: Optional[str] = None
    msc_class: Optional[str] = None
    acm_class: Optional[str] = None
    proxy: Optional[str] = None
    comments: str = ''
    previous_versions: Optional[List[AbsRef]] = None


class NoSuchAbs(RuntimeError):
    pass


def original_base_path(data_path: str) -> str:
    return os.path.join(data_path, 'orig')


def latest_base_path(data_path: str) -> str:
    return os.path.join(data_path, 'ftp')


def latest_path_month(data_path: str, identifier: AnyIdentifier) -> str:
    """
    Get the base path for the month block containing the "latest" e-prints.

    This is where the most recent version of each e-print always lives.
    """
    return os.path.join(
        latest_base_path(data_path),
        identifier.category_part if identifier.is_old_style else 'arxiv',
        'papers',
        identifier.yymm
    )


def original_path_month(data_path: str, identifier: AnyIdentifier) -> str:
    """
    Get the main base path for an abs file.

    This is where all of the versions except for the most recent one live.
    """
    return os.path.join(
        original_base_path(data_path),
        identifier.category_part if identifier.is_old_style else 'arxiv',
        'papers',
        identifier.yymm
    )


def latest_path(data_path: str, identifier: AnyIdentifier) -> str:
    return os.path.join(latest_path_month(data_path, identifier),
                        f'{identifier.numeric_part}.abs')


def original_path(data_path: str, identifier: D.VersionedIdentifier) -> str:
    return os.path.join(original_path_month(data_path, identifier),
                        f'{identifier.numeric_part}v{identifier.version}.abs')


def get_path(data_path: str, identifier: D.VersionedIdentifier) -> str:
    # We look first for an "original" abs file that is explicitly identified
    # as the version we are looking for.
    path = original_path(data_path, identifier)
    if os.path.exists(path):
        return path
    # If we are asking for the first version and haven't found it already, the
    # only possibility is that there is one version and its abs file is located
    # in the "latest" section.
    if identifier.version == 1:
        path = latest_path(data_path, identifier)
        if not os.path.exists(path):
            raise NoSuchAbs(f'Cannot find abs record for {identifier}')
        return path
    # The only remaining possibility is that the version we are looking for
    # is indeed the "latest" version, in which case we must be able to find
    # an abs record for the previous version in the "original" section.
    previous = D.VersionedIdentifier.from_parts(identifier.arxiv_id,
                                                identifier.version - 1)
    if os.path.exists(original_path(data_path, previous)):
        return latest_path(data_path, identifier)   # Voila!
    raise NoSuchAbs(f'Cannot find abs record for {identifier}')


def parse_versions(data_path: str, identifier: D.Identifier) \
        -> Iterable[AbsData]:
    return [parse(data_path, v) for v in list_versions(data_path, identifier)]


def parse_latest(data_path: str, identifier: D.Identifier) -> AbsData:
    """Parse the abs for the latest version of an e-print."""
    return _parse(latest_path(data_path, identifier))


def parse_first(data_path: str, identifier: D.Identifier) -> AbsData:
    """Parse the abs for the first version of an e-print."""
    return _parse(get_path(data_path,
                           D.VersionedIdentifier.from_parts(identifier, 1)))


def iter_all(data_path: str, from_id: Optional[D.Identifier] = None,
             to_id: Optional[D.Identifier] = None) -> Iterable[D.Identifier]:
    """
    List all of the identifiers for which we have abs files.

    The "latest" section will have an abs file for every e-print, so that's the
    only place we need look.
    """
    latest_root = latest_base_path(data_path)
    for dirpath, _, filenames in os.walk(latest_root):
        for filename in filenames:
            if filename.endswith('.abs'):
                prefix = dirpath.split(latest_root)[1].split('/')[1]
                numeric_part, _ = os.path.splitext(filename)
                if prefix == 'arxiv':
                    identifier = D.Identifier(numeric_part)
                else:
                    identifier = D.Identifier(f'{prefix}/{numeric_part}')
                if from_id and identifier < from_id:
                    continue
                elif to_id and identifier >= to_id:
                    continue
                yield identifier


def list_versions(data_path: str, identifier: D.Identifier) \
        -> List[D.VersionedIdentifier]:
    """
    List all of the versions for an identifier from abs files.

    This works by looking at the presence of abs files in both the "latest"
    and "original" locations.
    """
    identifiers: List[D.VersionedIdentifier] = []

    # We look first at "original" versions, as they will be explicitly named
    # with their numeric version affix.
    old_versions_exist = False
    orig_month_root = original_path_month(data_path, identifier)
    category = orig_month_root.split(data_path)[1].split('/')[2]
    for dpath, _, fnames in os.walk(orig_month_root):
        for filename in sorted(fnames):
            if filename.endswith('.abs') \
                    and filename.startswith(identifier.numeric_part):
                numeric_part_v, _ = os.path.splitext(filename)
                if identifier.is_old_style:
                    vid = D.VersionedIdentifier(f'{category}/{numeric_part_v}')
                else:
                    vid = D.VersionedIdentifier(numeric_part_v)
                old_versions_exist = True
                identifiers.append(vid)

    if old_versions_exist:
        # We are looking only at past versions above; the most recent version
        # lives somewhere else. We can infer its existence.
        _, v = numeric_part_v.split('v')
        identifiers.append(
            D.VersionedIdentifier.from_parts(identifier, int(v) + 1)
        )
    elif os.path.exists(latest_path(data_path, identifier)):
        # There is only one version, the first version, and it is the
        # latest version.
        identifiers.append(D.VersionedIdentifier.from_parts(identifier, 1))
    return identifiers


def parse(data_path: str, identifier: D.VersionedIdentifier) -> AbsData:
    return _parse(get_path(data_path, identifier))


def _parse(path: str) -> AbsData:
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
        raise IOError(f'Unexpected number of components parsed from {path}')

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

        source_type = D.SourceType(date_match.group('source_type'))
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
