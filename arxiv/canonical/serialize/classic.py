"""Parse fields from a single arXiv abstract (.abs) file."""

import os
import re
from typing import Any, Dict, List, Optional, Tuple
from functools import wraps
from dateutil import parser
from pytz import timezone
from datetime import datetime
from dateutil.tz import tzutc, gettz

from .. import domain

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

ASSUMED_LICENSE = domain.License(
    href='http://arxiv.org/licenses/nonexclusive-distrib/1.0/'
)


def parse(path: str) -> domain.EPrintMetadata:
    with open(path, mode='r', encoding='latin-1') as f:
        raw = f.read()

    # TODO: clean up
    modified = datetime.fromtimestamp(os.path.getmtime(path), tz=EASTERN)
    modified = modified.astimezone(tz=tzutc())

    # there are two main components to an .abs file that contain data,
    # but the split must always return four components
    components = RE_ABS_COMPONENTS.split(raw)
    if not len(components) == 4:
        raise IOError('Unexpected number of components parsed from .abs.')

    # everything else is in the second main component
    prehistory, misc_fields = re.split(r'\n\n', components[1])

    fields: Dict[str, Any] = _parse_metadata(key_value_block=misc_fields)
    fields['abstract'] = components[2]  # abstract is the first main component

    id_match = RE_ARXIV_ID_FROM_PREHISTORY.match(prehistory)

    if not id_match:
        raise IOError('Could not extract arXiv ID from prehistory component.')

    arxiv_id = id_match.group('arxiv_id')
    prehistory = re.sub(r'^.*\n', '', prehistory)
    parsed_version_entries = re.split(r'\n', prehistory)

    # submitter data
    from_match = RE_FROM_FIELD.match(parsed_version_entries.pop(0))
    if not from_match:
        raise IOError('Could not extract submitter data.')

    name = from_match.group('name')
    if name is not None:
        name = name.rstrip()

    # get the version history for this particular version of the document
    if not len(parsed_version_entries) >= 1:
        raise IOError('At least one version entry expected.')

    versions = _parse_versions(arxiv_id=arxiv_id,
                               version_entry_list=parsed_version_entries)
    version = versions[-1].version
    submitted_date = versions[-1].submitted_date
    source_type = versions[-1].source_type
    size_kilobytes = versions[-1].size_kilobytes

    # some transformations
    secondary_classification = []

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
        license = domain.License(fields['license'])
    else:
        license = ASSUMED_LICENSE

    return domain.EPrintMetadata(
        arxiv_id=arxiv_id,
        version=version,
        legacy=True,
        submitter=domain.Person(full_name=name) if name else None,
        submitted_date=submitted_date,
        announced_date='',
        license=license,
        primary_classification=primary_classification,
        title=fields['title'],
        abstract=fields['abstract'],
        authors=fields['authors'],
        source_type=source_type,
        size_kilobytes=size_kilobytes,
        secondary_classification=secondary_classification,
        journal_ref=fields.get('journal_ref'),
        report_num=fields.get('report_num'),
        doi=fields.get('doi'),
        msc_class=fields.get('msc_class'),
        acm_class=fields.get('acm_class'),
        proxy=fields.get('proxy'),
        comments=fields.get('comments', ''),
        previous_versions=versions[:-1]

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
            field_name = field_match.group(
                'field').lower().replace('-', '_')
            field_name = re.sub(r'_no$', '_num', field_name)
            fields_builder[field_name] = field_match.group(
                'value').rstrip()
        elif field_name != 'unknown':
            # we have a line with leading spaces
            fields_builder[field_name] += re.sub(r'^\s+', ' ', field_line)
    return fields_builder


def _parse_versions(arxiv_id: str, version_entry_list: List) \
        -> List[domain.VersionReference]:
    """Parse the version entries from the arXiv .abs file."""
    version_entries = list()
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
        version_entries.append(
            domain.VersionReference(arxiv_id=arxiv_id,
                                    version=len(version_entries) + 1,
                                    submitted_date=submitted_date,
                                    announced_date='',
                                    source_type=source_type,
                                    size_kilobytes=size_kilobytes))

    return version_entries
