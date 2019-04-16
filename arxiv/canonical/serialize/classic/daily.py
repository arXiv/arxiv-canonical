"""
Parser for the daily.log file.

From the original arXiv::Updates::DailyLog:

```
Module to provide information about updates to the archive
over specified periods. This should be the only section
of code that reads the daily.log file.

 Simeon Warner - 6Jan2000...
 25Jan2000 - modified so that undef $startdate or $enddate select
   the beginning or end of time respectively.
 25Jan2000 - modified so that by simply removing the `-' from
   and ISO8601 date we get YYYYMMDD from YYYY-MM-DD
 16Oct2000 - to allow easy resumption in the OAI1 interface and
   because it seems that it might be useful in other contexts the
   number limited behaviour has been changed. query_daily_log() and
   hence all other routines now stop at then end of a day and
   returns the that day (in the form YYYY-MM-DD) as the value
   if limited, undef otherwise.

Thoughts: If this is to be used on the mirror sites then we will need
to mirror the daily log. This probably means that that file
should be split up.

 [CVS: $Id: DailyLog.pm,v 1.6 2010/03/23 03:53:09 arxiv Exp $]
```

"""

from typing import Tuple, List, Mapping
from collections import defaultdict
from datetime import date
import string
import re

OLD_PTN = r'^(?P<announcement_date>\d{6})\|(?P<archive>[a-z-]+)\|(?P<line>.*)$'

PIPE = '|'
SPACE = ' '
HYPHEN = '-'
DOT = '.'
DIGITS = string.digits
AZ = string.ascii_lowercase
CATEGORY_CHARS = AZ + HYPHEN + DOT
ID_CHARS = DIGITS + DOT
TERMINATORS = PIPE + SPACE + DIGITS

NEW_STYLE_CUTOVER_AFTER = date(2007, 4, 2)
IDENTIFIER = re.compile(r'^(([a-z\-]+\/\d{7})|(\d{4}\.\d{4,5}))')
SQUASHED_IDENTIFIER = re.compile(r'(?P<archive>[a-z])(?P<identifier>\d{7})')
IDENTIFIER_RANGE = re.compile(r'^(?P<start_id>\d{7})\-(?P<end_id>\d{7})$')
SINGLE_IDENTIFIER = re.compile(r'^(\d{7})$')
OLD_STYLE_CROSS = re.compile(r'^(?P<archive>[\w\-]+)(\.[\w\-]+)?'
                             r'(?P<identifier>\d{7})(?P<category>\.[\w\-]+)?')
THREEPART_REPLACEMENT = re.compile(r'^(?P<archive>\.[a-zA-Z\-]+)?'
                                   r'(?P<identifier>\d{7})'
                                   r'(?P<category>\.[a-zA-Z\-]+)?$')
FOURPART_REPLACEMENT = re.compile(r'^(?P<archive>[a-z\-]+)(\.[a-zA-Z\-]+)?'
                                  r'(?P<identifier>\d{7})'
                                  r'(?P<category>\.[a-zA-Z\-]+)?$')


def parse_line(raw: str) -> Tuple[str, List[str]]:
    match = re.match(OLD_PTN, raw)
    announcement_date_raw = match.group('announcement_date')
    yy = int(announcement_date_raw[:2])
    month = int(announcement_date_raw[2:4])
    day = int(announcement_date_raw[4:])

    # This will be OK until 2091.
    year = 1900 + yy if yy > 90 else 2000 + yy
    announcement_date = date(year=year, month=month, day=day)

    archive = match.group('archive')
    line = match.group('line')
    # ($date,$archive,$new,$cross,$replace) = split(/\|/,$entry);
    new, cross, replace = line.split('|')

    # Includes both primary and cross-list on new e-print.
    new_eprints = parse_new(announcement_date, archive, new)

    # These are only the cross-list categories.
    cross_eprints = parse_cross(announcement_date, archive, cross)

    # Includes both primary and cross-list on replacement e-print.
    replace_eprints = parse_replacement(announcement_date, archive, replace)

    return announcement_date, archive, new_eprints, cross_eprints, replace_eprints


def parse_new(announcement_date: date, archive: str, frag: str) -> List[Tuple[str, str]]:
    entries = []
    if announcement_date > NEW_STYLE_CUTOVER_AFTER:
        for paper_id in frag.split():
            paper_id, dummy, categories = parse_newstyle_entry(paper_id)
            for category in categories:
                entries.append((paper_id, category))
    else:
        match_range = IDENTIFIER_RANGE.match(frag)
        if match_range:
            start_id = int(match_range.group('start_id'))
            end_id = int(match_range.group('end_id'))
            for identifier in range(start_id, end_id + 1):  # Inclusive.
                identifier = str(identifier).zfill(7)
                paper_id = f'{archive}/{identifier}'
                entries.append((paper_id, archive))
        elif SINGLE_IDENTIFIER.match(frag):
            entries.append((f'{archive}/{frag}', archive))
        elif re.match(r'\S') is None:   # Blank is OK
            pass
        else:
            # Warn "Bad new entires for $date|$archive\n"
            pass
    return entries


def parse_cross(announcement_date: date, archive: str, frag: str):
    """
    Parse the cross section of the line.

    Old entries for $archive lines are like

       |math|...|hep-th9901001.MP        => hep-th/9901001 crossed to math.MP
       |astro-ph|...|gr-qc0609044        => gr-qc/0609044 crossed to astro-ph
       |arxiv|...|0703.0003:astro-ph     => 0703.0003 crossed to astro-ph
       |arxiv|...|hep-th0501001:math.DG  => hep-th/0501001 crossed to math.DG

    This routine populates @$crossptr with two element arrays [$paperid,$to].
    """
    frag = frag.strip()     # Zap head or tail spaces to avoid blank entry.
    crossed_to: str     # What is it crossed to, a category name.
    entries = []
    for paper_id in frag.split():
        if announcement_date > NEW_STYLE_CUTOVER_AFTER:
            paper_id, dummy, categories = parse_newstyle_entry(paper_id)
            for crossed_to in categories:
                entries.append((paper_id, crossed_to))
        else:
            match = OLD_STYLE_CROSS.match(frag)
            if match:
                paper_id = '/'.join([match.group('archive'),
                                     match.group('identifier')])
                crossed_to = archive
                category = match.group('category')
                if category:
                    crossed_to += category
                entries.append((paper_id, crossed_to))
            else:
                # Warn "Bad cross entry for ($date|$archive) '$paperid'\n"
                pass
    return entries


def parse_replacement(announcement_date: date, archive: str, frag: str):
    """
    Parse the replacement part of the string.

    Replaces might have .abs after them => abs only replace.

    This routine accepts two array pointers, which may point to
    the same or different arrays, one for full replacements and
    the other for abstract-only replacements.
    """
    frag = frag.strip()  # Zap head or tail spaces to avoid blank entry.
    abs_only_replacements = []
    full_replacements = []
    for paper_id in frag.split():
        if announcement_date > NEW_STYLE_CUTOVER_AFTER:
            paper_id, abs_only, categories = parse_newstyle_entry(paper_id)
            entries = [(paper_id, category) for category in categories]
            if abs_only:
                abs_only_replacements += entries
            else:
                full_replacements += entries
        else:
            abs_only = False
            if paper_id.endswith('.abs'):
                abs_only = True
                paper_id = paper_id[:-4]
            match_threepart = THREEPART_REPLACEMENT.match(paper_id)
            match_fourpart = FOURPART_REPLACEMENT.match(paper_id)
            if match_threepart:
                identifier = match_threepart.group('identifier')
                paper_id = f'{archive}/{identifier}'
                crossed_to = archive
            elif match_fourpart:
                this_archive = match_fourpart.group('archive')
                identifier = match_fourpart.group('identifier')
                category = match_fourpart.group('category')
                paper_id = f'{this_archive}/{identifier}'
                crossed_to = archive
                if category:
                    crossed_to += f'.{category}'
            else:
                # Warn "Bad rep entry for ($date|$archive) '$paperid'\n"
                continue
            if abs_only:
                abs_only_replacements.append((paper_id, crossed_to))
            else:
                full_replacements.append((paper_id, crossed_to))
    return abs_only_replacements + full_replacements


def parse_newstyle_entry(entry: str) -> Tuple[str, bool, List[str]]:
    abs_only = False
    if entry.endswith('.abs'):
        abs_only = True
        entry = entry[:-4]
    paper_id, categories = entry.split(':', 1)
    categories = categories.split(':')
    # unsquash old identifier, if squashed
    squashed = SQUASHED_IDENTIFIER.match(paper_id)
    if squashed:
        paper_id = '/'.join(squashed.groups())
    assert IDENTIFIER.match(paper_id) is not None
    return paper_id, abs_only, categories
