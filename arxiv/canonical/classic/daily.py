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

The main goal of this implementation is parsing the log file for the purpose
of transforming it into the arXiv Canonical format. Specifically, we want to
use this legacy data structure to generate :class:`.Event` data that can be
serialized in the daily listing files.

"""

from typing import Tuple, List, Mapping, Iterable, NamedTuple, Optional
from collections import defaultdict
from datetime import date, datetime
import string
import re
from itertools import chain, groupby

import warnings

from ..domain import Event, Identifier, VersionedIdentifier, EventType

Entry = Tuple[str, str]
"""An ``arxiv_id, category`` tuple."""

LINE = re.compile(r'^(?P<event_date>\d{6})\|(?P<archive>[a-z-]+)'
                  r'\|(?P<data>.*)$')
"""Each line in the log file begins with a date stamp and an archive."""

NEW_STYLE_CUTOVER_AFTER = date(2007, 4, 2)
"""Date after which the new-style format was adopted."""

IDENTIFIER = re.compile(r'^(([a-z\-]+\/\d{7})|(\d{4}\.\d{4,5}))')
SQUASHED_IDENTIFIER = re.compile(r'(?P<archive>[a-z\-]+)(?P<identifier>\d{7})')
"""The old-style format ommitted the forward slash in the old identifier."""

IDENTIFIER_RANGE = re.compile(r'^(?P<start_id>\d{7})\-(?P<end_id>\d{7})$')
"""The old-style format supported ranges of identifiers, e.g. ``1234-1238``."""
SINGLE_IDENTIFIER = re.compile(r'^(\d{7})$')
"""Numeric part of an old-style arXiv ID."""
OLD_STYLE_CROSS = re.compile(r'^(?P<archive>[\w\-]+)(\.[\w\-]+)?'
                             r'(?P<identifier>\d{7})(?P<category>\.[\w\-]+)?')

# The semantics of these patterns are not clear to me. -Erick 2019-04-17
THREEPART_REPLACEMENT = re.compile(r'^(?P<archive>\.[a-zA-Z\-]+)?'
                                   r'(?P<identifier>\d{7})'
                                   r'(?P<category>\.[a-zA-Z\-]+)?$')
FOURPART_REPLACEMENT = re.compile(r'^(?P<archive>[a-z\-]+)(\.[a-zA-Z\-]+)?'
                                  r'(?P<identifier>\d{7})'
                                  r'(?P<category>\.[a-zA-Z\-]+)?$')

WEIRD_INVERTED_ENTRY = re.compile(r'^(?P<identifier>\d{7})(?:\.\d)?'
                                  r'(?P<archive>[a-z\-]+)(\.[a-zA-Z\-]+)?$')
"""
Pattern for a weird edge case not handled in the legacy code.

Here is an example:

.. code-block::

   quant-ph9902016 9704019.0chao-dyn 9902003.0chao-dyn 9904021.0chao-dyn

``quant-ph9902016`` is normal. But ``9704019.0chao-dyn`` does not match any
patterns in the legacy code. In this particular case (from 1999), we can infer
that ``9704019`` belongs with ``chao-dyn`` rather than ``quant-ph`` because
``quant-ph/9704019`` was last updated in 1997 and this entry is in 1999 when
``chao-dyn/9704019`` was last updated.

Not sure what the decimal part is supposed to mean. It does not appear to refer
to the e-print version. I also considered the possibility that it is a range
of some kind, e.g. ``9912003.4solv-int`` -> ``solv-int/9912003`` and
``solv-int/9912004``, but this is in a replacement section and there is only
one version of ``solv-int/9912004``.
"""


class EventData(NamedTuple):
    """Data about events that can be extracted from the daily log."""

    arxiv_id: str
    event_datetime: datetime
    event_type: EventType
    version: int
    categories: List[str]


class DailyLogParser:
    """Parses the daily log file."""

    def __init__(self) -> None:
        """Initialize both styles of parsers."""
        self.newstyle_parser = NewStyleLineParser()
        self.oldstyle_parser = OldStyleLineParser()

    def _parse_date(self, event_date_raw: str) -> date:
        """Parse date stamp in the format ``yymmdd``."""
        yy = int(event_date_raw[:2])
        month = int(event_date_raw[2:4])
        day = int(event_date_raw[4:])

        # This will be OK until 2091.
        year = 1900 + yy if yy > 90 else 2000 + yy
        event_date = date(year=year, month=month, day=day)
        return event_date

    def _parse_date_only(self, raw: str) -> Optional[date]:
        match = LINE.match(line)
        if match is None:
            return
        return self._parse_date(match.group('event_date'))

    def parse(self, path: str, for_date: Optional[date] = None) -> Iterable[EventData]:
        """
        Parse the daily log file.

        Parameters
        ----------
        path : str
            Path to the daily log file.

        Returns
        -------
        iterable
            Each item is an :class:`.EventData` from the log file.

        """
        return chain.from_iterable(
            (self.parse_line(line) for line in open(path, 'r', -1)
             if for_date is None
             or for_date == self._parse_date_only(line))
        )

    def parse_line(self, raw: str) -> Iterable[EventData]:
        """
        Parse a single line from the daily log file.

        Parameters
        ----------
        raw : str
            A single line.

        Returns
        -------
        iterable
            Yields :class:`.EventData` instances from the line.

        """
        match = LINE.match(raw)
        if match is None:
            raise ValueError(f'Line is malformed: {raw}')
        archive = match.group('archive')
        data = match.group('data')
        event_date = self._parse_date(match.group('event_date'))

        if event_date > NEW_STYLE_CUTOVER_AFTER:
            return self.newstyle_parser.parse(event_date, archive, data)
        return self.oldstyle_parser.parse(event_date, archive, data)


class LineParser:
    """Shared behavior among newstyle and oldstyle line parsing."""

    def _to_events(self, e_date: date, e_type: EventType,
                   entries: Iterable[Entry],
                   version: int = -1) -> Iterable[EventData]:
        event_datetime = datetime(e_date.year, e_date.month, e_date.day)
        for paper_id, entries in groupby(entries, key=lambda o: o[0]):
            yield EventData(paper_id, event_datetime, e_type, version,
                            [category for _, category in entries])

    def parse(self, e_date: date, archive: str, data: str) \
            -> Iterable[EventData]:
        """Parse data from a daily log file line."""
        new, cross, replace = data.split('|')
        return chain(self._to_events(e_date, EventType.NEW,
                                     self.parse_new(archive, new), 1),
                     self._to_events(e_date, EventType.CROSSLIST,
                                     self.parse_cross(archive, cross)),
                     self._to_events(e_date, EventType.REPLACED,
                                     self.parse_replace(archive, replace)))

    def parse_new(self, archive: str, fragment: str) -> Iterable[Entry]:
        """Parse entries for new e-prints."""
        raise NotImplementedError('Not implemented in this base class')

    def parse_cross(self, archive: str, fragment: str) -> Iterable[Entry]:
        """Parse entries for cross-list e-prints."""
        raise NotImplementedError('Not implemented in this base class')

    def parse_replace(self, archive: str, fragment: str) -> Iterable[Entry]:
        """Parse entries for replacements."""
        raise NotImplementedError('Not implemented in this base class')


class OldStyleLineParser(LineParser):
    """
    Parses data from old-style log lines.

    The original format used a separate line for each archive. The line
    contained three sections: e-prints newly announced in that archive,
    e-prints cross-listed to that archive, and e-prints replaced either in that
    archive or with a new cross-list category in that archive. Thus there may
    be multiple lines for a given announcement day, one per archive in which
    announcement activity occurred.
    """

    def parse_new(self, archive: str, fragment: str) -> Iterable[Entry]:
        """
        Parse entries for new e-prints.

        Parameters
        ----------
        archive : str
            Archive to which entries on this line apply.
        fragment : str
            Section of the line containing new e-print entries.

        Returns
        -------
        iterable
            Yields :class:`.Event` instances from this section.

        """
        match_range = IDENTIFIER_RANGE.match(fragment)
        if match_range:
            start_id = int(match_range.group('start_id'))
            end_id = int(match_range.group('end_id'))
            for _identifier in range(start_id, end_id + 1):  # Inclusive.
                identifier = str(_identifier).zfill(7)
                paper_id = f'{archive}/{identifier}'
                yield paper_id, archive
        elif SINGLE_IDENTIFIER.match(fragment):
            yield f'{archive}/{fragment}', archive
        elif re.match(r'\S', fragment) is None:   # Blank is OK
            pass
        else:
            warnings.warn(f'Failed parsing new entry (old style): {fragment}')

    def parse_cross(self, archive: str, fragment: str) -> Iterable[Entry]:
        """
        Parse entries for cross-list e-prints.

        Parameters
        ----------
        archive : str
            Archive to which entries on this line apply (to which the
            e-print has been cross-listed).
        fragment : str
            Section of the line containing cross-list entries.

        Returns
        -------
        iterable
            Yields :class:`.Event` instances from this section.

        """
        for paper_id in fragment.strip().split():
            match = OLD_STYLE_CROSS.match(paper_id)
            if match:
                paper_id = '/'.join([match.group('archive'),
                                     match.group('identifier')])
                crossed_to = archive
                category = match.group('category')
                if category:
                    crossed_to += category
                yield paper_id, crossed_to
            else:
                warnings.warn(f'Failed parsing cross (old style): {paper_id}')

    def parse_replace(self, archive: str, fragment: str) -> Iterable[Entry]:
        """
        Parse entries for replacements.

        Parameters
        ----------
        archive : str
            Archive to which entries on this line apply.
        fragment : str
            Section of the line containing replacement entries.

        Returns
        -------
        iterable
            Yields :class:`.Event` instances from this section.

        """
        for paper_id in fragment.strip().split():
            abs_only = False
            if paper_id.endswith('.abs'):
                abs_only = True
                paper_id = paper_id[:-4]
            match_threepart = THREEPART_REPLACEMENT.match(paper_id)
            match_fourpart = FOURPART_REPLACEMENT.match(paper_id)
            match_weird = WEIRD_INVERTED_ENTRY.match(paper_id)
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
            elif match_weird:
                this_archive = match_weird.group('archive')
                identifier = match_weird.group('identifier')
                paper_id = f'{this_archive}/{identifier}'
                crossed_to = archive
            else:
                warnings.warn(f'Failed parsing repl (old style): {paper_id}')
                continue
            yield paper_id, crossed_to


class NewStyleLineParser(LineParser):
    """
    Parses new-style daily log lines.

    Starting after 2007-04-02 (:const:`NEW_STYLE_CUTOVER_AFTER`), the format
    changed to put all announcement-related events on a given day on the same
    line. The three original sections of the line are preserved, but within
    each section are entries for e-prints from all archives.
    """

    def parse_new(self, archive: str, fragment: str) -> Iterable[Entry]:
        """
        Parse entries for new e-prints.

        Parameters
        ----------
        archive : str
            Literally just ``"arxiv"``; this is a dummy place-holder, since
            new-style lines contain entries for all archives for which
            announcements occurred on a particular day.
        fragment : str
            Section of the line containing new e-print entries.

        Returns
        -------
        iterable
            Yields :class:`.Event` instances from this section.

        """
        for paper_id in fragment.split():
            try:
                paper_id, dummy, categories = self._parse_entry(paper_id)
            except AssertionError:
                warnings.warn(f'Failed parsing new (new style): {paper_id}')
                continue
            for category in categories:
                yield paper_id, category

    def parse_cross(self, archive: str, fragment: str) -> Iterable[Entry]:
        """
        Parse entries for cross-lists.

        Parameters
        ----------
        archive : str
            Literally just ``"arxiv"``; this is a dummy place-holder, since
            new-style lines contain entries for all archives for which
            announcements occurred on a particular day.
        fragment : str
            Section of the line containing cross-list entries.

        Returns
        -------
        iterable
            Yields :class:`.Event` instances from this section.

        """
        for paper_id in fragment.split():
            try:
                paper_id, dummy, categories = self._parse_entry(paper_id)
            except AssertionError:
                warnings.warn(f'Failed parsing cross (new style): {paper_id}')
                continue
            for crossed_to in categories:
                yield paper_id, crossed_to

    def parse_replace(self, archive: str, fragment: str) -> Iterable[Entry]:
        """
        Parse entries for replaced e-prints.

        Parameters
        ----------
        archive : str
            Literally just ``"arxiv"``; this is a dummy place-holder, since
            new-style lines contain entries for all archives for which
            announcements occurred on a particular day.
        fragment : str
            Section of the line containing replacement entries.

        Returns
        -------
        iterable
            Yields :class:`.Event` instances from this section.

        """
        for paper_id in fragment.split():
            try:
                paper_id, abs_only, categories = self._parse_entry(paper_id)
            except AssertionError:
                warnings.warn(f'Failed parsing repl (new style): {paper_id}')
                continue
            for category in categories:
                yield paper_id, category

    def _parse_entry(self, entry: str) -> Tuple[str, bool, List[str]]:
        """
        Parse a single entry from within a section of the log line.

        An entry represents an announcement-related event for a single e-print.
        Data in the entry is delimited by a colon (``:``). The first item is
        the e-print identifier, followed by each of the categories associated
        with the event.
        """
        abs_only = False
        if entry.endswith('.abs'):
            abs_only = True
            entry = entry[:-4]
        paper_id, categories = entry.split(':', 1)
        categories_list = categories.split(':')
        # unsquash old identifier, if squashed
        squashed = SQUASHED_IDENTIFIER.match(paper_id)
        if squashed:
            paper_id = '/'.join(squashed.groups())
        assert IDENTIFIER.match(paper_id) is not None
        return paper_id, abs_only, categories_list
