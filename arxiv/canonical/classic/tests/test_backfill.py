"""Tests for :mod:`arxiv.canonical.classic.backfill`."""

import json
import os
import tempfile
from datetime import date, datetime
from unittest import TestCase, mock

from ...domain import EventType, Category, Identifier, License, \
    VersionedIdentifier
from ...log import Log
from ...register import IRegisterAPI
from .. import backfill, abs, daily


class TestBackfillRecord(TestCase):
    def setUp(self):
        """The classic record has two e-prints."""
        # One of them was first announced prior to the daily record.
        self.ident = Identifier('1902.00123')
        # The other one was announced after the daily record began.
        self.ident2 = Identifier('1902.00125')

        self.state_path = tempfile.mkdtemp()
        self.events = [
            # The first event we have in the daily record for 1902.00123.
            daily.EventData(
                arxiv_id=self.ident,
                event_date=date(2019, 2, 9),
                event_type=EventType.CROSSLIST,
                version=-1,   # Who knows what version this is?
                categories=[Category('cs.WT')],
            ),
            # Here is where 1902.00125 is first announced.
            daily.EventData(
                arxiv_id=self.ident2,
                event_date=date(2019, 2, 9),
                event_type=EventType.NEW,
                version=1,
                categories=[
                    Category('cs.DL'),
                    Category('cs.IR'),
                ]
            ),
            # Here is where the second version of 1902.00123 is announced.
            daily.EventData(
                arxiv_id=self.ident,
                event_date=date(2019, 2, 10),
                event_type=EventType.REPLACED,
                version=-1,   # Who knows what version this is?
                categories=[
                    Category('cs.DL'),
                    Category('cs.IR'),
                    Category('cs.WT'),
                    Category('cs.FO')
                ]
            )
        ]

        # We have abs records for everything...
        self.abs = [
            # The first version of 1902.00123 (pre-daily).
            abs.AbsData(
                identifier=VersionedIdentifier.from_parts(self.ident, 1),
                submitter=None,
                submitted_date=date(2019, 2, 1),
                announced_month='2019-02',
                updated_date=datetime.now(),
                license=License('http://foo.license'),
                primary_classification=Category('cs.DL'),
                title='foo title before daily.log existed',
                abstract='very abstract',
                authors='Ima N. Author',
                source_type='tex',
                size_kilobytes=42,
                submission_type=EventType.NEW,
                secondary_classification=[
                    Category('cs.IR'),
                    Category('cs.WT'),  # <- This was added by a cross event!
                ],
            ),
            # The second version of 1902.00123, which was noted in daily.log.
            abs.AbsData(
                identifier=VersionedIdentifier.from_parts(self.ident, 2),
                submitter=None,
                submitted_date=date(2019, 2, 9),
                announced_month='2019-02',
                updated_date=datetime.now(),
                license=License('http://foo.license'),
                primary_classification=Category('cs.DL'),
                title='fooooo title after daily.log exists',
                abstract='very abstract',
                authors='Ima N. Author',
                source_type='tex',
                size_kilobytes=42,
                submission_type=EventType.REPLACED,
                secondary_classification=[
                    Category('cs.IR'),
                    Category('cs.WT'),
                    Category('cs.FO')
                ],
            ),
            # The first version of 1902.00125, which was noted in daily.log.
            abs.AbsData(
                identifier=VersionedIdentifier.from_parts(self.ident2, 1),
                submitter=None,
                submitted_date=date(2019, 2, 9),
                announced_month='2019-02',
                updated_date=datetime.now(),
                license=License('http://foo.license'),
                primary_classification=Category('cs.DL'),
                title='another very cool title',
                abstract='very abstract',
                authors='Ima N. Author',
                source_type='tex',
                size_kilobytes=42,
                submission_type=EventType.REPLACED,
                secondary_classification=[
                    Category('cs.IR'),
                ],
            )
        ]

        def _get_abs(identifier):
            for a in self.abs:
                if a.identifier == identifier:
                    return a
            raise RuntimeError(f'No such abs: {identifier}')

        self._get_abs = _get_abs


    @mock.patch(f'{backfill.__name__}.daily')
    @mock.patch(f'{backfill.__name__}.abs')
    def test_backfill(self, mock_abs, mock_daily):
        register = mock.MagicMock(spec=IRegisterAPI)
        added_events = []
        register.add_events.side_effect = added_events.append
        mock_daily.parse.side_effect = [
            self.events,
            self.events[0:2],
            self.events,
        ]
        # This call will get events for a particular identifier during
        # parsing of pre-daily announcements. So we just return the events
        # for 1902.00123.
        mock_daily.scan.return_value = [self.events[0], self.events[2]]
        # Handle a call to list all of the identifiers prior to the first one
        # in daily.log.
        mock_abs.list_all.return_value = \
            list(set([a.identifier.arxiv_id for a in self.abs[:2]]))

        # Return an AbsData based on the requested identifier.
        mock_abs.get_path.side_effect = lambda b, i: i   # Pass ID through.
        mock_abs.parse.side_effect = self._get_abs       # Get AbsData by ID.

        # This is called when parsing the pre-daily records, and gets all of
        # the AbsData of the e-print that was first announced prior to daily.
        mock_abs.parse_versions.return_value = self.abs[0:2]

        backfill.backfill_record(register, '/daily', '/abs', self.state_path)

        # We expect an ordered series of events that represents both what is
        # directly known from daily.log and what is inferred from the presence
        # of abs files and replacement events in daily.log.
        expected = [
            (EventType.NEW, VersionedIdentifier('1902.00123v1')),
            (EventType.CROSSLIST, VersionedIdentifier('1902.00123v1')),
            (EventType.NEW, VersionedIdentifier('1902.00125v1')),
            (EventType.REPLACED, VersionedIdentifier('1902.00123v2')),
        ]
        for (expected_type, expected_id), event in zip(expected, added_events):
            self.assertEqual(expected_type, event.event_type)
            self.assertEqual(expected_id, event.identifier)

        with open(os.path.join(self.state_path, 'first.json')) as f:
            first = json.load(f)

        self.assertEqual(len(first), 2, 'Two entries in first announced index')
        self.assertIn(self.ident, first)
        self.assertIn(self.ident2, first)

        with open(os.path.join(self.state_path, 'current.json')) as f:
            current = json.load(f)

        self.assertEqual(len(current), 2,
                         'Two entries in current version index')
        self.assertIn(self.ident, current)
        self.assertEqual(current[self.ident], 2)
        self.assertIn(self.ident2, current)
        self.assertEqual(current[self.ident2], 1)

        log = Log(self.state_path)
        log_entries = list(log.read_all())
        self.assertEqual(len(log_entries), len(added_events),
                         'There is a log entry for each event')
        for entry in log_entries:
            self.assertEqual(entry.state, 'SUCCESS', 'All entries are SUCCESS')

        for event, entry in zip(added_events, log_entries):
            self.assertEqual(event.event_id, entry.event_id,
                             'Log entries are in the same order as events')



class TestLoadPredailyEvents(TestCase):
    """Load events from before there were events!"""

    @mock.patch(f'{backfill.__name__}.daily')
    @mock.patch(f'{backfill.__name__}.abs')
    def test_load_new_before_daily(self, mock_abs, mock_daily):
        """The first version of an e-print was announced prior to daily.log."""
        ident = Identifier('1902.00123')
        mock_abs.parse_versions.return_value = [
            abs.AbsData(
                identifier=VersionedIdentifier('1902.00123v1'),
                submitter=None,
                submitted_date=date(2019, 2, 1),
                announced_month='2019-02',
                updated_date=datetime.now(),
                license=License('http://foo.license'),
                primary_classification=Category('cs.DL'),
                title='foo title before daily.log existed',
                abstract='very abstract',
                authors='Ima N. Author',
                source_type='tex',
                size_kilobytes=42,
                submission_type=EventType.NEW,
                secondary_classification=[
                    Category('cs.IR'),
                    Category('cs.WT'),
                ],
            ),
            abs.AbsData(
                identifier=VersionedIdentifier('1902.00123v2'),
                submitter=None,
                submitted_date=date(2019, 2, 9),
                announced_month='2019-02',
                updated_date=datetime.now(),
                license=License('http://foo.license'),
                primary_classification=Category('cs.DL'),
                title='fooooo title after daily.log exists',
                abstract='very abstract',
                authors='Ima N. Author',
                source_type='tex',
                size_kilobytes=42,
                submission_type=EventType.REPLACED,
                secondary_classification=[
                    Category('cs.IR'),
                    Category('cs.WT'),
                    Category('cs.FO')
                ],
            )
        ]
        mock_daily.scan.return_value = [
            daily.EventData(
                arxiv_id=ident,
                event_date=date(2019, 2, 10),
                event_type=EventType.REPLACED,
                version=-1,   # Who knows what version this is?
                categories=[
                    Category('cs.DL'),
                    Category('cs.IR'),
                    Category('cs.WT'),
                    Category('cs.FO')
                ]
            )
        ]

        events = backfill._load_predaily('/foo', '/path', ident, {}, {})
        self.assertEqual(len(events), 1, 'Generates one event')
        self.assertEqual(events[0].event_type, EventType.NEW,
                         'Generates a NEW event')
        self.assertEqual(events[0].version.identifier,
                         VersionedIdentifier('1902.00123v1'),
                         'With the first version')
        self.assertEqual(events[0].version.metadata.title,
                         'foo title before daily.log existed',
                         'And the correct title')
        self.assertEqual(events[0].version.metadata.secondary_classification,
                         [Category('cs.IR'), Category('cs.WT')],
                         'And the correct cross-list categories')

    @mock.patch(f'{backfill.__name__}.daily')
    @mock.patch(f'{backfill.__name__}.abs')
    def test_load_new_before_daily_with_cross(self, mock_abs, mock_daily):
        """First version of an e-print in pre-history, with a cross event."""
        ident = Identifier('1902.00123')
        mock_abs.parse_versions.return_value = [
            abs.AbsData(
                identifier=VersionedIdentifier('1902.00123v1'),
                submitter=None,
                submitted_date=date(2019, 2, 1),
                announced_month='2019-02',
                updated_date=datetime.now(),
                license=License('http://foo.license'),
                primary_classification=Category('cs.DL'),
                title='foo title before daily.log existed',
                abstract='very abstract',
                authors='Ima N. Author',
                source_type='tex',
                size_kilobytes=42,
                submission_type=EventType.NEW,
                secondary_classification=[
                    Category('cs.IR'),
                    Category('cs.WT'),  # <- This was added by a cross event!
                ],
            ),
            abs.AbsData(
                identifier=VersionedIdentifier('1902.00123v2'),
                submitter=None,
                submitted_date=date(2019, 2, 9),
                announced_month='2019-02',
                updated_date=datetime.now(),
                license=License('http://foo.license'),
                primary_classification=Category('cs.DL'),
                title='fooooo title after daily.log exists',
                abstract='very abstract',
                authors='Ima N. Author',
                source_type='tex',
                size_kilobytes=42,
                submission_type=EventType.REPLACED,
                secondary_classification=[
                    Category('cs.IR'),
                    Category('cs.WT'),
                    Category('cs.FO')
                ],
            )
        ]
        mock_daily.scan.return_value = [
            daily.EventData(
                arxiv_id=ident,
                event_date=date(2019, 2, 9),
                event_type=EventType.CROSSLIST,
                version=-1,   # Who knows what version this is?
                categories=[Category('cs.WT')],
            ),
            daily.EventData(
                arxiv_id=ident,
                event_date=date(2019, 2, 10),
                event_type=EventType.REPLACED,
                version=-1,   # Who knows what version this is?
                categories=[
                    Category('cs.DL'),
                    Category('cs.IR'),
                    Category('cs.WT'),
                    Category('cs.FO')
                ]
            )
        ]

        events = backfill._load_predaily('/foo', '/bar', ident, {}, {})

        self.assertEqual(len(events), 1, 'Still generates one event')
        self.assertEqual(events[0].version.metadata.secondary_classification,
                         [Category('cs.IR')],
                         'But the cross-list category is not included in the'
                         ' NEW event for the first version!')


class TestDailyEvents(TestCase):
    """Load daily events!"""

    @mock.patch(f'{backfill.__name__}.abs')
    def test_load_new(self, mock_abs):
        """Load a NEW event."""
        ident = Identifier('2302.00123')
        event_datum = daily.EventData(
            arxiv_id=ident,
                event_date=date(2019, 2, 10),
                event_type=EventType.NEW,
                version=1,
                categories=[
                    Category('cs.DL'),
                    Category('cs.IR'),
                ]
        )

        mock_abs.parse.return_value = abs.AbsData(
            identifier=VersionedIdentifier.from_parts(ident, 1),
            submitter=None,
            submitted_date=datetime(2023, 2, 1, 2, 42, 1),
            announced_month='2023-02',
            updated_date=datetime.now(),
            license=License('http://foo.license'),
            primary_classification=Category('cs.DL'),
            title='foo title',
            abstract='very abstract',
            authors='Ima N. Author',
            source_type='tex',
            size_kilobytes=42,
            submission_type=EventType.NEW,
            secondary_classification=[
                Category('cs.IR'),
            ],
        )
        event = backfill._load_daily_event('', event_datum, {}, {})

        self.assertEqual(event.event_type, EventType.NEW, 'Creates NEW event')
        self.assertEqual(event.version.identifier,
                         VersionedIdentifier.from_parts(ident, 1),
                         'With the correct identifier')
        self.assertEqual(event.version.metadata.abstract, 'very abstract',
                         'And the correct abstract')
        self.assertEqual(
            event.event_date,
            datetime(2019, 2, 10, 20, 0, 0, 123, tzinfo=backfill.ET),
            'Event timestamp reflects the announcement day, with microsecond'
            ' based on the incremental part of the identifier to preserve'
            ' order.'
        )

    @mock.patch(f'{backfill.__name__}.abs')
    def test_load_cross(self, mock_abs):
        """Load a CROSSLIST event."""
        ident = Identifier('2302.00123')
        event_datum = daily.EventData(
            arxiv_id=ident,
            event_date=date(2019, 2, 12),
            event_type=EventType.CROSSLIST,
            version=-1,
            categories=[Category('cs.WT')]
        )

        mock_abs.parse.return_value = abs.AbsData(
            identifier=VersionedIdentifier.from_parts(ident, 1),
            submitter=None,
            submitted_date=datetime(2023, 2, 1, 2, 42, 1),
            announced_month='2023-02',
            updated_date=datetime.now(),
            license=License('http://foo.license'),
            primary_classification=Category('cs.DL'),
            title='foo title',
            abstract='very abstract',
            authors='Ima N. Author',
            source_type='tex',
            size_kilobytes=42,
            submission_type=EventType.NEW,
            secondary_classification=[
                Category('cs.IR'), Category('cs.WT')
            ],
        )
        current = {ident: 1}    # The current version number.
        event = backfill._load_daily_event('', event_datum, current, {})

        self.assertEqual(event.event_type, EventType.CROSSLIST,
                         'Creates CROSSLIST event')
        self.assertEqual(event.version.identifier,
                         VersionedIdentifier.from_parts(ident, 1),
                         'With the correct identifier')
        self.assertEqual(event.version.metadata.secondary_classification,
                         [Category('cs.IR'), Category('cs.WT')],
                         'And the correct cross-list classification')
        self.assertEqual(
            event.event_date,
            datetime(2019, 2, 12, 20, 0, 0, 123, tzinfo=backfill.ET),
            'Event timestamp reflects the announcement day, with microsecond'
            ' based on the incremental part of the identifier to preserve'
            ' order.'
        )

    @mock.patch(f'{backfill.__name__}.abs')
    def test_load_replacement(self, mock_abs):
        """Load a REPLACED event."""
        ident = Identifier('2302.00123')
        event_datum = daily.EventData(
            arxiv_id=ident,
            event_date=date(2019, 2, 12),
            event_type=EventType.REPLACED,
            version=-1,
            categories=[
                Category('cs.DL'),
                Category('cs.IR'),
                Category('cs.WT')
            ]
        )

        mock_abs.parse.return_value = abs.AbsData(
            identifier=VersionedIdentifier.from_parts(ident, 2),
            submitter=None,
            submitted_date=datetime(2023, 2, 1, 2, 42, 1),
            announced_month='2023-02',
            updated_date=datetime.now(),
            license=License('http://foo.license'),
            primary_classification=Category('cs.DL'),
            title='foo title',
            abstract='very abstract',
            authors='Ima N. Author',
            source_type='tex',
            size_kilobytes=42,
            submission_type=EventType.NEW,
            secondary_classification=[
                Category('cs.IR'), Category('cs.WT')
            ],
        )
        current = {ident: 1}    # The current version number.
        first = {ident: date(2019, 2, 11)}   # First announcement date.
        event = backfill._load_daily_event('', event_datum, current, first)

        self.assertEqual(mock_abs.get_path.call_args[0][1], '2302.00123v2',
                         'The correct abs version is pulled')

        self.assertEqual(event.event_type, EventType.REPLACED,
                         'Creates REPLACED event')
        self.assertEqual(event.version.identifier,
                         VersionedIdentifier.from_parts(ident, 2),
                         'With the correct identifier')
        self.assertEqual(event.version.metadata.secondary_classification,
                         [Category('cs.IR'), Category('cs.WT')],
                         'And the correct cross-list classification')
        self.assertEqual(
            event.event_date,
            datetime(2019, 2, 12, 20, 0, 0, 123, tzinfo=backfill.ET),
            'Event timestamp reflects the announcement day, with microsecond'
            ' based on the incremental part of the identifier to preserve'
            ' order.'
        )

