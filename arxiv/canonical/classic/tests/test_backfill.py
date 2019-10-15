from datetime import date, datetime
from unittest import TestCase, mock

from ...domain import EventType, Category, Identifier, License, \
    VersionedIdentifier
from .. import backfill, abs, daily


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

        events = backfill._load_predaily_events('/foo', '/path', ident, {}, {})
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

        events = backfill._load_predaily_events('/foo', '/bar', ident, {}, {})

        self.assertEqual(len(events), 1, 'Still generates one event')
        self.assertEqual(events[0].version.metadata.secondary_classification,
                         [Category('cs.IR')],
                         'But the cross-list category is not included in the'
                         ' NEW event for the first version!')


class TestDailyEvents(TestCase):
    """Load daily events!"""
