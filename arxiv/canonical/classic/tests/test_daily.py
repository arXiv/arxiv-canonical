"""Tests for :mod:`.serialize.classic.daily`."""

import os
from datetime import date
from unittest import TestCase

from .. import daily
from ...domain import Event, EventType

sample_data = """
980302|gr-qc|9802067-9802072|hep-th9712213 hep-th9802173 physics.class-ph9802047|9708027
980302|hep-ph|9802442-9802449|hep-th9802191 nucl-th9802079|9708203 9801356
980302|nucl-th|9802082-9802085|hep-ph9802370 hep-ph9802424 hep-ph9802430 physics.plasm-ph9703021|
980302|hep-lat|9802036-9802038||
980302|hep-ex|9802024|hep-ph9802408 physics.ins-det9802015|
980302|astro-ph|9802349-9802364|gr-qc9802066 hep-ph9802424 hep-ph9802430|9801284 9802337
980302|cond-mat|9802293-9802311|hep-th9802025 physics.plasm-ph9703021.stat-mech|9712229 9802271 9802278 9802283 cond-mat.soft9802278.mtrl-sci
980302|quant-ph|9802069-9802071||9710025 9710055
980302|physics|9802049-9802054|cond-mat.stat-mech9703144.plasm-ph|9802015 9802037
""".strip()

DATA = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')


class TestParseLine(TestCase):
    """Test :func:`daily.parse_line`."""

    def test_parse_oldstyle_line(self):
        """Parse an old-style line with new, cross, and replacements."""
        events = [e for e in daily.DailyLogParser().parse_line(
            "980302|hep-th|9802196-9802204|cond-mat.mes-hall9802266 cond-mat.mes-hall9802267 cond-mat.mes-hall9802290 hep-ph9802436|9709125 9712213 gr-qc9708027 hep-ph9708203"
        )]
        new = [e for e in events if e.event_type is EventType.NEW]
        cross = [e for e in events if e.event_type is EventType.CROSSLIST]
        replaced = [e for e in events if e.event_type is EventType.REPLACED]

        self.assertEqual(len(new), 9)
        for event in new:
            self.assertIn('hep-th', event.categories)
            self.assertEqual(event.version, 1)

        self.assertEqual(len(cross), 4)
        for event in cross:
            self.assertIn('hep-th', event.categories)
        self.assertIn('cond-mat/9802266', [e.arxiv_id for e in cross])
        self.assertIn('cond-mat/9802267', [e.arxiv_id for e in cross])
        self.assertIn('cond-mat/9802290', [e.arxiv_id for e in cross])
        self.assertIn('hep-ph/9802436', [e.arxiv_id for e in cross])

        self.assertEqual(len(replaced), 4)
        for event in replaced:
            self.assertIn('hep-th', event.categories)

        self.assertIn('hep-th/9709125', [e.arxiv_id for e in replaced])
        self.assertIn('hep-th/9712213', [e.arxiv_id for e in replaced])
        self.assertIn('gr-qc/9708027', [e.arxiv_id for e in replaced])
        self.assertIn('hep-ph/9708203', [e.arxiv_id for e in replaced])

    def test_parse_oldstyle_line_new_only(self):
        """Parse an old-style line with only new announcements."""
        events = [e for e in daily.DailyLogParser().parse_line(
            "980302|hep-lat|9802036-9802038||"
        )]
        new = [e for e in events if e.event_type is EventType.NEW]
        cross = [e for e in events if e.event_type is EventType.CROSSLIST]
        replaced = [e for e in events if e.event_type is EventType.REPLACED]

        self.assertEqual(len(new), (9802038 - 9802036) + 1)
        self.assertEqual(len(cross), 0)
        self.assertEqual(len(replaced), 0)

    def test_parse_oldstyle_new_and_cross(self):
        """Parse an old-style line with new and cross-list announcements."""
        events = [e for e in daily.DailyLogParser().parse_line(
            "980302|hep-ex|9802024|hep-ph9802408 physics.ins-det9802015|"
        )]
        new = [e for e in events if e.event_type is EventType.NEW]
        cross = [e for e in events if e.event_type is EventType.CROSSLIST]
        replaced = [e for e in events if e.event_type is EventType.REPLACED]

        self.assertEqual(len(new), 1)
        self.assertEqual(len(cross), 2)
        self.assertEqual(len(replaced), 0)
        self.assertIn('hep-ph/9802408', [e.arxiv_id for e in cross])
        self.assertIn('physics/9802015', [e.arxiv_id for e in cross])

    def test_parse_newstyle_line(self):
        """Parse a newstyle line."""
        parser = daily.DailyLogParser()
        with open(os.path.join(DATA, 'new.daily.log')) as f:
            lines = [line for line in f]
        events = [e for e in parser.parse_line(lines[0])]
        new = [e for e in events if e.event_type is EventType.NEW]
        cross = [e for e in events if e.event_type is EventType.CROSSLIST]
        replaced = [e for e in events if e.event_type is EventType.REPLACED]
        self.assertEqual(len(new), 530)
        self.assertEqual(len(cross), 15)
        self.assertEqual(len(replaced), 317)


class TestParse(TestCase):
    """Test parsing a whole daily log file."""

    def test_whole_file(self):
        """Test parsing the whole file."""
        parser = daily.DailyLogParser()
        iterable = parser.parse(os.path.join(DATA, 'new.daily.log'))

        self.assertTrue(hasattr(iterable, '__iter__'))

        events = [e for e in iterable]
        self.assertEqual(len(events), 1882, 'Reads 1,882 events from the log.')

    def test_for_date(self):
        """Parse only events for date 2019-04-12."""
        parser = daily.DailyLogParser()
        iterable = parser.parse(os.path.join(DATA, 'new.daily.log'),
                                for_date=date(2019, 4, 12))

        self.assertTrue(hasattr(iterable, '__iter__'))

        events = [e for e in iterable]
        self.assertEqual(len(events), 951, 'Reads 951 events from the log.')



class TestWeirdEdgeCase(TestCase):
    """
    Test the weird edge case described in :const:`.daily.WEIRD_INVERTED_ENTRY`.
    """

    def test_weird_line(self):
        """Test this weird line that is not handled in legacy code."""
        line = "991210|nlin-sys||cond-mat.mes-hall9912038 cond-mat.stat-mech9912081 cond-mat.stat-mech9912110 hep-th9908090 math.SG9912021 quant-ph9912007|quant-ph9902015 quant-ph9902016 9704019.0chao-dyn 9902003.0chao-dyn 9904021.0chao-dyn 9907001.0chao-dyn 9912003.4solv-int cond-mat.stat-mech9908480 cond-mat.stat-mech9911291"

        events = [e for e in daily.DailyLogParser().parse_line(line)]
        new = [e for e in events if e.event_type is EventType.NEW]
        cross = [e for e in events if e.event_type is EventType.CROSSLIST]
        replaced = [e for e in events if e.event_type is EventType.REPLACED]

        self.assertEqual(len(new), 0)
        self.assertEqual(len(cross), 6)
        self.assertEqual(len(replaced), 9)

        self.assertIn('quant-ph/9902015', [e.arxiv_id for e in replaced])
        self.assertIn('quant-ph/9902016', [e.arxiv_id for e in replaced])
        self.assertIn('chao-dyn/9704019', [e.arxiv_id for e in replaced])

        self.assertIn('chao-dyn/9704019', [e.arxiv_id for e in replaced])
        self.assertIn('chao-dyn/9902003', [e.arxiv_id for e in replaced])
        self.assertIn('chao-dyn/9904021', [e.arxiv_id for e in replaced])
        self.assertIn('chao-dyn/9907001', [e.arxiv_id for e in replaced])
        self.assertIn('solv-int/9912003', [e.arxiv_id for e in replaced])

        self.assertIn('cond-mat/9908480', [e.arxiv_id for e in replaced])
        self.assertIn('cond-mat/9911291', [e.arxiv_id for e in replaced])
