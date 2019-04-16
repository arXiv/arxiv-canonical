
from unittest import TestCase
import os

from .. import daily

sample_data = """
980302|hep-th|9802196-9802204|cond-mat.mes-hall9802266 cond-mat.mes-hall9802267 cond-mat.mes-hall9802290 hep-ph9802436|9709125 9712213 gr-qc9708027 hep-ph9708203
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

    def test_parse_line(self):
        for line in sample_data.split('\n'):
            print(daily.parse_line(line))

    def test_parse_newstyle_line(self):
        with open(os.path.join(DATA, 'new.daily.log')) as f:
            for line in f:
                print(daily.parse_line(line))
