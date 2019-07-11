import json
from unittest import TestCase, mock
from moto import mock_s3

from ...domain import Identifier
from .. import store

class TestLoadEPrint(TestCase):

    def setUp(self):
        self.eprint_json = json.dumps({
            '@type': 'EPrint',
            'abstract': 'Very abstract. Too short to be a real abstract.',
            'acm_class': None,
            'announced_date': '2019-07-11',
            'arxiv_id': '2004.00111',
            'authors': 'Ima N. Author (FSU)',
            'comments': None,
            'doi': None,
            'history': [],
            'is_withdrawn': False,
            'journal_ref': None,
            'legacy': False,
            'license': 'http://notalicense',
            'msc_class': None,
            'pdf': {'@type': 'File',
                    'checksum': 'bNmNEmoWNzA6LEaKswzI6w==',
                    'created': '2019-07-11T15:43:03.031980+00:00',
                    'filename': '2004.00111.pdf',
                    'mime_type': 'application/pdf',
                    'modified': '2019-07-11T15:43:03.031982+00:00'},
            'previous_versions': [],
            'primary_classification': 'cs.AR',
            'proxy': None,
            'reason_for_withdrawal': None,
            'report_num': None,
            'secondary_classification': ['cs.AI', 'cs.DL'],
            'size_kilobytes': 1,
            'source_package': {'@type': 'File',
                                'checksum': 'UkMjgWHli_2o5cX86fJFRg==',
                                'created': '2019-07-11T15:43:03.031967+00:00',
                                'filename': '2004.00111.tar.gz',
                                'mime_type': 'application/gzip',
                                'modified': '2019-07-11T15:43:03.031972+00:00'},
            'source_type': 'tex',
            'submitted_date': '2019-07-11',
            'submitter': None,
            'title': 'The Title of Everything',
            'version': 1
        }).encode('utf-8')

        self.manifest_json = json.dumps({
            'e-prints/2019/01/1901.00123/v1/1901.00123v1.json':
                '3Vk4TQYzizLHjcyNL62x2w==',
            'e-prints/2019/01/1901.00123/v1/1901.00123v1.pdf':
                'rL0Y20zC-Fzt72VPzMSk2A==',
            'e-prints/2019/01/1901.00123/v1/1901.00123v1.tar.gz':
                'rL0Y20zC-Fzt72VPzMSk2A==',
        }).encode('utf-8')

    def test_load_an_eprint(self):
        """Load an e-print, with lazily-loaded content."""
        store_service = store.CanonicalStore('foobucket')
        fake_content = b'foo'

        def load_fake_data(key):
            if key.endswith('manifest.json'):
                return self.manifest_json
            elif key.endswith('.json'):
                return self.eprint_json
            return fake_content

        store_service._load_key = load_fake_data

        eprint = store_service._load_eprint(Identifier('1901.00123'), 1)
        self.assertEqual(eprint.pdf.content.read(), fake_content,
                         'Lazily-loaded fake data is returned')
        self.assertEqual(eprint.source_package.content.read(), fake_content,
                         'Lazily-loaded fake data is returned')

