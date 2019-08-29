import io
import json
from datetime import datetime, date, timedelta
from pprint import pprint
from unittest import TestCase

from moto import mock_s3
from pytz import UTC

from ..domain import Listing, Event, EventType, Identifier, Category, \
    ContentType, Version, VersionedIdentifier, Metadata, License, CanonicalFile
from ..integrity import IntegrityListing
from ..serialize.record import RecordListing, ListingSerializer
from ..services import store
from ..register import methods, RegisterListing
from ..register.base import Register


class TestAddEPrints(TestCase):
    @mock_s3
    def test_add_eprints_from_scratch(self):
        """EPrints do not yet exist."""
        s = store.CanonicalStore('foobucket')
        s.inititalize()

        r = Register.load(s, 'all')
        created = datetime(2019, 1, 29, 20, 4, 23, tzinfo=UTC)
        r.add_versions(
            s,
            Version(
                identifier=Identifier('2901.00345'),
                version=1,
                announced_date=created.date(),
                announced_date_first=created.date(),
                submitted_date=created,
                updated_date=created,
                is_announced=True,
                metadata=Metadata(
                    primary_classification=Category('cs.DL'),
                    secondary_classification=[Category('cs.IR')],
                    title='Foo title',
                    abstract='It is abstract',
                    authors='Ima N. Author (FSU)',
                    license=License(href="http://some.license")
                ),
                source=CanonicalFile(
                    filename='2901.00345v1.tar.gz',
                    created=created,
                    modified=created,
                    size_bytes=4_304,
                    content_type=ContentType.targz,
                    content=io.BytesIO(b'fakecontent')
                ),
                render=CanonicalFile(
                    filename='2901.00345v1.pdf',
                    created=created,
                    modified=created,
                    size_bytes=404,
                    content_type=ContentType.pdf,
                    content=io.BytesIO(b'fakepdfcontent')
                )
            )
        )
        r.save(s)
        response = s.client.list_objects_v2(Bucket=s._bucket)

        keys = set()
        for obj in response['Contents']:
            keys.add(obj['Key'])

        expected_metadata = {
            '@type': 'Version',
            'announced_date': '2019-01-29',
            'announced_date_first': '2019-01-29',
            'identifier': '2901.00345',
            'is_announced': True,
            'is_legacy': False,
            'is_withdrawn': False,
            'metadata': {'@type': 'Metadata',
                        'abstract': 'It is abstract',
                        'acm_class': None,
                        'authors': 'Ima N. Author (FSU)',
                        'comments': None,
                        'doi': None,
                        'journal_ref': None,
                        'license': {'@type': 'License', 'href': 'http://some.license'},
                        'msc_class': None,
                        'primary_classification': 'cs.DL',
                        'report_num': None,
                        'secondary_classification': ['cs.IR'],
                        'title': 'Foo title'},
            'previous_versions': [],
            'proxy': None,
            'reason_for_withdrawal': None,
            'render': {'@type': 'CanonicalFile',
                        'content_type': 'pdf',
                        'created': '2019-01-29T20:04:23+00:00',
                        'filename': '2901.00345v1.pdf',
                        'modified': '2019-01-29T20:04:23+00:00',
                        'size_bytes': 404},
            'source': {'@type': 'CanonicalFile',
                        'content_type': 'targz',
                        'created': '2019-01-29T20:04:23+00:00',
                        'filename': '2901.00345v1.tar.gz',
                        'modified': '2019-01-29T20:04:23+00:00',
                        'size_bytes': 4304},
            'source_type': None,
            'submitted_date': '2019-01-29T20:04:23+00:00',
            'submitter': None,
            'updated_date': '2019-01-29T20:04:23+00:00',
            'version': 1
        }
        expected = {
            'global.manifest.json': {
                "entries": [
                    {"key": "eprints", "checksum": "VeJvt6JuAmScdJPa5Wam0g=="}
                ]
            },
            'e-prints.manifest.json': {
                "entries": [
                    {"key": "2029", "checksum": "l_M3xOAJMadYVxmeAlKdzw=="}
                ]
            },
            'e-prints/2029.manifest.json': {
                "entries": [
                    {"key": "2029-01", "checksum": "omUT3Qp6wWaxQ863op6LVQ=="}
                ]
            },
            'e-prints/2029/2029-01.manifest.json': {
                "entries": [
                    {"key": "2019-01-29",
                     "checksum": "1yBkNXUr7-neuyMOYEelPA=="}
                ]
            },
            'e-prints/2019/01/2019-01-29.manifest.json': {
                "entries": [
                    {"key": "2901.00345",
                     "checksum": "4TH-JDvjWN27GoE-bKB7TQ=="}
                ]
            },
            'e-prints/2029/01/2901.00345.manifest.json': {
                "entries": [
                    {"key": "2901.00345v1",
                     "checksum": "jufa1QiTS7U69mHT3X_PKA=="}
                ]
            },
            'e-prints/2029/01/2901.00345/2901.00345v1.manifest.json': {
                "entries": [
                    {"key": "e-prints/2029/01/2901.00345/v1/2901.00345v1.json",
                    "checksum": "NbJhk7XsuQKMHu63_eLBlA=="},
                    {"key": "e-prints/2029/01/2901.00345/v1/2901.00345v1.pdf",
                    "checksum": "faMW1JRszQ9WF7PMbJt21w=="},
                    {"key":
                        "e-prints/2029/01/2901.00345/v1/2901.00345v1.tar.gz",
                    "checksum": "iwS4H0Y-JpPbFaxAZeEv4w=="}
                ]
            },
            'e-prints/2029/01/2901.00345/v1/2901.00345v1.json':
                expected_metadata,
            'e-prints/2029/01/2901.00345/v1/2901.00345v1.pdf':
                b'fakepdfcontent',
            'e-prints/2029/01/2901.00345/v1/2901.00345v1.tar.gz':
                b'fakecontent',
        }

        self.assertSetEqual(keys, set(expected.keys()),
                            'Manifests and e-print files are created')
        # Verify record content.
        for key in keys:
            r = s.client.get_object(Bucket=s._bucket, Key=key)
            content = r['Body'].read()
            if key.endswith('.json'):
                self.assertDictEqual(json.loads(content), expected[key],
                                     'JSON data are stored correctly')
            else:
                self.assertEqual(content, expected[key],
                                 'Bitstream content is stored correctly')


# class TestAddEvents(TestCase):
#     @mock_s3
#     def test_add_events_from_scratch(self):
#         """Listing files do not yet exist."""
#         s = store.CanonicalStore('foobucket')
#         s.inititalize()

#         r = Register.load(s, 'all')
#         r.add_events(s,
#             Event(
#                 arxiv_id=Identifier('2901.00345'),
#                 event_date=datetime.now(),
#                 event_type=EventType.NEW,
#                 categories=[Category('cs.DL')],
#                 version=1
#             ),
#             Event(
#                 arxiv_id=Identifier('2901.00341'),
#                 event_date=datetime.now() - timedelta(days=2),
#                 event_type=EventType.REPLACED,
#                 categories=[Category('cs.IR')],
#                 version=2)
#             )
#         r.save(s)
#         response = s.client.list_objects_v2(Bucket=s._bucket)

#         keys = set()
#         for obj in response['Contents']:
#             keys.add(obj['Key'])
#         expected_keys = set([
#             'global.manifest.json',
#             'announcement.manifest.json',
#             'announcement/2019.manifest.json',
#             'announcement/2019/2019-08.manifest.json',
#             'announcement/2019/08/26/2019-08-26-listing.json',
#             'announcement/2019/08/28/2019-08-28-listing.json'
#         ])
#         self.assertSetEqual(keys, expected_keys,
#                             'Manifests and listing files are created')

        # today = date.today()
        # listing = Listing(date=today,
        #                   events=[Event(arxiv_id=Identifier('2901.00345'),
        #                                 event_date=datetime.now(),
        #                                 event_type=EventType.NEW,
        #                                 categories=[Category('cs.DL')],
        #                                 version=1)])
        # record = ListingSerializer.serialize(listing)
        # integrity = IntegrityListing.from_record(record)
        # register = RegisterListing(
        #     domain=listing,
        #     record=record,
        #     integrity=integrity
        # )
        # methods.store_listing(s, register)
        # self.assertEqual(register.domain,
        #                  methods.load_listing(s, today).domain)

# class TestStoreListing(TestCase):
#     @mock_s3
#     def test_store_listing(self):
#         s = store.CanonicalStore('foobucket')
#         s.inititalize()
#         today = date.today()
#         listing = Listing(date=today,
#                           events=[Event(arxiv_id=Identifier('2901.00345'),
#                                         event_date=datetime.now(),
#                                         event_type=EventType.NEW,
#                                         categories=[Category('cs.DL')],
#                                         version=1)])
#         register.store_listing(s, listing)
#         self.assertEqual(listing, register.load_listing(s, today))
#
#
# class TestLoadListing(TestCase):
#     @mock_s3
#     def test_load_listing(self):
#         s = store.CanonicalStore('foobucket')
#         s.inititalize()
#         today = date.today()
#         listing = Listing(date=today,
#                           events=[Event(arxiv_id=Identifier('2901.00345'),
#                                         event_date=datetime.now(),
#                                         event_type=EventType.NEW,
#                                         categories=[Category('cs.DL')],
#                                         version=1)])
#         register.store_listing(s, listing)
#         self.assertEqual(listing, register.load_listing(s, today))

#     @mock_s3
#     def test_load_listing_with_validation(self):
#         s = store.CanonicalStore('foobucket')
#         s.inititalize()
#         today = date.today()
#         listing = Listing(date=today,
#                           events=[Event(arxiv_id=Identifier('2901.00345'),
#                                         event_date=datetime.now(),
#                                         event_type=EventType.NEW,
#                                         categories=[Category('cs.DL')],
#                                         version=1)])

#         checksum = register.store_listing(s, listing)
#         self.assertEqual(listing, register.load_listing(s, today, checksum))

#     @mock_s3
#     def test_load_listing_full(self):
#         s = store.CanonicalStore('foobucket')
#         s.inititalize()
#         today = date.today()
#         listing = Listing(date=today,
#                           events=[Event(arxiv_id=Identifier('2901.00345'),
#                                         event_date=datetime.now(),
#                                         event_type=EventType.NEW,
#                                         categories=[Category('cs.DL')],
#                                         version=1)])

#         register.store_listing(s, listing)
#         register_listing = register.load_listing_full(s, today)
#         self.assertEqual(listing, register_listing.domain)
#         self.assertIsNone(register_listing.integrity.checksum)
#         self.assertEqual(261, register_listing.record.listing.size_bytes)
#         self.assertEqual(ContentType.json,
#                          register_listing.record.listing.content_type)

#     @mock_s3
#     def test_load_listing_full_with_validation(self):
#         s = store.CanonicalStore('foobucket')
#         s.inititalize()
#         today = date.today()
#         listing = Listing(date=today,
#                           events=[Event(arxiv_id=Identifier('2901.00345'),
#                                         event_date=datetime.now(),
#                                         event_type=EventType.NEW,
#                                         categories=[Category('cs.DL')],
#                                         version=1)])

#         checksum = register.store_listing(s, listing)
#         register_listing = register.load_listing_full(s, today, checksum)
#         self.assertEqual(listing, register_listing.domain)
#         self.assertEqual(checksum, register_listing.integrity.checksum)
#         self.assertEqual(261, register_listing.record.listing.size_bytes)
#         self.assertEqual(ContentType.json,
#                          register_listing.record.listing.content_type)