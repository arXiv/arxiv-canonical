from datetime import datetime, date, timedelta
from pprint import pprint
from unittest import TestCase

from moto import mock_s3

from ..domain import Listing, Event, EventType, Identifier, Category, \
    ContentType
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
        r.add_eprints(s,
            Event(
                arxiv_id=Identifier('2901.00345'),
                event_date=datetime.now(),
                event_type=EventType.NEW,
                categories=[Category('cs.DL')],
                version=1
            ),
            Event(
                arxiv_id=Identifier('2901.00341'),
                event_date=datetime.now() - timedelta(days=2),
                event_type=EventType.REPLACED,
                categories=[Category('cs.IR')],
                version=2)
            )
        r.save(s)
        response = s.client.list_objects_v2(Bucket=s._bucket)

        keys = set()
        for obj in response['Contents']:
            keys.add(obj['Key'])
        expected_keys = set([])
        self.assertSetEqual(keys, expected_keys,
                            'Manifests and listing files are created')


class TestAddEvents(TestCase):
    @mock_s3
    def test_add_events_from_scratch(self):
        """Listing files do not yet exist."""
        s = store.CanonicalStore('foobucket')
        s.inititalize()

        r = Register.load(s, 'all')
        r.add_events(s,
            Event(
                arxiv_id=Identifier('2901.00345'),
                event_date=datetime.now(),
                event_type=EventType.NEW,
                categories=[Category('cs.DL')],
                version=1
            ),
            Event(
                arxiv_id=Identifier('2901.00341'),
                event_date=datetime.now() - timedelta(days=2),
                event_type=EventType.REPLACED,
                categories=[Category('cs.IR')],
                version=2)
            )
        r.save(s)
        response = s.client.list_objects_v2(Bucket=s._bucket)

        keys = set()
        for obj in response['Contents']:
            keys.add(obj['Key'])
        expected_keys = set([
            'global.manifest.json',
            'announcement.manifest.json',
            'announcement/2019.manifest.json',
            'announcement/2019/2019-08.manifest.json',
            'announcement/2019/08/26/2019-08-26-listing.json',
            'announcement/2019/08/28/2019-08-28-listing.json'
        ])
        self.assertSetEqual(keys, expected_keys,
                            'Manifests and listing files are created')

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