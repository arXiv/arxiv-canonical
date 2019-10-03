"""Tests for :mod:`.register."""

import boto3
import io
import os
import json
import tempfile
from datetime import datetime, date, timedelta
from pprint import pprint
from unittest import TestCase, mock
from uuid import UUID

from moto import mock_s3
from pytz import UTC

from ..domain import Listing, Event, EventType, Identifier, Category, \
    ContentType, Version, VersionedIdentifier, Metadata, License, \
    EventIdentifier, CanonicalFile, EventSummary, ListingIdentifier, URI
from ..services import store, filesystem
from ..register.api import RegisterAPI
from ..role import Primary


def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


class TestAddFromFilesystem(TestCase):
    @mock_s3
    def test_add_eprints_from_scratch(self):
        """EPrints do not yet exist."""
        tempdir = tempfile.mkdtemp()
        source_uri = URI(os.path.join(tempdir, 'foosource.tar.gz'))
        with open(source_uri.path, 'wb') as f:
            f.write(b'fakecontent')

        pdf_uri = URI(os.path.join(tempdir, 'foo.pdf'))
        with open(pdf_uri.path, 'wb') as f:
            f.write(b'fakepdfcontent')

        s = store.CanonicalStore('foobucket')
        s.inititalize()
        fs = filesystem.Filesystem(tempdir)

        r = Primary(s, [fs, s], mock.MagicMock()).register

        identifier = VersionedIdentifier('2901.00345v1')
        created = datetime(2029, 1, 29, 20, 4, 23, tzinfo=UTC)
        # event_id = EventIdentifier.from_parts(identifier, created, '0')
        event = Event(
            identifier=identifier,
            event_date=created,
            event_type=EventType.NEW,
            categories=[Category('cs.DL')],
            version=Version(
                identifier=identifier,
                announced_date=created.date(),
                announced_date_first=created.date(),
                submitted_date=created,
                updated_date=created,
                is_announced=True,
                events=[],
                previous_versions=[],
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
                    ref=URI('arxiv:///foosource.tar.gz')
                ),
                render=CanonicalFile(
                    filename='2901.00345v1.pdf',
                    created=created,
                    modified=created,
                    size_bytes=404,
                    content_type=ContentType.pdf,
                    ref=URI('arxiv:///foo.pdf')
                )
            )
        )
        event_data = event.to_dict()
        event_data['version']['source']['content'] = source_uri
        event_data['version']['render']['content'] = pdf_uri

        e = Event.from_dict(event_data)
        # print(e.version.source.content, type(e.version.source.content))
        # r.add_events(event)

        # version = r.load_version(VersionedIdentifier('2901.00345v1'))
        # self.assertEqual(version.identifier,
        #                  VersionedIdentifier('2901.00345v1'),
        #                  'Loads the version of interest')
        # self.assertEqual(version.metadata, event.version.metadata,
        #                  'Metadata is preserved with fidelity')
        # self.assertIsNotNone(version.source)
        # self.assertIsNotNone(version.source.content)
        # self.assertEqual(version.source.content.read(), b'fakecontent',
        #                  'Content of the source file is available')
        # self.assertIsNotNone(version.render)
        # self.assertIsNotNone(version.render.content)
        # self.assertEqual(version.render.content.read(), b'fakepdfcontent',
        #                  'Content of the render file is available')
        # response = s.client.list_objects_v2(Bucket=s._bucket)

        # keys = set()
        # for obj in response['Contents']:
        #     keys.add(obj['Key'])

        # print('-----'*10)
        # ev, n = r.load_events(2029)
        # for e in ev:
        #     print('!', e.version.source.__dict__)

        # print('????')
        # print(r.load_version('2901.00345v1').source.content)



class TestLoadVersions(TestCase):
    @mock_s3
    def test_add_eprints_from_scratch(self):
        """EPrints do not yet exist."""
        s = store.CanonicalStore('foobucket')
        s.inititalize()

        r = Primary(s, [s], mock.MagicMock()).register

        # r = RegisterAPI(s)
        identifier = VersionedIdentifier('2901.00345v1')
        created = datetime(2029, 1, 29, 20, 4, 23, tzinfo=UTC)
        # event_id = EventIdentifier.from_parts(identifier, created, '0')
        event = Event(
            identifier=identifier,
            event_date=created,
            event_type=EventType.NEW,
            categories=[Category('cs.DL')],
            version=Version(
                identifier=identifier,
                announced_date=created.date(),
                announced_date_first=created.date(),
                submitted_date=created,
                updated_date=created,
                is_announced=True,
                events=[],
                previous_versions=[],
                metadata=Metadata(
                    primary_classification=Category('cs.DL'),
                    secondary_classification=[Category('cs.IR')],
                    title='Foo title',
                    abstract='It is abstract',
                    authors='Ima N. Author (FSU)',
                    license=License(href="http://some.license")
                ),
                source=CanonicalFile(filename='2901.00345v1.tar.gz',
                                     created=created,
                                     modified=created,
                                     size_bytes=4_304,
                                     content_type=ContentType.targz,
                                     ref=URI('arxiv:///foosource.tar.gz')
                                     ), # content=io.BytesIO(b'fakecontent')
                render=CanonicalFile(
                    filename='2901.00345v1.pdf',
                    created=created,
                    modified=created,
                    size_bytes=404,
                    content_type=ContentType.pdf,
                    ref=URI('arxiv:///foo.pdf')
                    # content=io.BytesIO(b'fakepdfcontent')
                )
            )
        )
        r.add_events(event)

        version = r.load_version(VersionedIdentifier('2901.00345v1'))
        self.assertEqual(version.identifier,
                         VersionedIdentifier('2901.00345v1'),
                         'Loads the version of interest')
        self.assertEqual(version.metadata, event.version.metadata,
                         'Metadata is preserved with fidelity')
        self.assertIsNotNone(version.source)
        self.assertIsNotNone(version.source.content)
        self.assertEqual(version.source.content.read(), b'fakecontent',
                         'Content of the source file is available')
        self.assertIsNotNone(version.render)
        self.assertIsNotNone(version.render.content)
        self.assertEqual(version.render.content.read(), b'fakepdfcontent',
                         'Content of the render file is available')
        response = s.client.list_objects_v2(Bucket=s._bucket)

        keys = set()
        for obj in response['Contents']:
            keys.add(obj['Key'])

        print('-----'*10)
        ev, n = r.load_events(2029)
        for e in ev:
            print('!', e.version.source.__dict__)

        print('????')
        print(r.load_version('2901.00345v1').source.content)


#     @mock_s3
#     def test_load_eprint(self):

#         s = store.CanonicalStore('foobucket')
#         s.inititalize()

#         r = RegisterAPI(s)
#         identifier = VersionedIdentifier('2901.00345v1')
#         created = datetime(2029, 1, 29, 20, 4, 23, tzinfo=UTC)
#         event_id = EventIdentifier()
#         event = Event(
#             uuid=event_id,
#             identifier=identifier,
#             event_date=created,
#             event_type=EventType.NEW,
#             categories=[Category('cs.DL')],
#             version=Version(
#                 identifier=VersionedIdentifier('2901.00345v1'),
#                 announced_date=created.date(),
#                 announced_date_first=created.date(),
#                 submitted_date=created,
#                 updated_date=created,
#                 is_announced=True,
#                 events=[event_id],
#                 previous_versions=[],
#                 metadata=Metadata(
#                     primary_classification=Category('cs.DL'),
#                     secondary_classification=[Category('cs.IR')],
#                     title='Foo title',
#                     abstract='It is abstract',
#                     authors='Ima N. Author (FSU)',
#                     license=License(href="http://some.license")
#                 ),
#                 source=CanonicalFile(
#                     filename='2901.00345v1.tar.gz',
#                     created=created,
#                     modified=created,
#                     size_bytes=4_304,
#                     content_type=ContentType.targz,
#                     content=io.BytesIO(b'fakecontent')
#                 ),
#                 render=CanonicalFile(
#                     filename='2901.00345v1.pdf',
#                     created=created,
#                     modified=created,
#                     size_bytes=404,
#                     content_type=ContentType.pdf,
#                     content=io.BytesIO(b'fakepdfcontent')
#                 )
#             )
#         )
#         created = datetime(2029, 1, 30, 20, 4, 23, tzinfo=UTC)
#         event2_id = UUID(hex='15a0e35e8e154624becbf615aaffdcc9')
#         event2 = Event(
#             uuid=event2_id,
#             identifier=VersionedIdentifier('2901.00345v2'),
#             event_date=created,
#             event_type=EventType.REPLACED,
#             categories=[Category('cs.DL')],
#             version=Version(
#                 identifier=VersionedIdentifier('2901.00345v2'),
#                 announced_date=created.date(),
#                 announced_date_first=created.date(),
#                 submitted_date=created,
#                 updated_date=created,
#                 is_announced=True,
#                 events=[event2_id],
#                 previous_versions=[],
#                 metadata=Metadata(
#                     primary_classification=Category('cs.DL'),
#                     secondary_classification=[Category('cs.IR')],
#                     title='Foo title',
#                     abstract='It is still abstract',
#                     authors='Ima N. Author (FSU)',
#                     license=License(href="http://some.license")
#                 ),
#                 source=CanonicalFile(
#                     filename='2901.00345v2.tar.gz',
#                     created=created,
#                     modified=created,
#                     size_bytes=4_304,
#                     content_type=ContentType.targz,
#                     content=io.BytesIO(b'morefakecontent')
#                 ),
#                 render=CanonicalFile(
#                     filename='2901.00345v2.pdf',
#                     created=created,
#                     modified=created,
#                     size_bytes=404,
#                     content_type=ContentType.pdf,
#                     content=io.BytesIO(b'morefakepdfcontent')
#                 )
#             )
#         )
#         r.add_events(s, event, event2)
#         r.save(s)

#         eprint = r.load_eprint(s, Identifier('2901.00345'))
#         self.assertEqual(len(eprint.versions), 2)


# class TestAddEPrints(TestCase):
#     @mock_s3
#     def test_add_eprints_from_scratch(self):
#         """EPrints do not yet exist."""
#         s = store.CanonicalStore('foobucket')
#         s.inititalize()

#         r = Register.load(s, 'all')
#         created = datetime(2029, 1, 29, 20, 4, 23, tzinfo=UTC)
#         event_id = UUID(hex='95a0e35e8e144624becbf615aaffdcc9')
#         r.add_events(
#             s,
#             Event(
#                 uuid=event_id,
#                 identifier=VersionedIdentifier('2901.00345v1'),
#                 event_date=created,
#                 event_type=EventType.NEW,
#                 categories=[Category('cs.DL')],
#                 version=Version(
#                     identifier=VersionedIdentifier('2901.00345v1'),
#                     announced_date=created.date(),
#                     announced_date_first=created.date(),
#                     submitted_date=created,
#                     updated_date=created,
#                     is_announced=True,
#                     events=[event_id],
#                     previous_versions=[],
#                     metadata=Metadata(
#                         primary_classification=Category('cs.DL'),
#                         secondary_classification=[Category('cs.IR')],
#                         title='Foo title',
#                         abstract='It is abstract',
#                         authors='Ima N. Author (FSU)',
#                         license=License(href="http://some.license")
#                     ),
#                     source=CanonicalFile(
#                         filename='2901.00345v1.tar.gz',
#                         created=created,
#                         modified=created,
#                         size_bytes=4_304,
#                         content_type=ContentType.targz,
#                         content=io.BytesIO(b'fakecontent')
#                     ),
#                     render=CanonicalFile(
#                         filename='2901.00345v1.pdf',
#                         created=created,
#                         modified=created,
#                         size_bytes=404,
#                         content_type=ContentType.pdf,
#                         content=io.BytesIO(b'fakepdfcontent')
#                     )
#                 )
#             )
#         )
#         r.save(s)
#         response = s.client.list_objects_v2(Bucket=s._bucket)

#         keys = set()
#         for obj in response['Contents']:
#             keys.add(obj['Key'])
#             # print(repr(obj['Key']))

#         expected_metadata = {
#             '@type': 'Version',
#             'announced_date': '2029-01-29',
#             'announced_date_first': '2029-01-29',
#             'identifier': '2901.00345v1',
#             'is_announced': True,
#             'is_legacy': False,
#             'is_withdrawn': False,
#             'events': [
#                 {'@type': 'UUID', 'hex': '95a0e35e8e144624becbf615aaffdcc9'}
#             ],
#             'metadata': {
#                 '@type': 'Metadata',
#                 'abstract': 'It is abstract',
#                 'acm_class': None,
#                 'authors': 'Ima N. Author (FSU)',
#                 'comments': None,
#                 'doi': None,
#                 'journal_ref': None,
#                 'license': {'@type': 'License', 'href': 'http://some.license'},
#                 'msc_class': None,
#                 'primary_classification': 'cs.DL',
#                 'report_num': None,
#                 'secondary_classification': ['cs.IR'],
#                 'title': 'Foo title'
#             },
#             'previous_versions': [],
#             'proxy': None,
#             'reason_for_withdrawal': None,
#             'render': {
#                 '@type': 'CanonicalFile',
#                 'content_type': 'pdf',
#                 'created': '2029-01-29T20:04:23+00:00',
#                 'filename': '2901.00345v1.pdf',
#                 'modified': '2029-01-29T20:04:23+00:00',
#                 'size_bytes': 404
#             },
#             'source': {
#                 '@type': 'CanonicalFile',
#                 'content_type': 'targz',
#                 'created': '2029-01-29T20:04:23+00:00',
#                 'filename': '2901.00345v1.tar.gz',
#                 'modified': '2029-01-29T20:04:23+00:00',
#                 'size_bytes': 4304
#             },
#             'source_type': None,
#             'submitted_date': '2029-01-29T20:04:23+00:00',
#             'submitter': None,
#             'updated_date': '2029-01-29T20:04:23+00:00'
#         }

#         expected_listing = {
#             '@type': 'Listing',
#             'date': '2029-01-29',
#             'events': [{'@type': 'Event',
#                         'categories': ['cs.DL'],
#                         'description': '',
#                         'event_agent': None,
#                         'event_date': '2029-01-29T20:04:23+00:00',
#                         'event_type': 'new',
#                         'identifier': '2901.00345v1',
#                         'legacy': False,
#                         'uuid': {'@type': 'UUID',
#                                  'hex': '95a0e35e8e144624becbf615aaffdcc9'},
#                         'version': expected_metadata}]
#         }

#         expected = {
#             'global.manifest.json': {
#                 "entries": [
#                     {'checksum': 'jVKl_YSPzQRw1wu6ezT7iw==', 'key': 'listings'},
#                     {'checksum': 'RxawUTP5PiQ8YszzhpP_GA==', 'key': 'eprints'}
#                 ]
#             },
#             'e-prints.manifest.json': {
#                 "entries": [
#                     {"key": "2029", "checksum": "WQW4AHXrQZCZZLa7fKj8lw=="}
#                 ]
#             },
#             'e-prints/2029.manifest.json': {
#                 "entries": [
#                     {"key": "2029-01", "checksum": "993eTgsdpxMGcLLRxU8VBQ=="}
#                 ]
#             },
#             'e-prints/2029/2029-01.manifest.json': {
#                 "entries": [
#                     {"key": "2029-01-29",
#                      "checksum": "NJjv9tlVzuyRGjDD9WHdzw=="}
#                 ]
#             },
#             'e-prints/2029/01/2029-01-29.manifest.json': {
#                 "entries": [
#                     {"key": "2901.00345",
#                      "checksum": "7qc3fKQJwO67NzJ9_cko3Q=="}
#                 ]
#             },
#             'e-prints/2029/01/2901.00345.manifest.json': {
#                 "entries": [
#                     {"key": "2901.00345v1",
#                      "checksum": "PgNWXljO-jzMXgCAocdBpg=="}
#                 ]
#             },
#             'e-prints/2029/01/2901.00345/2901.00345v1.manifest.json': {
#                 "entries": [
#                     {"key": "e-prints/2029/01/2901.00345/v1/2901.00345v1.json",
#                      "checksum": "IRoI6f8bPjF8wC46xIyPuw=="},
#                     {"key": "e-prints/2029/01/2901.00345/v1/2901.00345v1.pdf",
#                      "checksum": "faMW1JRszQ9WF7PMbJt21w=="},
#                     {"key":
#                          "e-prints/2029/01/2901.00345/v1/2901.00345v1.tar.gz",
#                      "checksum": "iwS4H0Y-JpPbFaxAZeEv4w=="}
#                 ]
#             },
#             'e-prints/2029/01/2901.00345/v1/2901.00345v1.json':
#                 expected_metadata,
#             'e-prints/2029/01/2901.00345/v1/2901.00345v1.pdf':
#                 b'fakepdfcontent',
#             'e-prints/2029/01/2901.00345/v1/2901.00345v1.tar.gz':
#                 b'fakecontent',
#             'announcement.manifest.json': {
#                 'entries': [
#                     {'checksum': '1v2-kDGs7ulgj-x21ayxCQ==', 'key': '2029'}
#                 ]
#             },
#             'announcement/2029.manifest.json': {
#                 'entries': [
#                     {'checksum': '5z_KFg8EutC14yXxHzFUTA==', 'key': '2029-01'}
#                 ]
#             },
#             'announcement/2029/01/29/2029-01-29-listing.json': expected_listing,
#             'announcement/2029/2029-01.manifest.json': {
#                 'entries': [
#                     {'checksum': 'F39iadmHujy_weFQnaYoWw==',
#                      'key': '2029-01-29'}
#                 ]
#             }
#         }
#         self.assertSetEqual(keys, set(expected.keys()),
#                             'Manifests and e-print files are created')
#         # Verify record content.
#         for key in expected.keys():
#             resp = s.client.get_object(Bucket=s._bucket, Key=key)
#             content = resp['Body'].read()
#             if key.endswith('.json'):
#                 self.assertEqual(ordered(json.loads(content)),
#                                  ordered(expected[key]),
#                                  f'JSON data are stored correctly ({key})')
#         #     else:
#         #         self.assertEqual(content, expected[key],
#         #                          'Bitstream content is stored correctly')


# # class TestAddEvents(TestCase):
# #     @mock_s3
# #     def test_add_events_from_scratch(self):
# #         """Listing files do not yet exist."""
# #         s = store.CanonicalStore('foobucket')
# #         s.inititalize()

# #         r = Register.load(s, 'all')
# #         created = datetime(2029, 8, 28, 20, 4, 23, tzinfo=UTC)
# #         r.add_events(
# #             s,
# #             Event(
# #                 arxiv_id=Identifier('2901.00345'),
# #                 event_date=created,
# #                 event_type=EventType.NEW,
# #                 categories=[Category('cs.DL')],
# #                 version=1
# #             ),
# #             Event(
# #                 arxiv_id=Identifier('2901.00341'),
# #                 event_date=created - timedelta(days=2),
# #                 event_type=EventType.REPLACED,
# #                 categories=[Category('cs.IR')],
# #                 version=2)
# #             )
# #         r.save(s)
# #         response = s.client.list_objects_v2(Bucket=s._bucket)

# #         keys = set()
# #         for obj in response['Contents']:
# #             keys.add(obj['Key'])
# #         expected_keys = set([
# #             'global.manifest.json',
# #             'announcement.manifest.json',
# #             'announcement/2029.manifest.json',
# #             'announcement/2029/2029-08.manifest.json',
# #             'announcement/2029/08/26/2029-08-26-listing.json',
# #             'announcement/2029/08/28/2029-08-28-listing.json'
# #         ])
# #         self.assertSetEqual(keys, expected_keys,
# #                             'Manifests and listing files are created')

