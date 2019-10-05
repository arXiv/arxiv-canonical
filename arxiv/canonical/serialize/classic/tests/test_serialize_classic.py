"""Tests for :mod:`.serialize.classic`."""

from unittest import TestCase
import os
import json
from pprint import pprint

import jsonschema

from .... import serialize
from ... import classic

DATA = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')
ABS_ROOT = os.path.join(DATA, 'orig')

ALL_ABS = [os.path.join(base, fname)
           for base, dirs, fnames in os.walk(ABS_ROOT)
           for fname in fnames if fname.endswith('.abs')]


class TestClassicDeserialize(TestCase):
    """Test deserialization of the classic abs format."""

    SCHEMA_PATH = os.path.abspath('schema/resources')

    def setUp(self):
        """Get a JSON Schema reference resolver."""
        resolver_path = 'file://%s/' % self.SCHEMA_PATH
        self.resolver = jsonschema.RefResolver(resolver_path, None)

    def test_parse(self):
        """Can parse and reserialize classic abs records."""
        with open(os.path.join(self.SCHEMA_PATH, 'Abs.json')) as f:
            schema = json.load(f)

        for abs in ALL_ABS:
            self.assertIsNone(
                jsonschema.validate(
                    json.loads(serialize.dumps(classic.abs.parse(abs))),
                    schema,
                    resolver=self.resolver
                ),
                f"Abs file {abs} is parsed successfully"
            )
