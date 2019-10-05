"""Tests for :mod:`arxiv.canonical.serialize`."""

from unittest import TestCase
import json
import os

import jsonschema

from ... import domain, serialize


class TestSerializeDeserialize(TestCase):
    """Test serializing and deserializing canonical domain objects."""

    def test_license(self):
        """Serialize and deserialize a :class:`.License`."""
        license = domain.License('http://some.license')
        self.assertEqual(license, serialize.loads(serialize.dumps(license)))


class TestAgainstSchema(TestCase):
    """Test serialized domain objects against JSON schema."""

    SCHEMA_PATH = os.path.abspath('schema/resources')

    def setUp(self):
        """Get a JSON Schema reference resolver."""
        resolver_path = 'file://%s/' % self.SCHEMA_PATH
        self.resolver = jsonschema.RefResolver(resolver_path, None)

    def test_license(self):
        """Serialized :class:`.License` should conform to schema."""
        with open(os.path.join(self.SCHEMA_PATH, 'License.json')) as f:
            schema = json.load(f)

        license = domain.License('http://some.license')

        self.assertIsNone(
            jsonschema.validate(json.loads(serialize.dumps(license)),
                                schema,
                                resolver=self.resolver)
        )
