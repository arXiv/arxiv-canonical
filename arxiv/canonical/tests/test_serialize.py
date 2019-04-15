"""Tests for :mod:`arxiv.canonical.serialize`."""

from unittest import TestCase
import json
import os

import jsonschema

from .. import domain, serialize


class TestSerializeDeserialize(TestCase):
    """Test serializing and deserializing canonical domain objects."""

    def test_license(self):
        """Serialize and deserialize a :class:`.License`."""
        license = domain.License('http://some.license', 'The Some License')
        self.assertEqual(license, serialize.loads(serialize.dumps(license)))

    def test_classification(self):
        """Serialize and deserialize a :class:`.Classification`."""
        clsn = domain.Classification(
            group=domain.ClassificationTerm("bar", "Bar Group"),
            archive=domain.ClassificationTerm("foo", "Foo Archive"),
            category=domain.ClassificationTerm("baz", "Baz Domain"),
        )
        self.assertEqual(clsn, serialize.loads(serialize.dumps(clsn)))


class TestAgainstSchema(TestCase):
    """Test serialized domain objects against JSON schema."""

    SCHEMA_PATH = os.path.abspath('schema/')

    def setUp(self):
        """Get a JSON Schema reference resolver."""
        resolver_path = 'file://%s/' % self.SCHEMA_PATH
        self.resolver = jsonschema.RefResolver(resolver_path, None)

    def test_license(self):
        """Serialized :class:`.License` should conform to schema."""
        with open(os.path.join(self.SCHEMA_PATH, 'License.json')) as f:
            schema = json.load(f)

        license = domain.License('http://some.license', 'The Some License')

        self.assertIsNone(
            jsonschema.validate(json.loads(serialize.dumps(license)),
                                schema,
                                resolver=self.resolver)
        )

    def test_classification(self):
        """Serialized :class:`.Classification` should conform to schema."""
        with open(os.path.join(self.SCHEMA_PATH, 'Classification.json')) as f:
            schema = json.load(f)

        clsn = domain.Classification(
            group=domain.ClassificationTerm("bar", "Bar Group"),
            archive=domain.ClassificationTerm("foo", "Foo Archive"),
            category=domain.ClassificationTerm("baz", "Baz Domain"),
        )
        self.assertIsNone(
            jsonschema.validate(json.loads(serialize.dumps(clsn)), schema,
                                resolver=self.resolver)
        )
