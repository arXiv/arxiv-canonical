"""Jsonschema validation script that supports file $refs."""
import click
import json
from jsonschema import RefResolver, Draft7Validator
from jsonschema import validate, RefResolver
from jsonschema.validators import validator_for

schema_store = {}
schema_files = [
    "Affiliation.json",
    "Common.json",
    "Document.json",
    "Funding.json",
    "Keyword.json",
    "LanguageCode.json",
    "License.json",
    "Author.json",
    "References.json",
    "RelatedLink.json",
    "Role.json",
    "Taxonomy.json",
    "Version.json"
]
try:
    for sf in schema_files:
        schema = json.loads(open(sf).read())
        schema_store[schema.get("$id", sf)] = schema
except Exception:
    print(f"Could not load {sf}")

@click.command()
@click.option(
    "--validation_schema",
    "-s",
    type=click.Path(exists=True),
    help="schema to validate against",
    default="Document.json"
)
@click.option(
    "--instance",
    "-i",
    type=click.Path(exists=True),
    help="path to instance json",
)
def main(instance: str, validation_schema: str) -> None:
    schema_json = json.loads(open(validation_schema).read())
    resolver = RefResolver.from_schema(schema_json, store=schema_store)
    Validator = validator_for(schema_json)
    validator = Validator(schema_json, resolver=resolver)

    with open(instance) as instance_file:
        instance_json = json.loads(instance_file.read())
    validator.validate(instance_json)


if __name__ == "__main__":
    main()
