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
    "Person.json",
    "References.json",
    "RelatedLink.json",
    "Role.json",
    "Taxonomy.json",
    "Version.json"
]
for sf in schema_files:
    schema = json.loads(open(sf).read())
    schema_store[schema.get("$id", sf)] = schema

document = json.loads(open("Document.json").read())
resolver = RefResolver.from_schema(document, store=schema_store)
Validator = validator_for(document)
validator = Validator(document, resolver=resolver)


@click.command()
@click.option(
    "--instance",
    "-i",
    type=click.Path(exists=True),
    help="path to instance json",
)
def main(instance: str) -> None:
    with open(instance) as instance_file:
        instance_json = json.loads(instance_file.read())
    validator.validate(instance_json)


if __name__ == "__main__":
    main()
