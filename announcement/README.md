# arXiv announcement agent

The announcement agent is responsible for adding new e-prints to the canonical
record.

## Version 0 : Clone legacy announcement record to canonical format

In v0 of the announcement agent, the e-print event consumer...

- Processes events from a Kinesis stream (see below).
- Retrieves metadata, content for e-prints from the legacy system.
- Uses ``arxiv.canonical`` to update the canonical record in the cloud.

## Events

The legacy system produces e-print events on a Kinesis stream called
``Announce``. Each message has the structure:

```json
{
    "event_type": "...",
    "identifier": "...",
    "version": "...",
    "timestamp": "..."    
}
```

``event_type`` may be one of:

| Event type | Description                                                   |
|------------|---------------------------------------------------------------|
| new        | An e-print is announced for the first time.                   |
| updated    | An e-print is updated without producing a new version.        |
| replaced   | A new version of an e-print is announced.                     |                                             |
| cross-list | Cross-list classifications are added for an e-print.          |
| withdrawn  | An e-print is withdrawn. This generates a new version.        |

``identifier`` is an arXiv identifier; see :class:`.Identifier`. 

``version`` is a positive integer.

``timestamp`` is an ISO-8601 datetime, localized to UTC.

## Legacy integration

Metadata, PDFs, and source are retrieved from the legacy system via HTTP 
request.

- Metadata: arxiv.org/docmeta/{IDENTIFIER}v{VERSION}
- PDF: arxiv.org/pdf/{IDENTIFIER}v{VERSION}
- Source: arxiv.org/src/{IDENTIFIER}v{VERSION}


# Contributing 

For a list of things that need doing, please see the issues tracker for this
repository.

## Quick-start

We use [Pipenv](https://github.com/pypa/pipenv) for dependency management.

```bash
pipenv install --dev
```

You can run either the API or the UI using the Flask development server.

```bash
FLASK_APP=ui.py FLASK_DEBUG=1 pipenv run flask run
```

Dockerfiles are also provided in the root of this repository. These use uWSGI and the
corresponding ``wsgi_[xxx].py`` entrypoints.

## Contributor guidelines

Please see the [arXiv contributor
guidelines](https://github.com/arXiv/.github/blob/master/CONTRIBUTING.md) for
tips on getting started.

## Code of Conduct

All contributors are expected to adhere to the [arXiv Code of
Conduct](https://arxiv.org/help/policies/code_of_conduct).