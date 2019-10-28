# arXiv NG Canonical Record

This repository contains a library and applications for working with the core
arXiv canonical record. The canonical record is the authoritative history and
state for announced e-prints on the arXiv platform.

Work on this project will proceed in two phases, each corresponding to a major
version:

## Version 0: Replication of the Legacy Record to the Canonical Record

The first major objective of this project is to replicate all of the core
announcement events that occur in the legacy system to the cloud-native
canonical record.

- The legacy system emits event notifications via a Kinesis stream for new
  e-prints, replacements, cross-listing, withdrawals, and updates.
- An announcement agent (``announcement/`` in this repo)...

  - consumes legacy events,
  - retrieves metadata, source package, and first-compiled PDF from legacy,
  - formats and stores content as part of the canonical record. The canonical
    record is designed to work on any storage platform that supports a
    key-value behavior. The first implementation will target AWS S3.

- The repository service (``repository/`` in this repo) makes e-print metadata,
  content, and events available via a RESTful JSON API. This is a Flask
  application that will be deployed as a Docker container.

Both the ``announcement/`` and ``repository/`` applications use the
``arxiv.canonical`` package (``arxiv/canonical/`` in this repo) to interact
with the canonical record.

## Version 1: Orchestration of the Announcement Process

Once several other dependencies are resolved in the legacy system, this project
will assume primary responsibility for announcing submitted e-prints on a
daily basis. This is a bit further down the road.

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
FLASK_APP=app.py FLASK_DEBUG=1 pipenv run flask run
```

Dockerfiles are also provided in the root of this repository. These use uWSGI
and the corresponding ``wsgi_[xxx].py`` entrypoints.

## AWS services, mocking

It's helpful to use a live API when developing components against AWS services.
We use [Localstack](https://github.com/localstack/localstack) for this
purpose.

## Contributor guidelines

Please see the [arXiv contributor
guidelines](https://github.com/arXiv/.github/blob/master/CONTRIBUTING.md) for
tips on getting started.

## Code of Conduct

All contributors are expected to adhere to the [arXiv Code of
Conduct](https://arxiv.org/help/policies/code_of_conduct).

# Background

## Record design goals

The goals for the canonical record itself, in priority order:

1. Record article source and metadata
2. Record announcement record (what articles were announced on what day)
3. Long term durability: low risk of corruption, easy to secure, easy to
   verify, easy to repair
4. Changes to articles are only though the publication system and are recorded
   in the publication records. Write access to the canonical record will be
   limited to the announcement process.
5. Works with with Legacy and seamless transition from Legacy to NG
6. Testable
7. Easy to backup, replicate, mirror, share
8. Minimal dependencies
9. Conceptually as simple as possible
10. Complete public readability of data

## Overview
The canonical record can be stored on any system that supports a key-binary
data structure, such as a filesystem or an object store. The two core data
structures in the record are:

1. E-prints, comprised of...

  - metadata,
  - submitted content,
  - and the first rendering of the PDF.

2. Announcement records, representing a single announcement-related event, such
   as a new version, a withdrawal, or a cross-list; these records are:

  - organized into daily announcement listing files and
  - emitted via a notification broker in real time, to trigger updates to
    downstream services and data stores.

## E-prints
An e-print is comprised of (1) a metadata record, (2) a source package,
containing the original content provided by the submitter, and (3) a canonical
rendering of the e-print in PDF format. A manifest is also stored for each
e-print, containing the keys for the resources above and a base-64 encoded MD5
hash of their binary content.

The key prefix structure for an e-print record is:

```
e-prints/<YYYY>/<MM>/<arXiv ID>/v<version>/
```

Where ``YYYY`` is the year and ``MM`` the month during which the first version
of the e-print was announced.

Sub-keys are:

- Metadata record: ``<arXiv ID>v<version>.json``
- Source package: ``<arXiv ID>v<version>.tar``
- PDF: ``<arXiv ID>v<version>.pdf``
- Manifest: ``<arXiv ID>v<version>.manifest.json``

The purpose of the e-print record is to provide the ultimate source of truth
regarding a particular e-print, and facilitate ease of access to all of the
versions of an e-print.

## Announcement listings
The announcement listings commemorate the announcement-related events that
occur on a given day. This includes new e-prints/versions, withdrawals,
cross-lists, etc.

The key prefix structure for an announcement listing file is:

```
announcement/<YYYY>/<MM>/<DD>/
```

Each daily key prefix may contain one or more sub-keys. Each sub-key ending in
.json is treated as a listing file. This allows for the possibility of
sharded/multi-threaded announcement processes that write separate listing
files, e.g. for specific classification domains.

``YYYY`` is the year, ``MM`` the month, and ``DD`` the day on which the
announcement events encoded therein occurred and on which the subordinate
listing files were generated.

## Preservation record
The preservation record is a daily digest containing e-print content,
announcement listings, and any suppress or remove directives (with
corresponding tombstones).


```
announcement/<listing>.json
e-prints/<arXiv ID>v<version>/
    <arXiv ID>v<version>.json         # Metadata record
    <arXiv ID>v<version>.tar           # Source package
    <arXiv ID>v<version>.pdf              # First PDF
    <arXiv ID>v<version>.manifest.json    # Manifest.
suppress/<arXiv ID>v<version>/tombstone
preservation.manifest.json
```

The preservation.manifest.json record is similar to the e-print manifest
record; it contains all of the keys and corresponding checksums for the items
in the preservation record.

## Differences from legacy arXiv

### Metadata

- Legacy metadata records use an arcane, non-standard serialization format. The
  NG metadata record is serialized as JSON and stored as UTF-8 encoded text
  files.
- Legacy metadata records contain information not suitable for uncontrolled
  public distribution (such as submitter e-mail addresses), which limits
  replication options and increases complexity. NG metadata records contain
  only metadata suitable for public consumption, and can be distributed without
  further processing.
- In the legacy system, if the whole record of changes to an article is needed,
  many listings files have to be visited. The NG metadata record includes
  history/changes element to the metadata record, the elements of which are
  announcement records, i.e. updates to that particular version (timestamp +
  description).

### Announcement listings

- Legacy arXiv uses monthly listing records, whereas NG listing records are
  created on a daily basis. This cuts down on the number of secondary
  indices/files that must be consistent to provide daily and weekly listings on
  the public site.

- The legacy system splits listings out by classification archive/category.
  This bakes the arXiv classification taxonomy into the structure of the data.
  The NG listing record decouples classification concerns from data structure;
  no assumptions are made about the classification of the e-prints described in
  an announcement listing record.
