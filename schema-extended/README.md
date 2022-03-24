# arXiv Extended Metadata Schema
## 2022-03-23

Files in this folder represent work done in collaboration with
the metadata working group (Cornell/arXiv, CERN/INSPIRE, others).
As of this writing, the fifth draft is complete. Drafts 2-5
were discussed in PRs:

- [Draft 5](https://github.com/arXiv/arxiv-canonical/pull/36)
- [Draft 4](https://github.com/arXiv/arxiv-canonical/pull/34)
- [Draft 3](https://github.com/arXiv/arxiv-canonical/pull/33)
- [Draft 2](https://github.com/arXiv/arxiv-canonical/pull/32)

## Schema structure

One of the goals of this new schema is to be flexible and modular, so it is spread across multiple files.

``Document.json`` is the main/parent schema for documents; it captures all the versions of a particular document.

``Version.json`` includes the majority of the metadata field
definitions.
