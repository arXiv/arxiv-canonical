# arXiv canonical repository

Provides a RESTful JSON API that exposes information about e-prints and 
e-print events.

## API

### ``/e-print/{IDENTIFIER}``

Retrieve an overview of events for a particular e-print. Includes refs to all
of its versions.

### ``/e-print/{IDENTIFIER}v{VERSION}``

Retrieve metadata for an e-print version.

### ``/e-print/{IDENTIFIER}v{VERSION}/events``

Retrieve events for an e-print version.

### ``/e-print/{IDENTIFIER}v{VERSION}/pdf``

Retrieve the first-compiled PDF of the e-print.

### ``/e-print/{IDENTIFIER}v{VERSION}/source``

Retrieve the source package for the e-print.

### ``/listing/{YEAR}/{MONTH?}/{DAY?}?skip={N}&show={M}&classification={ARCHIVE_OR_CATEGORY}``

Retrieve e-print events for a given year, month, day. 

### ``/listing/pastweek``

Retrieve e-print events for the past seven days.