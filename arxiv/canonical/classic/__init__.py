"""
(De)Serialization of the classic announcement record.

There are two sources of information that can be used to piece together the
announcement history of the classic record:

1. The daily.log file contains a daily record of new, replacement, and
   cross-list announcements.
2. The classic abs file contains version metadata.
"""