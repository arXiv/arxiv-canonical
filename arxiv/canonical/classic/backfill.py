"""
Methods for backfilling the NG record from classic.

What version is this?
=====================
The lacuna of the classic record is an unambiguous mapping between announcement
events and specific versions of an e-print. For example, if we encounter a
replacement event in the daily.log file, there is no explicit indication of
whether the resulting version is 2, 3, or some higher value. The abs file
does not provide this information either, as only the submission date of each
version is preserved (although this could at least be used as a lower bound).
So, we need to get creative.

Start at the beginning of time. Initialize a counter that keeps track of the
last version number seen for each e-print identifier.

Prior to the start of the daily.log (mid-1998): Read the abs file for each
e-print, and generate a ``new`` and subsequent ``replace`` event(s) using the
submission date(s) as the announcement date(s).

Read daily.log in order. Rely on the version number mapping to keep track of
where we are with each e-print.
"""
import logging
import os

from collections import Counter
from datetime import date, datetime
from itertools import chain
from operator import attrgetter

from typing import Iterable, List, Mapping, MutableMapping, Optional, Set, \
    Tuple

from pytz import timezone
from pprint import pprint

from ..domain import Event, Identifier, EventType, VersionedIdentifier, \
    Version, Metadata, EventSummary, Category
from ..log import Log, WRITE
from ..register import IRegisterAPI
from . import abs, daily, content
from .util import PersistentIndex, PersistentList

logger = logging.getLogger(__name__)
logger.setLevel(int(os.environ.get('LOGLEVEL', '40')))

ET = timezone('US/Eastern')


def backfill(register: IRegisterAPI,
             daily_path: str,
             abs_path: str,
             state_path: str,
             limit_to: Optional[Set[Identifier]] = None,
             cache_path: Optional[str] = None) -> Iterable[Event]:
    first = PersistentIndex()
    first.load(os.path.join(state_path, 'first.json'))
    current = PersistentIndex()
    current.load(os.path.join(state_path, 'current.json'))
    log = Log(state_path)

    resume_after = log.read_last_succeeded()
    skip = True if resume_after else False
    event: Optional[Event] = None

    logger.info(f'Start backfill; skip = {skip} (until {resume_after})')
    try:
        for event in load_all(daily_path, abs_path, current, first,
                              limit_to=limit_to, cache_path=cache_path):
            if skip:
                if resume_after and event.event_id == resume_after.event_id:
                    skip = False    # Start on the next event.
                continue
            logger.info(f'Handling: {event.event_date}: {event.identifier}')
            register.add_events(event)
            log.log_success(event.event_id, WRITE)
            yield event
    except Exception as e:
        if event:
            log.log_failure(event.event_id, WRITE,
                            message=f'Encountered error: {e}')

        raise
    finally:
        first.save()
        current.save()


def load_today(daily_path: str, abs_path: str,
               first: MutableMapping[Identifier, date],
               cache_path: Optional[str] = None) -> Iterable[Event]:
    """
    Load the events that were generated today.

    This is a unique case, in that we are able to directly infer the version
    associated with each event based on the most recent abs file for each
    e-print.
    """

    for datum in daily.parse(daily_path, for_date=datetime.now(ET).date(),
                             cache_path=cache_path):
        abs_datum = abs.parse_latest(abs_path, datum.arxiv_id)
        yield _make_event(abs_path, abs_datum, datum, abs_datum.identifier,
                          first)


def load_all(daily_path: str, abs_path: str,
             current: MutableMapping[Identifier, int],
             first: MutableMapping[Identifier, date],
             limit_to: Optional[Set[Identifier]] = None,
             cache_path: Optional[str] = None) -> Iterable[Event]:
    logger.info(f'Load events from {daily_path}')
    # We can't infer whether an abs file was written prior to the daily.log
    # from the abs file alone. But if the e-print identifier comes prior to the
    # earlier identifier for a ``new`` event in the daily.log, then we can
    # be certain it is not covered in the daily.log.
    first_entry = next(iter(daily.parse(daily_path, cache_path=cache_path)))
    logger.info(f'First: {first_entry.event_date}: {first_entry.arxiv_id}')

    first_day = daily.parse(daily_path, for_date=first_entry.event_date,
                            cache_path=cache_path)
    new_identifiers = sorted([Identifier(ed.arxiv_id) for ed in first_day
                              if ed.event_type is EventType.NEW])
    logger.info(f'Found {len(new_identifiers)} NEW events on the first day')
    first_identifier = new_identifiers[0]
    logger.info(f'The earliest NEW identifier is {first_identifier}')
    # These are e-prints that were first announced prior to the beginning of
    # the daily.log file, i.e. we have no ``new`` event.
    ids_prior_to_first_event = \
        list(abs.iter_all(abs_path, to_id=first_identifier))

    # Load all of the pre-daily events at once.
    logger.info('Loading pre-daily events for %i identifiers...',
                len(ids_prior_to_first_event))
    predaily_events: List[Event] = []
    for ident in ids_prior_to_first_event:
        if limit_to and ident not in limit_to:
            continue
        for event in _load_predaily(daily_path, abs_path, ident, current,
                                    first, cache_path=cache_path):
            predaily_events.append(event)
    predaily_events = sorted(predaily_events, key=attrgetter('event_date'))

    logger.info('Loaded %i pre-daily events', len(predaily_events))

    # Lazily load the daily events.
    daily_events = _load_events(abs_path, daily_path, current, first,
                                limit_to=limit_to, cache_path=cache_path)
    return chain(predaily_events, daily_events)


def _load_events(abs_path: str, daily_path: str,
                 current: MutableMapping[Identifier, int],
                 first: MutableMapping[Identifier, date],
                 limit_to: Optional[Set[Identifier]] = None,
                 cache_path: Optional[str] = None) -> Iterable[Event]:
    for event_datum in daily.parse(daily_path, cache_path=cache_path):
        if limit_to and event_datum.arxiv_id not in limit_to:
            continue
        yield _load_daily_event(abs_path, event_datum, current, first)


def _load_daily_event(abs_path: str, event_datum: daily.EventData,
                      current: MutableMapping[Identifier, int],
                      first: MutableMapping[Identifier, date]) -> Event:
    identifier = _make_id(event_datum, current)

    abs_datum = abs.parse(abs_path, identifier)
    assert abs_datum.identifier == identifier  # Loaded the correct abs file.

    event = _make_event(abs_path, abs_datum, event_datum, identifier, first)

    current[event.identifier.arxiv_id] = event.identifier.version
    if event.identifier.version == 1:
        first[event.identifier.arxiv_id] = event.event_date
    return event


def _make_event(abs_path: str, abs_datum: abs.AbsData,
                event_datum: daily.EventData,
                identifier: VersionedIdentifier,
                first: MutableMapping[Identifier, date]) -> Event:

    # Look up the date that the first version of this e-print was announced.
    if identifier.version > 1:
        announced_date_first = first[event_datum.arxiv_id]
    else:
        announced_date_first = event_datum.event_date

    primary, secondary = _make_categories(event_datum, abs_datum)
    if abs_datum.submission_type == EventType.WITHDRAWN:
        event_type = EventType.WITHDRAWN
        is_withdrawn = True
    else:
        event_type = event_datum.event_type
        is_withdrawn = False
    version = Version(
        identifier=identifier,
        announced_date=event_datum.event_date,
        announced_date_first=announced_date_first,
        submitted_date=abs_datum.submitted_date,
        updated_date=abs_datum.updated_date,
        metadata=Metadata(
            primary_classification=primary,
            secondary_classification=secondary,
            title=abs_datum.title,
            abstract=abs_datum.abstract,
            authors=abs_datum.authors,
            license=abs_datum.license,
            comments=abs_datum.comments,
            journal_ref=abs_datum.journal_ref,
            report_num=abs_datum.report_num,
            doi=abs_datum.doi,
            msc_class=abs_datum.msc_class,
            acm_class=abs_datum.acm_class
        ),
        events=[],
        submitter=abs_datum.submitter,
        proxy=abs_datum.proxy,
        is_announced=True,
        is_withdrawn=is_withdrawn,
        is_legacy=True,
        source=content.get_source(abs_path, identifier),
        render=content.get_render(abs_path, identifier),
        source_type=abs_datum.source_type
    )
    event = Event(
        identifier=abs_datum.identifier,
        event_date=_datetime_from_date(event_datum.event_date,
                                       identifier.arxiv_id),
        event_type=event_type,
        is_legacy=True,
        version=version,
        categories=event_datum.categories
    )
    version.events.append(event.summary)
    return event


def _load_predaily(daily_path: str, abs_path: str,
                   identifier: Identifier,
                   current: MutableMapping[Identifier, int],
                   first: MutableMapping[Identifier, date],
                   cache_path: Optional[str] = None) -> List[Event]:
    """
    Generate inferred events prior to daily.log based on abs files.

    Approach:

    - v1 announced date is the v1 submission date
    - if there are multiple versions:
      - scan the daily.log for all replacements of that e-print
      - align from the most recent version, backward
      - if there are any remaining versions between v1 and the lowest v from
        the previous step, use the submission date for that v from the abs
        file as the announced date.
    - if we have explicit cross-list events, exclude those crosses from any
      events that we generate here.

    """
    events: List[Event] = []
    abs_for_this_ident = sorted(abs.parse_versions(abs_path, identifier),
                                key=lambda a: a.identifier.version)
    N_versions = len(abs_for_this_ident)
    events_for_this_ident = sorted(daily.scan(daily_path, identifier,
                                              cache_path=cache_path),
                                   key=lambda d: d.event_date)
    # These result in new versions.
    replacements = [e for e in events_for_this_ident
                    if e.event_type == EventType.REPLACED]
    # These do not.
    crosslists = [e for e in events_for_this_ident
                  if e.event_type == EventType.CROSSLIST]

    # If there are more replacement events than we have abs beyond the
    # first version, we're in trouble.
    assert len(replacements) < len(abs_for_this_ident)

    # Work backward, since we do not know whether there were replacements
    # prior to the start of the daily.log file.
    repl_map = {}
    for i, event in enumerate(replacements[::-1]):
        repl_map[abs_for_this_ident[-(i + 1)].identifier.version] = event

    # Generate replacement events as needed, and remove cross-list
    # categories for which we have explicit CROSSLIST events in daily.
    for i, abs_datum in enumerate(abs_for_this_ident):
        if abs_datum.identifier.version in repl_map:
            event_date = _datetime_from_date(
                repl_map[abs_datum.identifier.version].event_date,
                identifier
            )
        else:
            # We don't know the announcement date, so we will fall back to
            # the submission date for this abs version.
            event_date = _datetime_from_date(abs_datum.submitted_date,
                                             identifier)

        # Some of the abs categories may have been added after the
        # initial new/replacement event. We want to pare out those
        # secondaries, since they were not actually present.
        while crosslists and crosslists[0].event_date < event_date.date():
            cross = crosslists.pop(0)
            last = events[-1]
            last.version.metadata.secondary_classification = [
                c for c in last.version.metadata.secondary_classification
                if c not in cross.categories
            ]

        # If we have aligned an abs version with an event from daily.log,
        # we will skip it for now; we will handle all events from daily.log
        # in order later on.
        if abs_datum.identifier.version not in repl_map:
            # This event is inferred from the presence of an abs file.
            events.append(_event_from_abs(abs_path, abs_datum, event_date))
            current[events[-1].identifier.arxiv_id] \
                = events[-1].identifier.version
            if events[-1].identifier.version == 1:
                first[events[-1].identifier.arxiv_id] = event_date
    return events


def _make_id(event_datum: daily.EventData,
             current: MutableMapping[Identifier, int]) -> VersionedIdentifier:
    if event_datum.event_type == EventType.NEW:
        identifier = VersionedIdentifier.from_parts(event_datum.arxiv_id, 1)
    elif event_datum.event_type.is_new_version:
        identifier = VersionedIdentifier.from_parts(
            event_datum.arxiv_id,
            current[event_datum.arxiv_id] + 1
        )
    else:
        identifier = VersionedIdentifier.from_parts(
            event_datum.arxiv_id,
            current[event_datum.arxiv_id]
        )
    return identifier


def _make_categories(event_datum: daily.EventData, abs_datum: abs.AbsData) \
        -> Tuple[Category, List[Category]]:
    if event_datum.event_type.is_new_version:
        primary_classification = event_datum.categories[0]
        secondary_classification = event_datum.categories[1:]
    else:
        primary_classification = abs_datum.primary_classification
        secondary_classification = abs_datum.secondary_classification
    # else:
    #     raise RuntimeError(f'Unxpected event type: {event_datum.event_type}')
    return primary_classification, secondary_classification


def _event_from_abs(abs_path: str, abs_data: abs.AbsData,
                    event_date: datetime) -> Event:
    version = Version(
        identifier=abs_data.identifier,
        announced_date=abs_data.submitted_date,
        announced_date_first=abs_data.submitted_date,
        submitted_date=event_date,
        updated_date=event_date,
        metadata=Metadata(
            primary_classification=abs_data.primary_classification,
            secondary_classification=abs_data.secondary_classification,
            title=abs_data.title,
            abstract=abs_data.abstract,
            authors=abs_data.authors,
            license=abs_data.license,
            comments=abs_data.comments,
            journal_ref=abs_data.journal_ref,
            report_num=abs_data.report_num,
            doi=abs_data.doi,
            msc_class=abs_data.msc_class,
            acm_class=abs_data.acm_class
        ),
        events=[],
        submitter=abs_data.submitter,
        proxy=abs_data.proxy,
        is_announced=True,
        is_withdrawn=False,
        is_legacy=True,
        source=content.get_source(abs_path, abs_data.identifier),
        render=content.get_render(abs_path, abs_data.identifier),
        source_type=abs_data.source_type
    )
    event = Event(
        identifier=abs_data.identifier,
        event_date=event_date,
        event_type=(EventType.NEW
                    if abs_data.identifier.version == 1
                    else EventType.REPLACED),
        is_legacy=True,
        version=version
    )
    version.events.append(event.summary)
    return event


def _event_from_cross(abs_datum: abs.AbsData, categories: List[Category],
                      event_date: datetime) -> Event:
    ...


def _datetime_from_date(source_date: date, identifier: Identifier) -> datetime:
    # We are artificially coercing a date value to a datetime, which
    # means that every event on a particular day will occur at
    # precisely the same moment. In order to preserve event order, we
    # set the microsecond part based on the arXiv ID.
    return datetime(year=source_date.year,
                    month=source_date.month,
                    day=source_date.day,
                    hour=20,
                    minute=0,
                    second=0,
                    microsecond=int(identifier.incremental_part),
                    tzinfo=ET)