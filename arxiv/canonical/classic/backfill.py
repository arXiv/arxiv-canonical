"""
Functions for backfilling the NG record from classic.

In order to ensure a smooth transition from classic to the NG announcement
process, we need to be able to initially operate both the classic and NG
canonical records in parallel. This means that we need to be able to:

1. Backfill the canonical record from the classic record, starting at the
   beginning of time and running up to the present. See :func:`backfill`.
2. Continuously update the canonical record from data in the classic system.
   See :func:`backfill_today`.

This module is implemented on the assumption that its functions will be
executed on a machine with access to the classic filesystem, specifically to
the abs/source files and daily.log file. It is agnostic, however, about the
target storage medium for the canonical record. So this these functions can be
used to backfill the canonical record both on local filesystems and in (for
example) an S3 bucket.

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
from functools import partial
from itertools import chain
from operator import attrgetter

from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, \
    Set, Tuple

from pytz import timezone
from pprint import pprint
from typing_extensions import Protocol

from ..domain import CanonicalFile, Category, ContentType, Event, EventSummary, EventType, \
    Identifier, Metadata, Version, VersionedIdentifier
from ..log import Log, WRITE
from ..register import IRegisterAPI
from . import abs, daily, content
from .util import PersistentIndex, PersistentList

logger = logging.getLogger(__name__)
logger.setLevel(int(os.environ.get('LOGLEVEL', '40')))

ET = timezone('US/Eastern')


class _ILoader(Protocol):
    """
    Interface for functions that load events from the classic record.

    This is here mostly because the semantics for ``typing.Callable`` are
    pretty limited.
    """

    def __call__(self,
                 current: Optional[MutableMapping[Identifier, int]] = None,
                 first: Optional[MutableMapping[Identifier, date]] = None,
                 limit_to: Optional[Set[Identifier]] = None,
                 cf_cache: Optional['_CF_PersistentIndex'] = None) \
            -> Iterable[Event]:
        """Load events from the classic record."""


_CF_Key = Tuple[VersionedIdentifier, ContentType]


class _CF_PersistentIndex(PersistentIndex):
    def __getitem__(self, key: _CF_Key) -> CanonicalFile:
        skey = f'{key[0]}::{key[1].value}'
        cf: CanonicalFile = super(_CF_PersistentIndex, self).__getitem__(skey)
        return cf

    def __setitem__(self, key: _CF_Key, value: CanonicalFile) -> None:
        skey = f'{key[0]}::{key[1].value}'
        return super(_CF_PersistentIndex, self).__setattr__(skey, value)

    def __contains__(self, key: Any) -> bool:
        if not isinstance(key, tuple):
            return False
        skey = f'{key[0]}::{key[1].value}'
        return bool(super(_CF_PersistentIndex, self).__contains__(skey))


def backfill(register: IRegisterAPI,
             daily_path: str,
             abs_path: str,
             ps_cache_path: str,
             state_path: str,
             limit_to: Optional[Set[Identifier]] = None,
             cache_path: Optional[str] = None,
             until: Optional[date] = None) -> Iterable[Event]:
    """
    Lazily backfill the canonical record from the classic record.

    Note: you **must** consume this iterator in order for backfilling to occur.
    This was implemented lazily because there is considerable I/O (including
    possibly some over the network), and being able to control processing rate
    at a high level was foreseen as important.

    Parameters
    ----------
    register : :class:`IRegisterAPI`
        A canonical register instance that will handle events derived from the
        classic record.
    daily_path : str
        Absolute path to the daily.log file.
    abs_path : str
        Absolute path of the directory containing abs files and source
        packages. Specifically, this is the directory that contains the ``ftp``
        and ``orig`` subdirectories.
    state_path : str
        Absolute path of a writeable directory where backfill state can be
        stored. This allows us to persist the backfill state, in case we need
        to restart after a failure.
    limit_to : set
        A set of :class:`Identifier`s indicating a subset of e-prints to
        backfill. If ``None`` (default) all events for all e-prints are
        backfilled.
    cache_path : str
        If provided, a writable directory where a cache of events can be
        maintained. This cuts down on spin-up time considerably.

    Returns
    -------
    iterator
        Yields :class:`Event`s that have been successfully backfilled.

    """
    loader = partial(_load_all, daily_path, abs_path, ps_cache_path,
                     cache_path=cache_path)
    return _backfill(register, loader, state_path, limit_to=limit_to,
                     until=until)


def backfill_today(register: IRegisterAPI,
                   daily_path: str,
                   abs_path: str,
                   ps_cache_path: str,
                   state_path: str,
                   cache_path: Optional[str] = None) -> Iterable[Event]:
    """
    Lazily backfill the canonical record from today's events in classic record.

    This is intended to be used to keep the canonical record up to date from
    the classic record on a daily basis, after the initial backfill.

    Note: you **must** consume this iterator in order for backfilling to occur.
    This was implemented lazily because there is considerable I/O (including
    possibly some over the network), and being able to control processing rate
    at a high level was foreseen as important.

    Parameters
    ----------
    register : :class:`IRegisterAPI`
        A canonical register instance that will handle events derived from the
        classic record.
    daily_path : str
        Absolute path to the daily.log file.
    abs_path : str
        Absolute path of the directory containing abs files and source
        packages. Specifically, this is the directory that contains the ``ftp``
        and ``orig`` subdirectories.
    state_path : str
        Absolute path of a writeable directory where backfill state can be
        stored. This allows us to persist the backfill state, in case we need
        to restart after a failure.
    cache_path : str
        If provided, a writable directory where a cache of events can be
        maintained. This cuts down on spin-up time considerably.

    Returns
    -------
    iterator
        Yields :class:`Event`s that have been successfully backfilled.

    """
    loader = partial(_load_today, daily_path, abs_path, ps_cache_path,
                     cache_path=cache_path)
    return _backfill(register, loader, state_path)


def _backfill(register: IRegisterAPI,
              loader: _ILoader,
              state_path: str,
              limit_to: Optional[Set[Identifier]] = None,
              until: Optional[date] = None) -> Iterable[Event]:
    # These mappings are stored on disk so that we can resume after a failure.
    # They will be created now if they don't already exist.
    first = PersistentIndex()
    first.load(os.path.join(state_path, 'first.json'))
    current = PersistentIndex()
    current.load(os.path.join(state_path, 'current.json'))
    cf_cache = _CF_PersistentIndex()
    cf_cache.load(os.path.join(state_path, 'content.json'))
    log = Log(state_path)   # The log keeps track of our progress.

    # We may be resuming after a failure. If so, we will start right after the
    # last successful event.
    resume_after = log.read_last_succeeded()
    skip = True if resume_after else False
    event: Optional[Event] = None

    logger.info(f'Starting backfill')   # The logger is just for us humans.
    if skip:
        logger.info(f'Skip until {resume_after}')
    i = 0
    try:
        # Because of the format of daily.log, it's not at all straightforward
        # to skip unwanted events without fully parsing them. In this
        # implementation all events are loaded and we are just choosy about
        # which ones we work with. At this level of the process, we are
        # skipping any events that were already backfilled successfully on
        # previous runs. Filtering for ``limit_to`` happens deeper.
        for event in loader(current=current, first=first, limit_to=limit_to,
                            cf_cache=cf_cache):
            logger.debug(f'Got event %s for %s (%s)', event.event_id,
                         event.identifier, event.event_date)
            if skip:
                if resume_after and event.event_id == resume_after.event_id:
                    skip = False    # Start on the next event.
                continue

            if until and event.event_date.date() >= until:
                # Explicitly instructed to stop processing when this date is
                # reached.
                break

            logger.info(f'Handling: {event.event_date}: {event.identifier}')
            register.add_events(event)  # Add event to the canonical record!
            log.log_success(event.event_id, WRITE)  # Mark our progress.
            i += 1
            logger.debug('Successfully handled %i events', i)
            cf_cache.save()   # Only save if successful.
            yield event
    except Exception as e:
        logger.error('Encountered unhandled exception: %s (%s)', e, type(e))
        if event:
            # Log lines are stored as json, so newlines in the exception should
            # not cause problems.
            log.log_failure(event.event_id, WRITE,
                            message=f'Encountered error: {e}')

        raise
    finally:
        # Keep our version and announcement-date mappings up to date on disk.
        first.save()
        current.save()


# Private functions follow in alphabetic order.

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


def _event_from_abs(abs_path: str, ps_cache_path: str, abs_data: abs.AbsData,
                    event_date: datetime,
                    cf_cache: Optional[_CF_PersistentIndex] = None) -> Event:

    source = content.get_source(abs_path, abs_data.identifier)
    formats = {cf.content_type: cf
               for cf in content.get_formats(abs_path, ps_cache_path,
                                             abs_data.identifier,
                                             abs_data.source_type, source,
                                             cf_cache=cf_cache)}
    render = formats.get(ContentType.pdf, None)
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
        source=source,
        render=render,
        formats=formats,
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


def _load_all(daily_path: str,
              abs_path: str,
              ps_cache_path: str,
              current: Optional[MutableMapping[Identifier, int]] = None,
              first: Optional[MutableMapping[Identifier, date]] = None,
              cf_cache: Optional[_CF_PersistentIndex] = None,
              limit_to: Optional[Set[Identifier]] = None,
              cache_path: Optional[str] = None) -> Iterable[Event]:
    """
    Load all classic events, using both abs files and the daily.log.

    Pre-daily.log events are inferred from the abs files. Events derived from
    daily.log are supplemented by the abs files to infer things like version
    number.

    Parameters
    ----------
    daily_path : str
        Absolute path to the daily.log file.
    abs_path : str
        Absolute path of the directory containing abs files and source
        packages. Specifically, this is the directory that contains the ``ftp``
        and ``orig`` subdirectories.
    current : mapping
        A ``dict`` or other mutable mapping of :class:`Identifier`s onto the
        most recent loaded numeric version of the corresponding e-print.
    first : mapping
        A ``dict`` or other mutable mapping of :class:`Identifier`s onto the
        announcement date of the first version of the corresponding e-print.
    limit_to : set
        A set of :class:`Identifier`s indicating a subset of e-prints to load.
        If ``None`` (default) all events for all e-prints are loaded.
    cache_path : str
        If provided, a writable directory where a cache of events can be
        maintained. This cuts down on spin-up time considerably.

    Returns
    -------
    iterator
        Yields :class:`Event`s in chronological order.

    """
    if current is None:
        current = {}
    if first is None:
        first = {}

    logger.info(f'Load events from {daily_path}')
    # We can't infer whether an abs file was written prior to the daily.log
    # from the abs file alone. But if the e-print identifier comes prior to the
    # earlier identifier for a ``new`` event in the daily.log, then we can
    # be certain it is not covered in the daily.log.
    first_entry = next(
        iter(daily.parse(daily_path, cache_path=cache_path)),
        None
    )
    if first_entry is None:
        raise RuntimeError('Could not load the first entry from daily.log')
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
        for event in _load_predaily(daily_path, abs_path, ps_cache_path, ident,
                                    current, first, cache_path=cache_path,
                                    cf_cache=cf_cache):
            predaily_events.append(event)
    predaily_events = sorted(predaily_events, key=attrgetter('event_date'))

    logger.info('Loaded %i pre-daily events', len(predaily_events))

    # Lazily load the daily events.
    daily_events = _load_events(abs_path, daily_path, ps_cache_path,
                                current, first, limit_to=limit_to,
                                cache_path=cache_path,
                                cf_cache=cf_cache)
    return chain(predaily_events, daily_events)


def _load_daily_event(abs_path: str, ps_cache_path: str,
                      event_datum: daily.EventData,
                      current: MutableMapping[Identifier, int],
                      first: MutableMapping[Identifier, date],
                      cf_cache: Optional[_CF_PersistentIndex] = None) -> Event:
    identifier = _make_id(event_datum, current)

    abs_datum = abs.parse(abs_path, identifier)

    if abs_datum.identifier != identifier:  # Loaded the correct abs file?
        raise RuntimeError(f'Loaded the wrong abs file! Expected {identifier},'
                           f' got {abs_datum.identifier}. This may be because'
                           f' the abs file for {identifier} is missing.')

    event = _make_event(abs_path, ps_cache_path, abs_datum, event_datum,
                        identifier, first, cf_cache=cf_cache)

    current[event.identifier.arxiv_id] = event.identifier.version
    if event.identifier.version == 1:
        first[event.identifier.arxiv_id] = event.event_date
    return event


def _load_events(abs_path: str, daily_path: str, ps_cache_path: str,
                 current: MutableMapping[Identifier, int],
                 first: MutableMapping[Identifier, date],
                 limit_to: Optional[Set[Identifier]] = None,
                 cache_path: Optional[str] = None,
                 cf_cache: Optional[_CF_PersistentIndex] = None) \
        -> Iterable[Event]:
    for event_datum in daily.parse(daily_path, cache_path=cache_path):
        if limit_to and event_datum.arxiv_id not in limit_to:
            continue
        yield _load_daily_event(abs_path, ps_cache_path, event_datum, current,
                                first, cf_cache=cf_cache)


def _load_predaily(daily_path: str, abs_path: str, ps_cache_path: str,
                   identifier: Identifier,
                   current: MutableMapping[Identifier, int],
                   first: MutableMapping[Identifier, date],
                   cache_path: Optional[str] = None,
                   cf_cache: Optional[_CF_PersistentIndex] = None) \
        -> List[Event]:
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
            events.append(_event_from_abs(abs_path, ps_cache_path, abs_datum,
                                          event_date, cf_cache=cf_cache))
            current[events[-1].identifier.arxiv_id] \
                = events[-1].identifier.version
            if events[-1].identifier.version == 1:
                first[events[-1].identifier.arxiv_id] = event_date
    return events


def _load_today(daily_path: str,
                abs_path: str,
                ps_cache_path: str,
                first: Optional[MutableMapping[Identifier, date]] = None,
                cache_path: Optional[str] = None,
                **_: Any) -> Iterable[Event]:
    """
    Load the events that were generated today.

    This is a unique case, in that we are able to directly infer the version
    associated with each event based on the most recent abs file for each
    e-print.

    Parameters
    ----------
    daily_path : str
        Absolute path to the daily.log file.
    abs_path : str
        Absolute path of the directory containing abs files and source
        packages. Specifically, this is the directory that contains the ``ftp``
        and ``orig`` subdirectories.
    first : mapping
        A ``dict`` or other mutable mapping of :class:`Identifier`s onto the
        announcement date of the first version of the corresponding e-print.
    cache_path : str
        If provided, a writable directory where a cache of events can be
        maintained. This cuts down on spin-up time considerably.

    Returns
    -------
    iterator
        Yields :class:`Event`s in chronological order.

    """
    if first is None:
        first = {}

    for datum in daily.parse(daily_path, for_date=datetime.now(ET).date(),
                             cache_path=cache_path):
        abs_datum = abs.parse_latest(abs_path, datum.arxiv_id)
        yield _make_event(abs_path, ps_cache_path, abs_datum, datum,
                          abs_datum.identifier, first)


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


def _make_event(abs_path: str, ps_cache_path: str, abs_datum: abs.AbsData,
                event_datum: daily.EventData,
                identifier: VersionedIdentifier,
                first: MutableMapping[Identifier, date],
                cf_cache: Optional[_CF_PersistentIndex] = None) -> Event:

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

    source = content.get_source(abs_path, identifier)
    formats = {cf.content_type: cf
               for cf in content.get_formats(abs_path, ps_cache_path,
                                             identifier, abs_datum.source_type,
                                             source, cf_cache=cf_cache)}
    render = formats.get(ContentType.pdf, None)

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
        source=source,
        render=render,
        formats=formats,
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
