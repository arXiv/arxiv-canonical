"""
Command-line tools for the classic record.

Next step: propagating events during backfill
=============================================
In the current implementation, the legacy record is used to backfill the
NG canonical record on a start-up and daily basis (after announcement for that
day is complete). Especially for the daily update, it will be desirable to
also propagate the backfilled events on the announcement event stream. Use
the implementation in :mod:`arxiv.canonical.services.stream` to emit events
as they are yielded by the backfill/backfill_today function.

Note that this assumes that a minimal version of the NG canonical repository
application is running and accessible to consumers, who will need to retrieve
bitstreams identified by canonical URIs.
"""

import os
from datetime import date, datetime
from typing import Optional

import click

from ..register import RegisterAPI
from ..services import CanonicalFilesystem, Filesystem, RemoteSource
from . import backfill as _backfill


@click.group()
def cli() -> None:
    """Placeholder for the CLI command group provided by this module."""
    pass


# The \b on its own line in the docstring is because click's docstring parsing
# kind of sucks:
# https://click.palletsprojects.com/en/7.x/documentation/?highlight=help#preventing-rewrapping
@click.command('backfill',
               short_help='Backfill canonical record from the classic record.')
@click.argument('record_path')
@click.argument('classic_path', default='/data')
@click.argument('daily_path', default='/data/logs_archive/misc/daily.log')
@click.argument('ps_cache_path', default='/cache')
@click.option('--state-path', default=None, type=str)
@click.option('--until', default=None, type=click.DateTime(['%Y-%m-%d']),   # pylint: disable=no-member
              help='If provided, will only backfill up to the specified date.')
@click.option('--remote', default='arxiv.org', type=str,
              help='Host to use when formats are missing from ps_cache')
def backfill(record_path: str,
             classic_path: str,
             daily_path: str,
             ps_cache_path: str,
             state_path: Optional[str] = None,
             cache_path: Optional[str] = None,
             until: Optional[datetime] = None,
             remote: str = 'arxiv.org') -> None:
    """
    Backfill the canonical record from the classic record.

    TODO: add support for ``s3://`` path for ``record_path``.

    \b
    Parameters
    ----------
    record_path : str
        Full path to the target canonical record.
    classic_path : str
        Path to data directory containing orig/, ftp/. Default: ``/data``.
    daily_path : str
        Full path to the daily.log file. Default:
        ``/data/logs_archive/misc/daily.log``.
    ps_cache_path : str
        Full path to the directory containing ps_cache/. Default: ``/cache``.
    state_path : str
        Path for the backfill state. Allows re-starting from the last
        successfully handled event. Default: ``.backfill/`` in the CWD.
    cache_path : str
        Path for the backfill cache. Used to cache expensive metadata about
        classic bitstreams. Default: ``.backfill/cache/`` in the CWD.
    until : date
        If provided, will only backfill up to the specified date.
    remote : str
        Host to use when formats are missing from ps_cache.

    """
    if state_path is None:
        state_path = './.backfill'
    if cache_path is None:
        cache_path = './.backfill/cache'
    storage = CanonicalFilesystem(record_path)
    classic = Filesystem(classic_path)
    remote_source = RemoteSource(remote)
    api = RegisterAPI(storage, [storage, classic, remote_source])

    until_date: Optional[date] = None if not until else until.date()

    for event in _backfill.backfill(api, daily_path, classic_path,
                                    ps_cache_path, state_path,
                                    cache_path=cache_path, until=until_date):
        click.echo(f'{event.event_date}'
                   f'\t{event.identifier}'
                   f'\t{event.event_type.value}')


@click.command('backfill_today',
               short_help='Backfill today\'s events from the classic record.')
@click.argument('record_path')
@click.argument('classic_path', default='/data')
@click.argument('daily_path', default='/data/logs_archive/misc/daily.log')
@click.argument('ps_cache_path', default='/cache')
@click.option('--state-path', default=None, type=str)
@click.option('--until', default=None, type=click.DateTime(['%Y-%m-%d']),   # pylint: disable=no-member
              help='If provided, will only backfill up to the specified date.')
@click.option('--remote', default='arxiv.org', type=str,
              help='Host to use when formats are missing from ps_cache')
def backfill_today(record_path: str,
                   classic_path: str,
                   daily_path: str,
                   ps_cache_path: str,
                   state_path: Optional[str] = None,
                   cache_path: Optional[str] = None,
                   remote: str = 'arxiv.org') -> None:
    """
    Backfill today\'s events from the classic record.

    This is a unique case, in that we are able to directly infer the version
    associated with each event based on the most recent abs file for each
    e-print.

    TODO: add support for ``s3://`` path for ``record_path``.

    \b
    Parameters
    ----------
    record_path : str
        Full path to the target canonical record.
    classic_path : str
        Path to data directory containing orig/, ftp/. Default: ``/data``.
    daily_path : str
        Full path to the daily.log file. Default:
        ``/data/logs_archive/misc/daily.log``.
    ps_cache_path : str
        Full path to the directory containing ps_cache/. Default: ``/cache``.
    state_path : str
        Path for the backfill state. Allows re-starting from the last
        successfully handled event. Default: ``.backfill/`` in the CWD.
    cache_path : str
        Path for the backfill cache. Used to cache expensive metadata about
        classic bitstreams. Default: ``.backfill/cache/`` in the CWD.
    remote : str
        Host to use when formats are missing from ps_cache.

    """
    if state_path is None:
        state_path = './.backfill'
    if cache_path is None:
        cache_path = './.backfill/cache'
    storage = CanonicalFilesystem(record_path)
    classic = Filesystem(classic_path)
    remote_source = RemoteSource(remote)
    api = RegisterAPI(storage, [storage, classic, remote_source])

    for event in _backfill.backfill_today(api, daily_path, classic_path,
                                          ps_cache_path, state_path,
                                          cache_path=cache_path):
        click.echo(f'{event.event_date}'
                   f'\t{event.identifier}'
                   f'\t{event.event_type.value}')


cli.add_command(backfill)


if __name__ == '__main__':
    cli()