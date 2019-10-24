"""Command-line tools for the classic record."""

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
def backfill(record_path: str,
             classic_path: str,
             daily_path: str,
             ps_cache_path: str,
             state_path: Optional[str] = None,
             cache_path: Optional[str] = None,
             until: Optional[datetime] = None) -> None:
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

    """
    if state_path is None:
        state_path = './.backfill'
    if cache_path is None:
        cache_path = './.backfill/cache'
    storage = CanonicalFilesystem(record_path)
    classic = Filesystem(classic_path)
    remote = RemoteSource('arxiv.org')
    api = RegisterAPI(storage, [storage, classic, remote])

    until_date: Optional[date] = None if not until else until.date()

    for event in _backfill.backfill(api, daily_path, classic_path,
                                    ps_cache_path, state_path,
                                    cache_path=cache_path, until=until_date):
        click.echo(f'{event.event_date}'
                   f'\t{event.identifier}'
                   f'\t{event.event_type.value}')


cli.add_command(backfill)


if __name__ == '__main__':
    cli()