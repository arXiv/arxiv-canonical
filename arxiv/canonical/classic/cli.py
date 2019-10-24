"""Command-line tools for the classic record."""

import os
from datetime import date
from typing import Optional

import click

from ..services import CanonicalFilesystem, Filesystem, RemoteSource
from . import backfill as _backfill


@click.group()
def cli():
    pass


@click.command()
@click.argument('record_path')
@click.argument('classic_path', default='/data')
@click.argument('daily_path', default='/data/logs_archive/misc/daily.log')
@click.argument('ps_cache_path', default='/cache')
@click.option('--state-path', default=None, type=str)
def backfill(record_path: str,
             classic_path: str,
             daily_path: str,
             ps_cache_path: str,
             state_path: Optional[None] = None,
             cache_path: Optional[str] = None,
             until: Optional[date] = None) -> None:
    """
    Backfill the canonical record from the classic record.

    Parameters
    ----------
    record_path : str
        Full path to the target canonical record.
    classic_path : str
        Path to data directory containing orig/, ftp/.
    daily_path : str
        Full path to the daily.log file.
    ps_cache_path : str
        Full path to the directory containing ps_cache/.


    """
    if state_path is None:
        state_path = './.backfill'
    if cache_path is None:
        cache_path = './.backfill/cache'
    storage = CanonicalFilesystem(record_path)
    classic = Filesystem(classic_path)
    remote = RemoteSource('arxiv.org')
    api = RegisterAPI(storage, [storage, classic, remote])
    _backfill.backfill(api, daily_path, classic_path, ps_cache_path,
                       self.state_path,
                                    limit_to=set(self.identifiers),
                                    cache_path=self.cache_path)


cli.add_command(backfill)


if __name__ == '__main__':
    cli()