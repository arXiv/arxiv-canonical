"""Transform Classic abs files into NG canonical format."""

import os
from typing import Iterable

import click

from arxiv import canonical


@click.command()
@click.option('--abs', help='Path to directory with classic abs files')
@click.option('--daily', help='Path to daily.log file')
@click.option('--output', help='Path to directory for NG canonical data')
def transform(abs: str, daily: str, output: str):
    """."""
    if not os.path.exists(abs):
        raise RuntimeError(f'No such path: {abs}')
    if not os.path.exists(daily):
        raise RuntimeError(f'No such path: {daily}')
    if not os.path.exists(output):
        os.makedirs(output)

    iter_data = (canonical.serialize.classic.parse(abs_path)
                 for abs_path in iter_abs(input))

    for record in iter_data:
        print(record.version, record.comments)


def iter_abs(input: str) -> Iterable[str]:
    """Grab paths to abs files from the ``input`` directory."""
    for parent, dirs, fnames in os.walk(input):
        for fname in fnames:
            if fname.endswith('.abs'):
                yield os.path.join(parent, fname)


if __name__ == '__main__':
    transform()
