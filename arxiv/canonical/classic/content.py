import logging
import os
from datetime import datetime
from typing import Optional

from pytz import timezone

from .. import domain as D
from ..services import RemoteSource

logger = logging.getLogger(__name__)
logger.setLevel(int(os.environ.get('LOGLEVEL', '40')))

ET = timezone('US/Eastern')

REMOTE: Optional['RemoteSourceWithHead'] = None


def get_latest_source_path(data_path: str, identifier: D.Identifier) -> str:
    prefix = identifier.category_part if identifier.is_old_style else 'arxiv'
    return os.path.join(data_path, 'ftp', prefix, 'papers', identifier.yymm,
                        f'{identifier}.tar.gz')


def get_source_path(data_path: str, identifier: D.VersionedIdentifier) -> str:
    prefix = identifier.category_part if identifier.is_old_style else 'arxiv'
    return os.path.join(data_path, 'orig', prefix, 'papers', identifier.yymm,
                        f'{identifier}.tar.gz')


def get_source(data_path: str, identifier: D.VersionedIdentifier) \
        -> D.CanonicalFile:
    logger.debug(f'Getting source for {identifier}')
    path = get_source_path(data_path, identifier)
    mtime = datetime.utcfromtimestamp(os.path.getmtime(path)).astimezone(ET)
    return D.CanonicalFile(
        created=mtime,
        modified=mtime,
        size_bytes=os.path.getsize(path),
        content_type=D.ContentType.targz,
        ref=D.URI(path),
        filename=os.path.split(path)[1]
    )


def get_render(data_path: str, identifier: D.VersionedIdentifier) \
        -> D.CanonicalFile:
    logger.debug(f'Getting render for {identifier}')
    global REMOTE
    if REMOTE is None:
        REMOTE = RemoteSourceWithHead('arxiv.org')
    return REMOTE.head(D.URI(f'https://arxiv.org/pdf/{identifier}.pdf'))


class RemoteSourceWithHead(RemoteSource):
    def head(self, key: D.URI) -> D.CanonicalFile:
        response = self._session.head(key, allow_redirects=True)
        mtime = datetime.strptime(response.headers['Last-Modified'],
                                  '%a, %d %b %Y %H:%M:%S %Z').astimezone(ET)
        return D.CanonicalFile(
            created=mtime,
            modified=mtime,
            size_bytes=int(response.headers['Content-Length']),
            content_type=D.ContentType.pdf,
            ref=D.URI(response.url),    # There may have been redirects.
            filename=response.url.split('/')[-1]
        )