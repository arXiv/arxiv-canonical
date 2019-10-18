import logging
import os
from datetime import datetime
from functools import partial
from typing import Callable, Iterable, Optional

from pytz import timezone

from .. import domain as D
from ..services import RemoteSource

logger = logging.getLogger(__name__)
logger.setLevel(int(os.environ.get('LOGLEVEL', '40')))

ET = timezone('US/Eastern')

REMOTE: Optional['RemoteSourceWithHead'] = None


def get_source_path(data_path: str, ident: D.VersionedIdentifier) -> str:
    return _get_path(_orig_source, _latest_source, None, data_path, ident)


def get_path(data_path: str, ident: D.VersionedIdentifier,
             content_type: D.ContentType) -> str:
    return _get_path(partial(_orig_content_type, content_type),
                     partial(_latest_content_type, content_type),
                     partial(_cache_content_type, content_type),
                     data_path, ident)


def get_resource(data_path: str, ident: D.VersionedIdentifier,
                 content_type: D.ContentType) -> D.CanonicalFile:
    logger.debug(f'Getting {content_type.value} for {ident}')
    path = get_path(data_path, ident, content_type)
    mtime = datetime.utcfromtimestamp(os.path.getmtime(path)).astimezone(ET)
    return D.CanonicalFile(
        created=mtime,
        modified=mtime,
        size_bytes=os.path.getsize(path),
        content_type=content_type,
        ref=D.URI(path),
        filename=os.path.split(path)[1]
    )


def get_source(data: str, ident: D.VersionedIdentifier) -> D.CanonicalFile:
    logger.debug(f'Getting source for {ident}')
    path = get_source_path(data, ident)
    mtime = datetime.utcfromtimestamp(os.path.getmtime(path)).astimezone(ET)
    return D.CanonicalFile(
        created=mtime,
        modified=mtime,
        size_bytes=os.path.getsize(path),
        content_type=D.ContentType.targz,
        ref=D.URI(path),
        filename=os.path.split(path)[1]
    )


def get_formats(data: str, ident: D.VersionedIdentifier,
                source_type: D.SourceType) -> Iterable[D.CanonicalFile]:
    """Get the dissemination formats for a version."""
    for content_type in source_type.available_formats:
        filename = f'{ident}.{content_type.ext}'
        cached = _cache(data, ident, filename)
        if os.path.exists(cached):
            mtime = datetime.utcfromtimestamp(os.path.getmtime(cached)) \
                .astimezone(ET)
            yield D.CanonicalFile(
                created=mtime,
                modified=mtime,
                size_bytes=os.path.getsize(cached),
                content_type=content_type,
                ref=D.URI(cached),
                filename=filename
            )
        else:
            yield _get_via_http(ident, content_type)


def _get_via_http(ident: D.VersionedIdentifier,
                  content_type: D.ContentType) -> D.CanonicalFile:
    """Retrieve the"""
    logger.debug(f'Getting render for {ident}')
    global REMOTE       # This is fine for now since this is single-threaded.
    if REMOTE is None:
        REMOTE = RemoteSourceWithHead('arxiv.org')
    path = f'{content_type.value}/{ident}.{content_type.ext}'
    return REMOTE.head(D.URI(f'https://arxiv.org/{path}'))


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


def _latest(data: str, ident: D.Identifier, filename: str) -> str:
    cat = ident.category_part if ident.is_old_style else 'arxiv'
    return os.path.join(data, 'ftp', cat, 'papers', ident.yymm, filename)


def _orig(data: str, ident: D.VersionedIdentifier, filename: str) -> str:
    cat = ident.category_part if ident.is_old_style else 'arxiv'
    return os.path.join(data, 'orig', cat, 'papers', ident.yymm, filename)


def _cache(data: str, ident: D.VersionedIdentifier, filename: str) -> str:
    cat = ident.category_part if ident.is_old_style else 'arxiv'
    return os.path.join(data, 'cache', cat, 'papers', ident.yymm, filename)


def _latest_content_type(content_type: D.ContentType, data: str,
                         ident: D.Identifier) -> str:
    return _latest(data, ident, f'{ident}.{content_type.ext}')


def _orig_content_type(content_type: D.ContentType, data: str,
                       ident: D.VersionedIdentifier) -> str:
    return _orig(data, ident, f'{ident}.{content_type.ext}')


def _cache_content_type(content_type: D.ContentType, data: str,
                       ident: D.VersionedIdentifier) -> str:
    return _cache(data, ident, f'{ident}.{content_type.ext}')


def _latest_source(data_path: str, ident: D.Identifier) -> str:
    return _latest(data_path, ident, f'{ident}.tar.gz')


def _orig_source(data_path: str, ident: D.VersionedIdentifier) -> str:
    return _orig(data_path, ident, f'{ident}.tar.gz')


def _get_path(get_orig: Callable[[str, D.VersionedIdentifier], str],
              get_latest: Callable[[str, D.Identifier], str],
              get_cache: Optional[Callable[[str, D.VersionedIdentifier], str]],
              data_path: str,
              ident: D.VersionedIdentifier) -> str:
    """
    Generic logic for finding the path to a resource.

    Resources for the latest version are stored separately from resources
    for prior versions. But resources for the latest version are not named
    with their version number affix. So it takes a bit of a dance to figure
    out whether a respondant resource exists, and where it is located.
    """
    orig = get_orig(data_path, ident)   # For versions prior to the latest,
    if os.path.exists(orig):            # resources are named with their
        return orig                     # version affix.

    if get_cache:                       # Some things may reside in the cache.
        cache = get_cache(data_path, ident)
        if os.path.exists(cache):
            return cache

    # If this is the first version, the only other place it could be is
    # in the "latest" section.
    latest = get_latest(data_path, ident.arxiv_id)
    if ident.version == 1 and os.path.exists(latest):
        return latest

    # If the prior version exists in the "original" section, then the latest
    # version must be the one that we are working with.
    prior = D.VersionedIdentifier.from_parts(ident.arxiv_id, ident.version - 1)
    if os.path.exists(get_orig(data_path, prior)):
        return latest
    raise IOError(f'No path found for {ident}')