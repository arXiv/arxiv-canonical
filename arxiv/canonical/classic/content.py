"""
Functions for resolving classic content.

TODO: really need to cache stuff here.
"""
import logging
import os
import time
from datetime import datetime
from functools import partial
from typing import Callable, Iterable, List, MutableMapping, Optional, Tuple

from pytz import timezone

from .. import domain as D
from ..services import RemoteSource

logger = logging.getLogger(__name__)
logger.setLevel(int(os.environ.get('LOGLEVEL', '40')))

ET = timezone('US/Eastern')

REMOTE: Optional['RemoteSourceWithHead'] = None


_CF_Cache = MutableMapping[Tuple[D.VersionedIdentifier, D.ContentType],
                           Optional[D.CanonicalFile]]


def get_source_path(dpath: str, ident: D.VersionedIdentifier) -> str:
    for ext in D.list_source_extensions():
        path = _get_path(_orig_source, _latest_source, dpath, ident, ext)
        if path is not None:
            return path
    raise IOError(f'No source path found for {ident}')


def get_source(data: str, ident: D.VersionedIdentifier) -> D.CanonicalFile:
    """Get the source file for a version from classic."""
    logger.debug(f'Getting source for {ident}')
    path = get_source_path(data, ident)
    mtime = datetime.utcfromtimestamp(os.path.getmtime(path)).astimezone(ET)
    try:
        content_type = D.ContentType.from_filename(path)
    except ValueError:
        # In classic, stand-alone tex files were not given extensions.
        content_type = D.ContentType.tex

    is_gzipped = bool(path.endswith('.gz'))
    cf = D.CanonicalFile(
        modified=mtime,
        size_bytes=os.path.getsize(path),
        content_type=content_type,
        ref=D.URI(path),
        filename=content_type.make_filename(ident),
        is_gzipped=is_gzipped
    )
    logger.debug('Got source file for %s: %s', ident, cf.ref)
    return cf


def get_formats(dpath: str,
                ps_cache_path: str,
                ident: D.VersionedIdentifier,
                source_type: Optional[D.SourceType],
                source: D.CanonicalFile,
                cf_cache: Optional[_CF_Cache] = None) \
        -> Iterable[D.CanonicalFile]:
    """Get the dissemination formats for a version."""
    available_formats: Optional[List[D.ContentType]] = None
    if source.filename is not None:
        available_formats = D.available_formats_by_ext(source.filename)
    if available_formats is None and source_type is not None:
        available_formats = source_type.available_formats

    if not available_formats:   # Nothing more can be done at this point.
        logger.debug('No available dissemination formats for: %s', ident)
        return

    for content_type in available_formats:
        cf: Optional[D.CanonicalFile] = None   # What we hope to yield.

        cache_key = (ident, content_type)
        if cf_cache is not None and cache_key in cf_cache:
            yield cf_cache[cache_key]
            continue

        path: Optional[str] = None  # We hope to find the file on disk.

        # We want to try both gziped and non-gzipped variants of the filename.
        ext = content_type.ext
        ext_gz = f'{ext}.gz' if not ext.endswith('.gz') else ext
        ext = ext if not ext.endswith('.gz') else f'{ext}.gz'

        # In some cases, the resource may have just been the original
        # source (e.g. pdf-only submissions).
        for _ext in (ext, ext_gz):
            path = _get_path(_orig_source, _latest_source, dpath, ident, _ext)
            if path and os.path.exists(path):
                logger.debug('Got source path for %s %s: %s',
                             ident, content_type.value, path)
                break
            logger.debug('Tried source path for %s %s with ext %s',
                            ident, content_type.value, _ext)

        # Otherwise look in the ps_cache.
        if path is None or not os.path.exists(path):
            for _ext in (ext, ext_gz):
                path = _cache(content_type, ps_cache_path, ident, _ext)
                if path and os.path.exists(path):
                    logger.debug('Got ps_cache path for %s %s: %s',
                                ident, content_type.value, path)
                    break
                logger.debug('Tried ps_cache for %s %s with ext %s',
                                ident, content_type.value, _ext)

        if path is not None and os.path.exists(path):
            mtime = datetime.utcfromtimestamp(os.path.getmtime(path)) \
                .astimezone(ET)
            # We want the canonical filename to correspond to the content type
            # more precisely (this is not the case in classic).
            filename = content_type.make_filename(ident)
            cf = D.CanonicalFile(
                modified=mtime,
                size_bytes=os.path.getsize(path),
                content_type=content_type,
                ref=D.URI(path),
                filename=filename,
                is_gzipped=bool(path.endswith('.gz'))
            )
        else:   # Fall back to a HEAD request to the main site.
            cf = _get_via_http(ident, content_type)

        if cf is not None:
            # Sanity check.
            assert cf.filename is not None
            if not cf.filename.endswith(cf.content_type.ext):
                logger.error('Expected ext %s, but filename is %s',
                             cf.content_type.ext, cf.filename)
                raise RuntimeError('Expected ext %s, but filename is %s' %
                                   (cf.content_type.ext, cf.filename))

        if cf_cache is not None:    # A null result is still worth saving.
            cf_cache[cache_key] = cf

        if cf is not None:
            yield cf


def _get_via_http(ident: D.VersionedIdentifier,
                  content_type: D.ContentType,
                  remote: str = 'arxiv.org') -> Optional[D.CanonicalFile]:
    """Retrieve the"""
    logger.debug('Getting metadata for %s for %s via http',
                 content_type.value, ident)
    global REMOTE       # This is fine for now since this is single-threaded.
    if REMOTE is None:
        REMOTE = RemoteSourceWithHead(remote)

    # The .dvi extension is not supported in the classic /dvi route.
    if content_type == D.ContentType.dvi:
        path = f'{content_type.value}/{ident}'
    else:
        path = f'{content_type.value}/{ident}.{content_type.ext}'

    cf = REMOTE.head(D.URI(f'https://arxiv.org/{path}'), content_type)
    if cf is not None:
        cf.filename = content_type.make_filename(ident, cf.is_gzipped)
    return cf


class RemoteSourceWithHead(RemoteSource):
    def head(self, key: D.URI, content_type: D.ContentType) \
            -> Optional[D.CanonicalFile]:
        response = self._session.head(key, allow_redirects=True)
        # arXiv may need to rebuild the product.
        while response.status_code == 200 and 'Refresh' in response.headers:
            time.sleep(int(response.headers['Refresh']))
            response = self._session.head(key, allow_redirects=True)
        if response.status_code != 200:
            logger.error('%i: %s', response.status_code, response.headers)
            raise IOError(f'Could not retrieve {key}: {response.status_code}')

        # At this point, we are most likely encountering the "unavailable"
        # page, which (intriguingly) returns 200 instead of 404.
        if 'Last-Modified' not in response.headers:
            return None

        mtime = datetime.strptime(response.headers['Last-Modified'],
                                  '%a, %d %b %Y %H:%M:%S %Z').astimezone(ET)

        # Oddly, arxiv.org may return compressed content (i.e. not just for
        # transport). We've been around for a while!
        is_gzipped = bool(response.headers.get('Content-Encoding') == 'x-gzip')
        return D.CanonicalFile(
            modified=mtime,
            size_bytes=int(response.headers['Content-Length']),
            content_type=content_type,
            ref=D.URI(response.url),    # There may have been redirects.
            is_gzipped=is_gzipped
        )


def _latest(data: str, ident: D.Identifier, filename: str) -> str:
    cat = ident.category_part if ident.is_old_style else 'arxiv'
    return os.path.join(data, 'ftp', cat, 'papers', ident.yymm, filename)


def _orig(data: str, ident: D.VersionedIdentifier, filename: str) -> str:
    cat = ident.category_part if ident.is_old_style else 'arxiv'
    return os.path.join(data, 'orig', cat, 'papers', ident.yymm, filename)


def _cache(content_type: D.ContentType, ps_cache_path: str,
           ident: D.VersionedIdentifier, ext: str) -> str:
    if ident.is_old_style:
        filename = f'{ident.numeric_part}v{ident.version}.{ext}'
    else:
        filename = f'{ident}.{ext}'
    cat = ident.category_part if ident.is_old_style else 'arxiv'
    return os.path.join(ps_cache_path, 'ps_cache', cat, content_type.value,
                        ident.yymm, filename)


def _latest_source(path: str, ident: D.Identifier, ext: str) -> str:
    if ident.is_old_style:
        fname = f'{ident.numeric_part}.{ext.lstrip(".")}'
    else:
        fname = f'{ident}.{ext.lstrip(".")}'
    return _latest(path, ident, fname)


def _orig_source(path: str, ident: D.VersionedIdentifier, ext: str) -> str:
    if ident.is_old_style:
        fname = f'{ident.numeric_part}v{ident.version}.{ext.lstrip(".")}'
    else:
        fname = f'{ident}.{ext.lstrip(".")}'
    return _orig(path, ident, fname)


def _get_path(get_orig: Callable[[str, D.VersionedIdentifier, str], str],
              get_latest: Callable[[str, D.Identifier, str], str],
              dpath: str,
              ident: D.VersionedIdentifier,
              ext: str) -> Optional[str]:
    """
    Generic logic for finding the path to a resource.

    Resources for the latest version are stored separately from resources
    for prior versions. But resources for the latest version are not named
    with their version number affix.

    A second challenge is that in some cases we do not know ahead of time what
    file format (and hence filename) we are looking for.

    So it takes a bit of a dance to figure out whether a respondant resource
    exists, and where it is located.
    """
    # For versions prior to the latest, resources are named with their
    # version affix.
    orig = get_orig(dpath, ident, ext)
    logger.debug(orig)
    if os.path.exists(orig):
        logger.debug(f'found orig path: {orig}')
        return orig

    # If this is the first version, the only other place it could be is
    # in the "latest" section.

    latest = get_latest(dpath, ident.arxiv_id, ext)
    if ident.version == 1 and os.path.exists(latest):
        logger.debug(f'can only be in latest: {latest}')
        return latest

    # If the prior version exists in the "original" section, then the latest
    # version must be the one that we are working with.
    prior = D.VersionedIdentifier.from_parts(ident.arxiv_id, ident.version - 1)
    # Have to check for the abs file, since we don't know what format the
    # previous version was in.
    if os.path.exists(get_orig(dpath, prior, 'abs')):
        if os.path.exists(latest):
            logger.debug(f'prior version in orig; must be latest: {latest}')
            return latest

    return None