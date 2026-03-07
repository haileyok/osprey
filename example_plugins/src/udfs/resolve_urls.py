from typing import List, Set
from urllib.parse import urlparse

import requests
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.osprey_shared.logging import get_logger
from udfs.cache import DAY, cache_client
from udfs.list import list_cache

logger = get_logger('resolve_urls')

_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; SkyWatch/1.0; +https://github.com/skywatch-bsky/skywatch-automod)',
}

_TIMEOUT = 3
_CACHE_TTL = 3 * DAY
_CACHE_PREFIX = 'resolved-url:'


class ResolveShortenedUrlsArguments(ArgumentsBase):
    urls: List[str]
    list: str
    """Name of the YAML list file containing shortener domains (e.g. 'url_shorteners')"""


class ResolveShortenedUrls(UDFBase[ResolveShortenedUrlsArguments, List[str]]):
    execute_async = True

    def execute(
        self,
        execution_context: ExecutionContext,
        arguments: ResolveShortenedUrlsArguments,
    ) -> List[str]:
        shortener_domains = list_cache.get_list(arguments.list, case_sensitive=False)
        resolved = []
        for url in arguments.urls:
            resolved.append(_resolve_url(url, shortener_domains))
        return resolved


def _ensure_scheme(url: str) -> str:
    if not url.startswith('http://') and not url.startswith('https://'):
        return 'https://' + url
    return url


def _resolve_url(url: str, shortener_domains: Set[str]) -> str:
    url = _ensure_scheme(url)
    if not _is_shortener(url, shortener_domains):
        return url

    cache_key = f'{_CACHE_PREFIX}{url.lower()}'
    cached = cache_client.get_str(cache_key)
    if cached:
        return cached

    resolved = _resolve_url_http(url)

    if resolved != url:
        cache_client.set(cache_key, resolved, _CACHE_TTL)

    return resolved


def _resolve_url_http(url: str) -> str:
    # Try HEAD first (faster, less bandwidth)
    try:
        resp = requests.head(url, allow_redirects=True, timeout=_TIMEOUT, headers=_HEADERS)
        return resp.url
    except Exception:
        pass

    # Some services block HEAD requests, try GET as fallback
    try:
        logger.debug(f'HEAD request failed for {url}, trying GET')
        resp = requests.get(url, allow_redirects=True, timeout=_TIMEOUT, headers=_HEADERS, stream=True)
        resp.close()
        return resp.url
    except requests.exceptions.Timeout:
        logger.warning(f'Timeout resolving URL: {url}')
    except Exception as e:
        logger.warning(f'Failed to resolve URL {url}: {e}')

    return url


def _is_shortener(url: str, shortener_domains: Set[str]) -> bool:
    try:
        hostname = urlparse(url).hostname
        if not hostname:
            return False
        hostname = hostname.lower()
        if hostname.startswith('www.'):
            hostname = hostname[4:]
        return hostname in shortener_domains
    except Exception:
        return False
