"""Version-scoped REST client with finite request timeouts and no transport retries.

PyIceberg's REST client has no portable documented timeout property. Wrap
only this catalog's sessions, never requests globally. Keep PyIceberg auth and
signing adapters intact. This guarded extension point is integration-tested when
PyIceberg is installed; --help does not import this module.
"""

from __future__ import annotations

from functools import wraps
from typing import Any

from .errors import CompatibilityError


def bound_session(session: Any, timeout: float) -> Any:
    """Preserve authentication while preventing timeout=None and automatic retries."""
    from urllib3.util.retry import Retry

    request = session.request

    @wraps(request)
    def bounded_request(method, url, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = timeout
        # REST endpoints are configured explicitly. Refuse redirecting credentials
        # or replaying a state-changing POST against a different endpoint.
        kwargs["allow_redirects"] = False
        return request(method, url, **kwargs)

    session.request = bounded_request
    for adapter in session.adapters.values():
        if hasattr(adapter, "max_retries"):
            adapter.max_retries = Retry(total=0, connect=0, read=0, status=0, redirect=0)
    return session


def create_catalog(name: str, timeout: float, properties: dict[str, Any], *, configured: Any = False):
    """`configured` is a configured catalog to copy, or True for a session-only client."""
    from pyiceberg.catalog.rest import DEFAULT_ENDPOINTS, DEFAULT_NAMESPACE_SEPARATOR, RestCatalog

    if not callable(getattr(RestCatalog, "_create_session", None)):
        raise CompatibilityError("PyIceberg REST session API changed; cannot safely apply request timeouts")

    class BoundedRestCatalog(RestCatalog):
        def _fetch_config(self):
            if not configured:
                super()._fetch_config()
                return
            # Negotiated state lives outside properties; reuse it without another /v1/config request.
            self._supported_endpoints = set(getattr(configured, "_supported_endpoints", DEFAULT_ENDPOINTS))
            self._namespace_separator = getattr(
                configured, "_namespace_separator", DEFAULT_NAMESPACE_SEPARATOR
            )

        def _create_session(self):
            return bound_session(super()._create_session(), timeout)

    return BoundedRestCatalog(name, **properties)
