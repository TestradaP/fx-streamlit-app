from __future__ import annotations

import ssl
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Adaptador que mantiene TLS 1.2 como mínimo y permite negociar versiones más nuevas.
class SSLSuiteAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context()
        context.minimum_version = ssl.TLSVersion.TLSv1_2

        kwargs["ssl_context"] = context
        return super().init_poolmanager(*args, **kwargs)


def build_session(user_agent: str = "Modulo11-USDCOP/0.1") -> Session:
    retry = Retry(
        total=4,
        read=4,
        connect=4,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )

    adapter = SSLSuiteAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)

    session = Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": user_agent})
    return session
