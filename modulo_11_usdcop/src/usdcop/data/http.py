from __future__ import annotations

import ssl
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Adaptador personalizado para forzar estrictamente TLS 1.2 y evitar caídas en servidores antiguos
class SSLSuiteAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        # Creamos un contexto SSL seguro estándar
        context = ssl.create_default_context()
        
        # 💡 TRUCO CLAVE: Forzamos a que el protocolo sea ÚNICAMENTE TLS 1.2
        # Al igualar el mínimo y el máximo, evitamos que ofrezca TLS 1.3 (que rompe al DANE)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.maximum_version = ssl.TLSVersion.TLSv1_2
        
        kwargs['ssl_context'] = context
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
    
    # Usamos nuestro adaptador SSLSuiteAdapter corregido
    adapter = SSLSuiteAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    
    session = Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": user_agent})
    return session