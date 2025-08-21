import os


def _parse_bool(value: str, default: bool) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


# Configurações do PostgreSQL real
PG_HOST: str = os.getenv("PG_HOST")
PG_PORT: int = int(os.getenv("PG_PORT"))

# Configurações do Proxy local
PROXY_HOST: str = os.getenv("PROXY_HOST")
PROXY_PORT: int = int(os.getenv("PROXY_PORT"))

# Código de requisição para negar SSL (cliente -> 'N')
SSL_REQUEST_CODE: int = int(os.getenv("SSL_REQUEST_CODE"))

# Ativar logs de consultas para depuração
DEBUG_LOG_QUERIES: bool = _parse_bool(os.getenv("DEBUG_LOG_QUERIES"))


