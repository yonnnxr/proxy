import os
from pathlib import Path


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(
            f"Variável de ambiente obrigatória não definida: {name}. "
            f"Defina {name} antes de executar o proxy."
        )
    return value


def _get_required_int(name: str) -> int:
    raw = _get_required_env(name)
    try:
        return int(raw)
    except ValueError:
        raise RuntimeError(f"Valor inválido para {name}: '{raw}'. Esperado inteiro.")


def _get_required_bool(name: str) -> bool:
    raw = _get_required_env(name).strip().lower()
    if raw in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise RuntimeError(
        f"Valor inválido para {name}: '{raw}'. Use true/false (1/0, on/off, yes/no)."
    )


def _get_optional_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise RuntimeError(f"Valor inválido para {name}: '{raw}'. Esperado inteiro.")


def _load_dotenv_file(path: Path) -> None:
    try:
        if not path.exists() or not path.is_file():
            return
        lines = path.read_text(encoding="utf-8").splitlines()
        for idx, line in enumerate(lines):
            stripped = line.strip()
            if idx == 0 and stripped.startswith('\ufeff'):
                stripped = stripped.lstrip('\ufeff').strip()
            if not stripped or stripped.startswith('#'):
                continue
            if stripped.lower().startswith('export '):
                stripped = stripped[7:].lstrip()
            if '=' not in stripped:
                continue
            key, value = stripped.split('=', 1)
            key = key.strip()
            value = value.strip()
            # Remover comentário inline não citado
            in_squote = False
            in_dquote = False
            new_value_chars = []
            for ch in value:
                if ch == "'" and not in_dquote:
                    in_squote = not in_squote
                elif ch == '"' and not in_squote:
                    in_dquote = not in_dquote
                elif ch == '#' and not in_squote and not in_dquote:
                    break
                new_value_chars.append(ch)
            value = ''.join(new_value_chars).rstrip()
            if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
                value = value[1:-1]
            # Não sobrescrever variáveis já definidas no ambiente
            if key and (key not in os.environ):
                os.environ[key] = value
    except Exception:
        # Falha silenciosa: não bloquear o app por um .env mal formatado
        pass


# Carregar .env automaticamente (cwd e diretório do módulo)
_dotenv_candidates = [
    Path.cwd() / ".env",
    Path.cwd() / ".ENV",
    Path.cwd() / "ENV",
    Path(__file__).resolve().parent / ".env",
    Path(__file__).resolve().parent / ".ENV",
    Path(__file__).resolve().parent / "ENV",
]
for _candidate in _dotenv_candidates:
    _load_dotenv_file(_candidate)


# Configurações do PostgreSQL real
PG_HOST: str = _get_required_env("PG_HOST")
PG_PORT: int = _get_required_int("PG_PORT")

# Configurações do Proxy local
PROXY_HOST: str = _get_required_env("PROXY_HOST")
PROXY_PORT: int = _get_required_int("PROXY_PORT")

# Código de requisição para negar SSL (cliente -> 'N')
SSL_REQUEST_CODE: int = _get_required_int("SSL_REQUEST_CODE")

# Ativar logs de consultas para depuração
DEBUG_LOG_QUERIES: bool = _get_required_bool("DEBUG_LOG_QUERIES")


# Timeouts opcionais (segundos)
# Tempo máximo para conectar no PostgreSQL real
CONNECT_TIMEOUT_SECS: int = _get_optional_int("CONNECT_TIMEOUT_SECS", 10)
# Tempo máximo aguardando dados do cliente durante o startup/handshake
CLIENT_IDLE_TIMEOUT_SECS: int = _get_optional_int("CLIENT_IDLE_TIMEOUT_SECS", 30)

