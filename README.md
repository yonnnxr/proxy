## Proxy PostgreSQL com Reescrita de SQL

Proxy TCP para PostgreSQL que:
- Normaliza referências a `schema.tabela` numéricas para `"schema"."tabela"`.
- Reescreve comparações `codlig` vs `matricula` para evitar erros de tipo (int vs varchar).
- Nega SSL do cliente (responde `N` para o pedido de SSL).

### Estrutura
- `proxy.py`: entrypoint.
- `config.py`: variáveis de configuração via ambiente.
- `proxy_server.py`: implementação do servidor proxy.
- `sql_rewriter.py`: reescrita de SQL.

### Variáveis de ambiente obrigatórias
- `PG_HOST`: host do PostgreSQL de destino.
- `PG_PORT`: porta do PostgreSQL de destino.
- `PROXY_HOST`: host onde o proxy escutará.
- `PROXY_PORT`: porta onde o proxy escutará.
- `SSL_REQUEST_CODE`: código inteiro para pedido de SSL (use `80877103`).
- `DEBUG_LOG_QUERIES`: `true`/`false` para habilitar logs de consultas.

Exemplo `.env` (PowerShell):
```powershell
$env:PG_HOST = "11.111.11.11"
$env:PG_PORT = "5432"
$env:PROXY_HOST = "11.111.11.11"
$env:PROXY_PORT = "5433"
$env:SSL_REQUEST_CODE = "80877103"
$env:DEBUG_LOG_QUERIES = "true"
```

### Executando
1. Defina as variáveis de ambiente conforme acima.
2. Execute:
```powershell
python .\proxy.py
```

Conecte seu cliente ao `PROXY_HOST:PROXY_PORT`. O proxy encaminhará para `PG_HOST:PG_PORT`.

### Observações
- A reescrita `codlig::text = "matricula"` também cobre a ordem inversa.
- Se o conteúdo de `matricula` for sempre numérico e houver índice, considere alinhar tipos no banco para melhor desempenho.


