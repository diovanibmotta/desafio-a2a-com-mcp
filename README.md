# A Ponte: agente A2A com MCP por dentro

Hill Valley Tech: cinco salas, um servidor MCP que as expoe e um agente que as oferece ao mundo via A2A.

```
cliente A2A --JSON-RPC /a2a--> [agente :7300] --MCP Streamable HTTP /mcp--> [servidor MCP :7301]
                               (servidor A2A + host MCP)                    (tools, resource, MRTR)
```

- `servidor-mcp/server.py`: SDK oficial `mcp` 2.3.0 (Python), MCP revisao 2026-07-28, Streamable HTTP stateless.
- `agente/agent.py`: servidor A2A v1.0 (JSON-RPC) + cliente MCP cru sobre HTTP (httpx). Sem LLM.
- `dados/`, `validador/`, `exemplos/` ficam intactos.

## Como rodar

Requer Python 3.10+. A partir de um clone limpo, na raiz do repositorio (Linux/macOS/Git Bash):

```bash
# 1. dependencias (versoes travadas em requirements.txt / pyproject.toml)
python3 -m venv servidor-mcp/.venv && servidor-mcp/.venv/bin/pip install -r servidor-mcp/requirements.txt
python3 -m venv agente/.venv && agente/.venv/bin/pip install -r agente/requirements.txt

# 2. segredo de integridade do requestState (>= 32 bytes). Gere o seu; nunca commite o valor.
export REQUEST_STATE_SECRET=$(python3 -c "import secrets; print(secrets.token_hex(32))")

# 3. terminal 1: servidor MCP (o stderr mostra metodo, id e traceparent de cada request)
servidor-mcp/.venv/bin/python servidor-mcp/server.py

# 4. terminal 2: agente A2A
agente/.venv/bin/python agente/agent.py

# 5. terminal 3: validador (rode com os dois processos recem-iniciados)
python3 validador/validar.py --agente http://localhost:7300 --mcp http://localhost:7301
```

No Windows (Git Bash) troque `.venv/bin/` por `.venv/Scripts/`. No PowerShell:
`$env:REQUEST_STATE_SECRET = python -c "import secrets; print(secrets.token_hex(32))"`.

O mesmo `REQUEST_STATE_SECRET` precisa estar no ambiente do servidor MCP em qualquer restart (e o que permite o
`requestState` sobreviver a ele). Sem a variavel, ou com menos de 32 bytes, o servidor recusa subir.

Variaveis opcionais (defaults = portas do desafio): `MCP_PORT` (7301), `MCP_HOST`, `AGENT_PORT` (7300),
`AGENT_HOST`, `AGENT_PUBLIC_URL` (URL base no Agent Card), `MCP_URL` (endpoint MCP usado pelo agente).

## Onde a ponte acontece

Em `agente/agent.py`, funcao `interpretar` (marcador `PONTE-IDA`): quando `tools/call` volta com
`resultType: "input_required"`, o agente le a chave unica de `inputRequests` e o enum de `requestedSchema`, grava
chave + `requestState` + argumentos originais em `Task.pausa` (campo que nunca e serializado), e poe a Task em
`TASK_STATE_INPUT_REQUIRED` com a mensagem `alternativas: a, b`. A volta esta em `continuar` (marcador
`PONTE-VOLTA`): o `SendMessage` com `taskId` e `escolha=<sala>` vira um novo `tools/call` do mesmo `reservar_sala`,
com **id JSON-RPC novo** (`ClienteMCP.chamar` gera um por request), `inputResponses` com a mesma chave e o
`requestState` ecoado sem modificacao; `escolha=recusar` vira `action: "decline"` e a Task termina em
`TASK_STATE_CANCELED`. O agente nunca abre o `requestState` e nao usa callback de elicitation do SDK: o
`input_required` chega cru. No servidor, o ponto equivalente e `escolha_de_sala` + `reservar_sala` em
`servidor-mcp/server.py`: o resolver devolve um `Elicit` (nao ha chamada ao cliente) e o SDK encerra a resposta com
`input_required`; no retry ele reconstroi tudo do `requestState`.

## Decisoes tecnicas

- **Protecao do requestState:** `RequestStateSecurity(keys=[REQUEST_STATE_SECRET])` do SDK: AES-256-GCM (cifra e
  autentica, entao alem de nao ser adulteravel tambem nao e legivel), chave derivada por HKDF-SHA256. Alem da
  integridade, o envelope do SDK amarra o estado a metodo, tool, **digest dos argumentos** (argumentos adulterados no
  retry fazem o estado ser rejeitado com -32602) e audiencia (`central-de-salas`). Adulterado ou expirado: -32602.
- **Validade:** 600 s (10 minutos), dentro da faixa de 5 a 30 minutos. O estado e autocontido: o servidor nao guarda
  nada entre o `input_required` e o retry, e um retry apos restart do processo funciona (testado reiniciando o
  servidor com o mesmo segredo).
- **Segredo:** so via `REQUEST_STATE_SECRET`, validado no boot (>= 32 bytes). Nada hardcoded.
- **Sem sessao:** servidor em `stateless_http=True`; todo request leva `protocolVersion` e `clientCapabilities` no
  `_meta` (sem eles: -32602/HTTP 400). Sem elicitation em form mode declarada: -32021/HTTP 400 com
  `requiredCapabilities`. O agente declara `{"elicitation": {"form": {}}}` e envia `MCP-Protocol-Version`,
  `Mcp-Method` e `Mcp-Name` em todo request.
- **Estado das Tasks:** em memoria no agente (`TASKS`, dict por id), cada Task com seu proprio `Pausa` e seu proprio
  lock; duas Tasks pausadas ao mesmo tempo nao trocam de `requestState`. Estado terminal e definitivo: `SendMessage`
  em Task terminal devolve erro JSON-RPC -32004. Reservas tambem sao em memoria (nao sobrevivem a restart).
- **traceparent:** o agente le o header `traceparent` do cliente A2A, guarda na Task e propaga o mesmo trace-id (span-id
  novo) no `_meta` de todo request MCP daquela Task (`tools/list`, `resources/read`, `tools/call` e o retry). O
  servidor registra `metodo`, `id` e `traceparent` de cada request no stderr.
- **Descoberta:** a cada Task o agente chama `tools/list` (e confere `reservar_sala`) e le `politica://uso` antes do
  primeiro `tools/call`; a versao da politica vem do resource e entra no artifact `reserva`.
- **Erros de execucao:** `isError: true` do MCP vira `TASK_STATE_FAILED` com a mensagem exata da tool no historico.
- **Sem LLM:** pedido em formato fixo (`reservar sala=... inicio=... fim=... responsavel=...`), decisao por regra;
  dependencias: apenas httpx, starlette, uvicorn.
- **Limitacoes do SDK:** nenhuma que tenha exigido contornar o protocolo. O SDK emite a chave de `inputRequests` como
  `<modulo>:<resolver>` (`__main__:escolha_de_sala`), por isso o servidor deve ser iniciado como script
  (`python servidor-mcp/server.py`), como no comando acima, para que a chave seja estavel entre restarts.

## Saida do validador

Execucao com os dois processos recem-iniciados (codigo de saida 0):

```
trace-id desta execucao: fbc4ff51a5db235dbb42e30c29368a27
procure esse valor no stderr do servidor MCP para conferir a propagacao do traceparent.

PASS 01 tools/list traz as tres tools
PASS 02 toda tool tem inputSchema de objeto
PASS 03 listar_salas devolve structuredContent e o mesmo JSON em texto
PASS 04 _meta sem protocolVersion devolve -32602 e HTTP 400
PASS 05 _meta sem clientCapabilities devolve -32602 e HTTP 400
PASS 06 tool inexistente e recusada, por -32602 ou por isError
PASS 07 resources/read de politica://uso devolve a politica
PASS 08 resources/read de URI inexistente devolve -32602
PASS 09 sala inexistente devolve isError com a mensagem exata
PASS 10 fora da janela devolve isError com a mensagem exata
PASS 11 duracao acima de 2h devolve isError com a mensagem exata
PASS 12 intervalo invertido devolve isError com a mensagem exata
PASS 13 conflito devolve input_required com inputRequests e requestState
PASS 14 a elicitation e form mode e oferece as alternativas na ordem certa
PASS 15 conflito sem a capability elicitation devolve -32021 e HTTP 400
PASS 16 retry com inputResponses e requestState conclui a reserva
PASS 17 requestState adulterado e rejeitado com -32602
PASS 18 argumentos adulterados no retry nao tomam efeito
PASS 19 recusa conclui sem reservar e sem isError
PASS 20 conflito sem alternativa possivel devolve isError com a mensagem exata

PASS 21 agent card responde 200 no well-known com JSON
PASS 22 o card declara a interface JSON-RPC com url e versao 1.0
PASS 23 o card declara a skill reservar-sala
PASS 24 SendMessage com sala livre conclui a Task
PASS 25 o artifact chama reserva e traz a versao da politica
PASS 26 GetTask devolve id, contextId e estado corrente
PASS 27 SendMessage com sala ocupada pausa a Task
PASS 28 a Task pausada lista as alternativas na ordem certa
PASS 29 escolha fora do enum mantem a Task pausada
PASS 30 a continuacao conclui a Task na sala escolhida
PASS 31 SendMessage em Task terminal e recusado
PASS 32 a recusa termina a Task em CANCELED
PASS 33 duas Tasks pausadas ao mesmo tempo concluem cada uma com a sua reserva
PASS 34 nenhuma resposta A2A carrega o requestState
PASS 35 sala inexistente termina a Task em FAILED com a mensagem da tool
PASS 36 o agente e deterministico: o mesmo pedido produz a mesma pausa

resumo: 36 passaram, 0 falharam, de 36 verificacoes
```
