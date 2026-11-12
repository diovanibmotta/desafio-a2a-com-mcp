"""Agente Central de Salas: host MCP por dentro, servidor A2A v1.0 (JSON-RPC) por fora.

O agente traduz protocolo, nao dominio: conflito, politica e alternativas sao decisao do
servidor MCP. Sem LLM: o pedido tem formato fixo e a decisao e por regra.

A PONTE (procure por "PONTE" neste arquivo):
  1. PONTE-IDA:   input_required do MCP  -> Task em TASK_STATE_INPUT_REQUIRED
  2. PONTE-VOLTA: SendMessage escolha=.. -> tools/call repetido com id novo + requestState
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import sys
from dataclasses import dataclass, field
from typing import Any

import httpx
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

PORTA = int(os.environ.get("AGENT_PORT", "7300"))
HOST = os.environ.get("AGENT_HOST", "127.0.0.1")
URL_PUBLICA = os.environ.get("AGENT_PUBLIC_URL", f"http://127.0.0.1:{PORTA}").rstrip("/")
MCP_URL = os.environ.get("MCP_URL", "http://127.0.0.1:7301/mcp")
PROTOCOLO_MCP = "2026-07-28"
CAPABILITIES = {"elicitation": {"form": {}}}

SUBMITTED = "TASK_STATE_SUBMITTED"
WORKING = "TASK_STATE_WORKING"
INPUT_REQUIRED = "TASK_STATE_INPUT_REQUIRED"
COMPLETED = "TASK_STATE_COMPLETED"
CANCELED = "TASK_STATE_CANCELED"
FAILED = "TASK_STATE_FAILED"
TERMINAIS = {COMPLETED, CANCELED, FAILED}

# Codigos de erro JSON-RPC / A2A
PARSE_ERROR, INVALID_REQUEST, METHOD_NOT_FOUND, INVALID_PARAMS = -32700, -32600, -32601, -32602
TASK_NOT_FOUND, UNSUPPORTED_OPERATION = -32001, -32004

PEDIDO_RE = re.compile(
    r"^\s*reservar\s+sala=(?P<sala>\S+)\s+inicio=(?P<inicio>\S+)\s+fim=(?P<fim>\S+)\s+responsavel=(?P<responsavel>.+?)\s*$"
)
ESCOLHA_RE = re.compile(r"^\s*escolha=(?P<valor>\S+)\s*$")


def log(msg: str) -> None:
    print(f"[agente] {msg}", file=sys.stderr, flush=True)


def novo_id(prefixo: str) -> str:
    return f"{prefixo}-{secrets.token_hex(6)}"


# --------------------------------------------------------------------------- MCP (host)
class ErroMCP(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code, self.message = code, message


def traceparent_filho(traceparent: str | None) -> str | None:
    """Mesmo trace-id, span-id novo."""
    if not traceparent:
        return None
    partes = traceparent.split("-")
    if len(partes) != 4 or len(partes[1]) != 32:
        return None
    return f"{partes[0]}-{partes[1]}-{secrets.token_hex(8)}-{partes[3]}"


class ClienteMCP:
    """Cliente MCP cru sobre HTTP. Nao usa callback de elicitation: o input_required chega
    inteiro ate o chamador. O objeto vive entre chamadas, mas nenhum estado de protocolo
    (versao, capabilities) e inferido de request anterior: cada request leva tudo no _meta."""

    def __init__(self, url: str):
        self.url = url
        self.http = httpx.AsyncClient(timeout=30)
        self._contador = 0

    async def chamar(self, metodo: str, params: dict[str, Any], traceparent: str | None = None,
                     nome: str | None = None) -> dict[str, Any]:
        self._contador += 1
        meta: dict[str, Any] = {
            "io.modelcontextprotocol/protocolVersion": PROTOCOLO_MCP,
            "io.modelcontextprotocol/clientInfo": {"name": "agente-central-de-salas", "version": "1.0.0"},
            "io.modelcontextprotocol/clientCapabilities": CAPABILITIES,
        }
        tp = traceparent_filho(traceparent)
        if tp:
            meta["traceparent"] = tp
        cabecalhos = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": PROTOCOLO_MCP,
            "Mcp-Method": metodo,
        }
        if nome:
            cabecalhos["Mcp-Name"] = nome
        # id unico por request (inclusive para o retry do MRTR)
        corpo = {"jsonrpc": "2.0", "id": f"ag-{self._contador}-{secrets.token_hex(4)}", "method": metodo,
                 "params": {**params, "_meta": meta}}
        log(f"-> mcp {metodo} id={corpo['id']} traceparent={tp}")
        r = await self.http.post(self.url, json=corpo, headers=cabecalhos)
        ctype = r.headers.get("content-type", "")
        if "text/event-stream" in ctype:
            dados = [ln[5:].strip() for ln in r.text.splitlines() if ln.startswith("data:")]
            resposta = json.loads(dados[-1])
        else:
            resposta = r.json()
        if "error" in resposta:
            e = resposta["error"]
            raise ErroMCP(e.get("code", -32603), e.get("message", "erro MCP"))
        return resposta["result"]

    async def descobrir_tools(self, tp: str | None) -> set[str]:
        resultado = await self.chamar("tools/list", {}, tp)
        return {t["name"] for t in resultado.get("tools", [])}

    async def versao_da_politica(self, tp: str | None) -> str:
        r = await self.chamar("resources/read", {"uri": "politica://uso"}, tp, nome="politica://uso")
        texto = r["contents"][0]["text"]
        m = re.match(r"\s*versao:\s*(\S+)", texto.splitlines()[0])
        if not m:
            raise ErroMCP(-32603, "politica sem versao na primeira linha")
        return m.group(1)


# --------------------------------------------------------------------------- Tasks
@dataclass
class Pausa:
    """Estado interrompido de uma Task. O requestState e OPACO: so guardado e ecoado.
    Fica fora da Task serializada, entao nunca sai em card, artifact ou mensagem."""

    chave: str
    request_state: str
    alternativas: list[str]
    params: dict[str, Any]  # name + arguments do tools/call original


@dataclass
class Task:
    id: str
    context_id: str
    state: str = SUBMITTED
    mensagem_status: dict[str, Any] | None = None
    history: list[dict[str, Any]] = field(default_factory=list)
    artifacts: list[dict[str, Any]] = field(default_factory=list)
    pausa: Pausa | None = None
    traceparent: str | None = None
    politica: str | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def serializar(self) -> dict[str, Any]:
        status: dict[str, Any] = {"state": self.state}
        if self.mensagem_status:
            status["message"] = self.mensagem_status
        return {"id": self.id, "contextId": self.context_id, "status": status,
                "history": list(self.history), "artifacts": list(self.artifacts)}

    def responder(self, texto: str, estado: str) -> None:
        msg = {"messageId": novo_id("msg"), "role": "ROLE_AGENT", "parts": [{"text": texto}],
               "taskId": self.id, "contextId": self.context_id}
        self.history.append(msg)
        self.mensagem_status = msg
        self.state = estado


TASKS: dict[str, Task] = {}
MCP = ClienteMCP(MCP_URL)


def texto_do_resultado(resultado: dict[str, Any]) -> str:
    return " ".join(p.get("text", "") for p in resultado.get("content", []) if p.get("type", "text") == "text")


async def interpretar(task: Task, resultado: dict[str, Any], params: dict[str, Any]) -> None:
    """Traduz o resultado de tools/call para o estado da Task."""
    if resultado.get("resultType") == "input_required":
        # ------------------------------------------------------------------ PONTE-IDA
        # input_required do MCP vira TASK_STATE_INPUT_REQUIRED. O requestState e guardado
        # em task.pausa (ligado a ESTA Task) e nunca volta ao cliente A2A.
        pedidos = resultado.get("inputRequests") or {}
        chave = next(iter(pedidos))
        campo = pedidos[chave]["params"]["requestedSchema"]["properties"]["sala"]
        alternativas = list(campo.get("enum") or ([campo["const"]] if "const" in campo else []))
        task.pausa = Pausa(chave, resultado["requestState"], alternativas, params)
        task.responder("alternativas: " + ", ".join(alternativas), INPUT_REQUIRED)
        return
    task.pausa = None
    if resultado.get("isError"):
        task.responder(texto_do_resultado(resultado), FAILED)
        return
    dados = resultado.get("structuredContent") or {}
    if dados.get("reservado") is False:
        task.responder("Reserva recusada pelo usuario.", CANCELED)
        return
    reserva = {k: dados.get(k) for k in ("reserva", "sala", "inicio", "fim", "responsavel")}
    reserva["politica"] = task.politica
    task.artifacts.append({"artifactId": novo_id("art"), "name": "reserva",
                           "parts": [{"text": json.dumps(reserva)}]})
    task.responder(f"Reserva {reserva['reserva']} confirmada na {reserva['sala']}.", COMPLETED)


async def executar_pedido(task: Task, pedido: dict[str, str]) -> None:
    task.state = WORKING
    try:
        tools = await MCP.descobrir_tools(task.traceparent)  # descoberta em runtime
        if "reservar_sala" not in tools:
            raise ErroMCP(-32601, "servidor MCP nao expoe reservar_sala")
        task.politica = await MCP.versao_da_politica(task.traceparent)
        params = {"name": "reservar_sala", "arguments": pedido}
        resultado = await MCP.chamar("tools/call", params, task.traceparent, nome="reservar_sala")
        await interpretar(task, resultado, params)
    except (ErroMCP, httpx.HTTPError) as e:
        task.pausa = None
        task.responder(str(getattr(e, "message", e)), FAILED)


async def continuar(task: Task, texto: str, traceparent: str | None) -> None:
    pausa = task.pausa
    assert pausa is not None
    m = ESCOLHA_RE.match(texto)
    valor = m.group("valor") if m else None
    if valor is None or (valor != "recusar" and valor not in pausa.alternativas):
        task.responder("alternativas: " + ", ".join(pausa.alternativas), INPUT_REQUIRED)
        return
    if traceparent:
        task.traceparent = traceparent
    task.state = WORKING
    resposta = {"action": "decline"} if valor == "recusar" else {"action": "accept", "content": {"sala": valor}}
    # ---------------------------------------------------------------------- PONTE-VOLTA
    # Mesmo tools/call, id JSON-RPC novo (ClienteMCP.chamar gera um por request),
    # mesma chave de inputRequests e requestState ecoado sem modificacao.
    params = {**pausa.params, "inputResponses": {pausa.chave: resposta}, "requestState": pausa.request_state}
    try:
        resultado = await MCP.chamar("tools/call", params, task.traceparent, nome="reservar_sala")
        await interpretar(task, resultado, pausa.params)
    except (ErroMCP, httpx.HTTPError) as e:
        task.pausa = None
        task.responder(str(getattr(e, "message", e)), FAILED)


# --------------------------------------------------------------------------- A2A
AGENT_CARD = {
    "name": "Central de Salas",
    "description": "Reserva salas de reuniao da Hill Valley Tech.",
    "provider": {"organization": "Hill Valley Tech", "url": "https://hillvalley.example"},
    "version": "1.0.0",
    "supportedInterfaces": [{"url": f"{URL_PUBLICA}/a2a", "protocolBinding": "JSONRPC", "protocolVersion": "1.0"}],
    "capabilities": {"streaming": False, "pushNotifications": False, "extendedAgentCard": False},
    "defaultInputModes": ["text/plain"],
    "defaultOutputModes": ["text/plain"],
    "skills": [{
        "id": "reservar-sala",
        "name": "Reservar sala",
        "description": "Reserva uma sala em um intervalo. Se houver conflito, pergunta qual alternativa usar.",
        "tags": ["salas", "agenda"],
        "inputModes": ["text/plain"],
        "outputModes": ["text/plain"],
        "examples": ["reservar sala=sala-garagem inicio=2026-11-03T14:00:00-03:00 "
                     "fim=2026-11-03T15:00:00-03:00 responsavel=Marty"],
    }],
}


class ErroRPC(Exception):
    def __init__(self, code: int, message: str):
        self.code, self.message = code, message


async def send_message(params: dict[str, Any], traceparent: str | None) -> dict[str, Any]:
    msg = params.get("message")
    if not isinstance(msg, dict):
        raise ErroRPC(INVALID_PARAMS, "message ausente")
    texto = " ".join(p.get("text", "") for p in msg.get("parts", []) if isinstance(p, dict))
    task_id = msg.get("taskId")

    if task_id:
        task = TASKS.get(task_id)
        if task is None:
            raise ErroRPC(TASK_NOT_FOUND, f"Task nao encontrada: {task_id}")
        async with task.lock:
            if task.state in TERMINAIS:
                raise ErroRPC(UNSUPPORTED_OPERATION, f"Task {task_id} ja esta em estado terminal ({task.state})")
            if task.state != INPUT_REQUIRED or task.pausa is None:
                raise ErroRPC(UNSUPPORTED_OPERATION, f"Task {task_id} nao aguarda entrada ({task.state})")
            task.history.append({**msg, "contextId": task.context_id})
            await continuar(task, texto, traceparent)
            return {"task": task.serializar()}

    task = Task(id=novo_id("task"), context_id=msg.get("contextId") or novo_id("ctx"), traceparent=traceparent)
    TASKS[task.id] = task
    async with task.lock:
        task.history.append({**msg, "taskId": task.id, "contextId": task.context_id})
        m = PEDIDO_RE.match(texto)
        if not m:
            task.responder("Pedido invalido. Use: reservar sala=<id> inicio=<iso8601> fim=<iso8601> "
                           "responsavel=<nome>", FAILED)
        else:
            await executar_pedido(task, m.groupdict())
        return {"task": task.serializar()}


async def get_task(params: dict[str, Any]) -> dict[str, Any]:
    task = TASKS.get(str(params.get("id", "")))
    if task is None:
        raise ErroRPC(TASK_NOT_FOUND, f"Task nao encontrada: {params.get('id')}")
    return {"task": task.serializar()}


async def a2a_endpoint(request: Request) -> JSONResponse:
    try:
        corpo = await request.json()
    except ValueError:
        return JSONResponse({"jsonrpc": "2.0", "id": None, "error": {"code": PARSE_ERROR, "message": "JSON invalido"}})
    rid = corpo.get("id") if isinstance(corpo, dict) else None
    try:
        if not isinstance(corpo, dict) or corpo.get("jsonrpc") != "2.0" or "method" not in corpo:
            raise ErroRPC(INVALID_REQUEST, "request JSON-RPC invalido")
        params = corpo.get("params") or {}
        tp = request.headers.get("traceparent")
        log(f"<- a2a {corpo['method']} id={rid} traceparent={tp}")
        if corpo["method"] == "SendMessage":
            resultado = await send_message(params, tp)
        elif corpo["method"] == "GetTask":
            resultado = await get_task(params)
        else:
            raise ErroRPC(METHOD_NOT_FOUND, f"metodo nao suportado: {corpo['method']}")
        return JSONResponse({"jsonrpc": "2.0", "id": rid, "result": resultado})
    except ErroRPC as e:
        return JSONResponse({"jsonrpc": "2.0", "id": rid, "error": {"code": e.code, "message": e.message}})


async def agent_card(_: Request) -> JSONResponse:
    return JSONResponse(AGENT_CARD)


app = Starlette(routes=[
    Route("/.well-known/agent-card.json", agent_card, methods=["GET"]),
    Route("/a2a", a2a_endpoint, methods=["POST"]),
])

if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORTA, log_level="warning")
