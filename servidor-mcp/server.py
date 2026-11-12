"""Servidor MCP da Central de Salas (Streamable HTTP, MCP 2026-07-28).

O servidor NAO guarda estado de conversa: o ciclo MRTR da reserva (input_required ->
retry) viaja inteiro no requestState, selado pelo SDK com AES-256-GCM.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, Any, Literal

from mcp.server.elicitation import AcceptedElicitation, ElicitationResult
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.mcpserver.resolve import Elicit, Resolve
from mcp.server.request_state import RequestStateSecurity
from pydantic import BaseModel, Field, create_model

RAIZ = Path(os.environ.get("DADOS_DIR", Path(__file__).resolve().parent.parent / "dados"))
PORTA = int(os.environ.get("MCP_PORT", "7301"))
HOST = os.environ.get("MCP_HOST", "127.0.0.1")
TTL_REQUEST_STATE = 600.0  # 10 minutos: dentro da faixa de 5 a 30 exigida
SP = timezone(timedelta(hours=-3))


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


# --- segredo de integridade do requestState (nunca no codigo) -----------------------
SEGREDO = os.environ.get("REQUEST_STATE_SECRET", "")
if len(SEGREDO.encode()) < 32:
    sys.exit(
        "REQUEST_STATE_SECRET ausente ou com menos de 32 bytes. Gere com:\n"
        '  python3 -c "import secrets; print(secrets.token_hex(32))"'
    )

# --- dados ------------------------------------------------------------------------
SALAS: list[dict[str, Any]] = json.loads((RAIZ / "salas.json").read_text(encoding="utf-8"))
RESERVAS: list[dict[str, Any]] = json.loads((RAIZ / "reservas.json").read_text(encoding="utf-8"))
POLITICA_TEXTO = (RAIZ / "politica-de-uso.md").read_text(encoding="utf-8")
POLITICA_VERSAO = POLITICA_TEXTO.splitlines()[0].split(":", 1)[1].strip()
_proximo = [len(RESERVAS) + 1]


# --- modelos de saida -------------------------------------------------------------
class SalaOut(BaseModel):
    id: str
    nome: str
    capacidade: int
    recursos: list[str]


class ListaDeSalas(BaseModel):
    salas: list[SalaOut]


class ConflitoOut(BaseModel):
    id: str
    inicio: str
    fim: str
    responsavel: str


class Disponibilidade(BaseModel):
    sala: str
    livre: bool
    conflitos: list[ConflitoOut]


class ReservaOut(BaseModel):
    reserva: str | None = None
    reservado: bool = True
    sala: str | None = None
    inicio: str | None = None
    fim: str | None = None
    responsavel: str | None = None
    politica: str | None = None
    motivo: str | None = None


# --- regras -----------------------------------------------------------------------
def _parse(valor: str) -> datetime:
    try:
        d = datetime.fromisoformat(valor)
    except ValueError:
        raise ToolError(f"Intervalo invalido: data/hora fora do formato ISO 8601: {valor}") from None
    return d if d.tzinfo else d.replace(tzinfo=SP)


def validar(sala: str, inicio: str, fim: str) -> tuple[datetime, datetime]:
    if not any(s["id"] == sala for s in SALAS):
        raise ToolError(f"Sala inexistente: {sala}")
    i, f = _parse(inicio), _parse(fim)
    if f <= i:
        raise ToolError("Intervalo invalido: fim deve ser posterior a inicio")
    li, lf = i.astimezone(SP), f.astimezone(SP)
    abre = li.replace(hour=8, minute=0, second=0, microsecond=0)
    fecha = li.replace(hour=20, minute=0, second=0, microsecond=0)
    if li < abre or lf > fecha:
        raise ToolError("Fora da janela de uso: a politica permite reservas entre 08:00 e 20:00")
    if f - i > timedelta(hours=2):
        raise ToolError("Duracao acima do limite: a politica permite no maximo 2 horas")
    return i, f


def conflitos(sala: str, i: datetime, f: datetime) -> list[dict[str, Any]]:
    return [
        r
        for r in RESERVAS
        if r["sala"] == sala and _parse(r["inicio"]) < f and i < _parse(r["fim"])
    ]


def alternativas(sala: str, i: datetime, f: datetime) -> list[str]:
    pedida = next(s for s in SALAS if s["id"] == sala)
    livres = [
        s
        for s in SALAS
        if s["id"] != sala and s["capacidade"] >= pedida["capacidade"] and not conflitos(s["id"], i, f)
    ]
    livres.sort(key=lambda s: (s["capacidade"], s["id"]))
    return [s["id"] for s in livres[:3]]


def criar_reserva(sala: str, inicio: str, fim: str, responsavel: str) -> ReservaOut:
    rid = f"res-{_proximo[0]:04d}"
    _proximo[0] += 1
    RESERVAS.append({"id": rid, "sala": sala, "inicio": inicio, "fim": fim, "responsavel": responsavel})
    return ReservaOut(
        reserva=rid, reservado=True, sala=sala, inicio=inicio, fim=fim, responsavel=responsavel,
        politica=POLITICA_VERSAO,
    )


# --- resolver da elicitation (MRTR) ----------------------------------------------
async def escolha_de_sala(sala: str, inicio: str, fim: str) -> Elicit[BaseModel] | None:
    """Se o intervalo conflita e ha alternativas, pede a escolha. Senao nao pergunta nada.

    Nao ha chamada ao cliente aqui: o SDK termina a resposta com resultType=input_required
    e o cliente volta com um tools/call novo (inputResponses + requestState).
    """
    try:
        i, f = validar(sala, inicio, fim)
    except ToolError:
        return None  # o corpo da tool devolve o erro de execucao
    if not conflitos(sala, i, f):
        return None
    alts = alternativas(sala, i, f)
    if not alts:
        return None  # o corpo da tool devolve "Sem alternativas disponiveis"
    modelo = create_model(
        "EscolhaDeSala",
        sala=(Literal[tuple(alts)], Field(description="Sala alternativa escolhida", title="Sala")),  # type: ignore[valid-type]
    )
    return Elicit("A sala pedida esta ocupada nesse intervalo. Escolha uma alternativa.", modelo)


# --- servidor ---------------------------------------------------------------------
mcp = MCPServer(
    name="central-de-salas",
    version="1.0.0",
    request_state_security=RequestStateSecurity(keys=[SEGREDO], ttl=TTL_REQUEST_STATE),
)


async def _log_request(ctx: Any, call_next: Any) -> Any:
    params = ctx.params or {}
    meta = params.get("_meta") or {}
    log(f"[mcp] metodo={ctx.method} id={ctx.request_id} traceparent={meta.get('traceparent')}")
    return await call_next(ctx)


mcp.middleware.insert(0, _log_request)


@mcp.tool(description="Lista todas as salas com capacidade e recursos.")
def listar_salas() -> ListaDeSalas:
    return ListaDeSalas(salas=[SalaOut(**s) for s in SALAS])


@mcp.tool(description="Diz se uma sala esta livre no intervalo, e quais reservas conflitam.")
def consultar_disponibilidade(sala: str, inicio: str, fim: str) -> Disponibilidade:
    i, f = validar(sala, inicio, fim)
    cs = conflitos(sala, i, f)
    return Disponibilidade(
        sala=sala, livre=not cs,
        conflitos=[ConflitoOut(id=c["id"], inicio=c["inicio"], fim=c["fim"], responsavel=c["responsavel"]) for c in cs],
    )


@mcp.tool(description="Reserva uma sala. Se o intervalo estiver ocupado, pergunta qual alternativa usar.")
def reservar_sala(
    sala: str,
    inicio: str,
    fim: str,
    responsavel: str,
    escolha: Annotated[ElicitationResult[BaseModel], Resolve(escolha_de_sala)],
) -> ReservaOut:
    if not isinstance(escolha, AcceptedElicitation):  # decline / cancel: nao e erro
        return ReservaOut(reservado=False, motivo="recusado")
    destino = sala
    if escolha.data is not None:
        destino = escolha.data.sala  # type: ignore[attr-defined]
    i, f = validar(destino, inicio, fim)
    if conflitos(destino, i, f):
        if destino == sala and not alternativas(sala, i, f):
            raise ToolError("Sem alternativas disponiveis no intervalo")
        raise ToolError(f"Sala ocupada no intervalo: {destino}")
    return criar_reserva(destino, inicio, fim, responsavel)


@mcp.resource("politica://uso", mime_type="text/markdown", description="Politica de uso das salas.")
def politica_de_uso() -> str:
    return POLITICA_TEXTO


if __name__ == "__main__":
    log(f"[mcp] politica {POLITICA_VERSAO}; ouvindo em http://{HOST}:{PORTA}/mcp")
    mcp.run("streamable-http", host=HOST, port=PORTA, stateless_http=True)
