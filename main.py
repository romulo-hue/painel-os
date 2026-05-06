import os
import json
import base64
import logging
import time
import threading
from typing import Any, Generator, Optional
from datetime import datetime

import requests
from fastapi import FastAPI, Depends, Request, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, Boolean, func, text
from sqlalchemy.orm import sessionmaker, declarative_base, Session

# =========================================================
# CONFIG
# =========================================================

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL nao configurada no ambiente")

# Endpoints da API FrotaWeb/integração.
# Configure no Render se forem diferentes:
# FROTAWEB_OS_URL=https://frotaweb-os-corretiva-api.onrender.com/os-corretiva
# FROTAWEB_SERVICO_URL=https://frotaweb-os-corretiva-api.onrender.com/os-corretiva/servicos
FROTAWEB_OS_URL = os.getenv(
    "FROTAWEB_OS_URL",
    "https://frotaweb-os-corretiva-api.onrender.com/os-corretiva",
)

FROTAWEB_SERVICO_URL = os.getenv(
    "FROTAWEB_SERVICO_URL",
    "https://frotaweb-os-corretiva-api.onrender.com/os-corretiva/servicos",
)

# Credenciais padrao, usadas quando o app nao enviar credenciais.
# Recomendo configurar no Render em Environment.
FROTAWEB_EMPRESA = os.getenv("FROTAWEB_EMPRESA", "1")
FROTAWEB_USUARIO = os.getenv("FROTAWEB_USUARIO", "")
FROTAWEB_SENHA = os.getenv("FROTAWEB_SENHA", "")
FROTAWEB_FILIAL = os.getenv("FROTAWEB_FILIAL", "1")
FROTAWEB_RECURSO_HUMANO = os.getenv("FROTAWEB_RECURSO_HUMANO", "")
HTTP_VERIFY_TLS = str(os.getenv("HTTP_VERIFY_TLS", "false")).strip().lower() in ("1", "true", "yes", "on")
HTTP_MAX_RETRIES = max(1, int(os.getenv("HTTP_MAX_RETRIES", "4")))
HTTP_RETRY_BACKOFF_SECONDS = max(1, int(os.getenv("HTTP_RETRY_BACKOFF_SECONDS", "5")))
HTTP_RETRYABLE_STATUS_CODES = {429, 502, 503, 504}
HTTP_WAKEUP_ENABLED = str(os.getenv("HTTP_WAKEUP_ENABLED", "true")).strip().lower() in ("1", "true", "yes", "on")
HTTP_WAKEUP_DELAY_SECONDS = max(1, int(os.getenv("HTTP_WAKEUP_DELAY_SECONDS", "3")))
QUEUE_POLL_SECONDS = max(2, int(os.getenv("QUEUE_POLL_SECONDS", "5")))

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
logger = logging.getLogger("painel_os")

app = FastAPI(
    title="Painel O.S. Corretiva - Tryout FrotaWeb",
    version="4.0.0",
    description="Recebe O.S. e serviços do app, salva no painel e envia manualmente ao FrotaWeb nos formatos de tryout.",
)

UPLOAD_DIR = os.getenv("UPLOAD_DIR", "uploads/fotos")
os.makedirs(UPLOAD_DIR, exist_ok=True)
app.mount("/fotos", StaticFiles(directory=UPLOAD_DIR), name="fotos")

worker_stop_event = threading.Event()
worker_thread: Optional[threading.Thread] = None
worker_lock = threading.Lock()

# =========================================================
# DATABASE
# =========================================================

def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class OrdemServico(Base):
    __tablename__ = "ordens_servico"

    id = Column(Integer, primary_key=True, index=True)

    # Credenciais
    empresa = Column(String(100), nullable=True)
    usuario = Column(String(255), nullable=True)
    senha = Column(String(255), nullable=True)
    filial = Column(String(100), nullable=True)
    recurso_humano = Column(String(100), nullable=True)

    # Tryout O.S. corretiva
    codigo_veiculo = Column(String(100), nullable=True)
    descricao_defeito = Column(Text, nullable=True)
    numero_os = Column(String(100), nullable=True)
    placa = Column(String(50), nullable=True)
    codigo_componente = Column(String(100), nullable=True)
    data_abertura = Column(String(100), nullable=True)
    data_hora_abertura = Column(String(100), nullable=True)
    hodometro = Column(String(100), nullable=True)
    horimetro_entrada = Column(String(100), nullable=True)
    data_hora_saida = Column(String(100), nullable=True)
    hodometro_saida = Column(String(100), nullable=True)
    horimetro_saida = Column(String(100), nullable=True)
    data_hora_inicio = Column(String(100), nullable=True)
    data_hora_previsao_liberacao = Column(String(100), nullable=True)
    horas_previstas = Column(String(100), nullable=True)
    horas_realizadas = Column(String(100), nullable=True)
    codigo_filial = Column(String(100), nullable=True)
    codigo_departamento = Column(String(100), nullable=True)
    codigo_oficina = Column(String(100), nullable=True)
    codigo_servico = Column(String(100), nullable=True)
    codigo_solicitante = Column(String(100), nullable=True)
    codigo_motorista = Column(String(100), nullable=True)
    numero_ocorrencia = Column(String(100), nullable=True)
    numero_contrato = Column(String(100), nullable=True)
    valor_acrescimo = Column(String(100), nullable=True)
    numero_os_retorno = Column(String(100), nullable=True)
    observacoes = Column(Text, nullable=True)

    investimento = Column(Boolean, nullable=False, default=False)
    acidente = Column(Boolean, nullable=False, default=False)
    socorro = Column(Boolean, nullable=False, default=False)
    servico_retorno = Column(Boolean, nullable=False, default=False)
    programada = Column(Boolean, nullable=False, default=False)

    status_envio = Column(String(50), nullable=False, default="PENDENTE")
    retorno_envio = Column(Text, nullable=True)
    payload_original = Column(Text, nullable=True)
    campos_brutos = Column(Text, nullable=True)

    # Fotos e JSON original recebidos do aplicativo
    fotos_app = Column(Text, nullable=True)
    app_payload = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)


class ServicoOS(Base):
    __tablename__ = "ordens_servico_servicos"

    id = Column(Integer, primary_key=True, index=True)

    # Credenciais
    empresa = Column(String(100), nullable=True)
    usuario = Column(String(255), nullable=True)
    senha = Column(String(255), nullable=True)
    filial = Column(String(100), nullable=True)
    recurso_humano = Column(String(100), nullable=True)

    # Tryout serviço
    numero_os = Column(String(100), nullable=True, index=True)
    codigo_veiculo = Column(String(100), nullable=True)
    placa = Column(String(50), nullable=True)
    codigo_servico = Column(String(100), nullable=True)
    codigo_recurso_humano = Column(String(100), nullable=True)
    tempo_gasto = Column(String(100), nullable=True)
    valor_hora = Column(String(100), nullable=True)

    status_envio = Column(String(50), nullable=False, default="PENDENTE")
    retorno_envio = Column(Text, nullable=True)
    payload_original = Column(Text, nullable=True)
    campos_brutos = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)


class TentativaEnvio(Base):
    __tablename__ = "tentativas_envio"

    id = Column(Integer, primary_key=True, index=True)
    tipo_registro = Column(String(20), nullable=False)
    registro_id = Column(Integer, nullable=False, index=True)
    status_execucao = Column(String(50), nullable=False, default="PENDENTE")
    status_http = Column(String(20), nullable=True)
    sucesso = Column(Boolean, nullable=False, default=False)
    mensagem = Column(Text, nullable=True)
    diagnostico = Column(Text, nullable=True)
    payload_enviado = Column(Text, nullable=True)
    retorno_recebido = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


Base.metadata.create_all(bind=engine)


def garantir_colunas():
    with engine.begin() as conn:
        os_cols = {
            "empresa": "VARCHAR(100)",
            "usuario": "VARCHAR(255)",
            "senha": "VARCHAR(255)",
            "filial": "VARCHAR(100)",
            "recurso_humano": "VARCHAR(100)",
            "codigo_veiculo": "VARCHAR(100)",
            "descricao_defeito": "TEXT",
            "numero_os": "VARCHAR(100)",
            "placa": "VARCHAR(50)",
            "codigo_componente": "VARCHAR(100)",
            "data_abertura": "VARCHAR(100)",
            "data_hora_abertura": "VARCHAR(100)",
            "hodometro": "VARCHAR(100)",
            "horimetro_entrada": "VARCHAR(100)",
            "data_hora_saida": "VARCHAR(100)",
            "hodometro_saida": "VARCHAR(100)",
            "horimetro_saida": "VARCHAR(100)",
            "data_hora_inicio": "VARCHAR(100)",
            "data_hora_previsao_liberacao": "VARCHAR(100)",
            "horas_previstas": "VARCHAR(100)",
            "horas_realizadas": "VARCHAR(100)",
            "codigo_filial": "VARCHAR(100)",
            "codigo_departamento": "VARCHAR(100)",
            "codigo_oficina": "VARCHAR(100)",
            "codigo_servico": "VARCHAR(100)",
            "codigo_solicitante": "VARCHAR(100)",
            "codigo_motorista": "VARCHAR(100)",
            "numero_ocorrencia": "VARCHAR(100)",
            "numero_contrato": "VARCHAR(100)",
            "valor_acrescimo": "VARCHAR(100)",
            "numero_os_retorno": "VARCHAR(100)",
            "observacoes": "TEXT",
            "investimento": "BOOLEAN DEFAULT FALSE",
            "acidente": "BOOLEAN DEFAULT FALSE",
            "socorro": "BOOLEAN DEFAULT FALSE",
            "servico_retorno": "BOOLEAN DEFAULT FALSE",
            "programada": "BOOLEAN DEFAULT FALSE",
            "status_envio": "VARCHAR(50) DEFAULT 'PENDENTE'",
            "retorno_envio": "TEXT",
            "payload_original": "TEXT",
            "campos_brutos": "TEXT",
            "fotos_app": "TEXT",
            "app_payload": "TEXT",
            "created_at": "TIMESTAMP WITH TIME ZONE DEFAULT now()",
            "updated_at": "TIMESTAMP WITH TIME ZONE DEFAULT now()",
            "deleted_at": "TIMESTAMP WITH TIME ZONE",
        }
        for nome, tipo in os_cols.items():
            conn.execute(text(f"ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS {nome} {tipo}"))
        conn.execute(text("UPDATE ordens_servico SET status_envio = 'PENDENTE' WHERE status_envio IS NULL"))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ordens_servico_servicos (
                id SERIAL PRIMARY KEY,
                empresa VARCHAR(100),
                usuario VARCHAR(255),
                senha VARCHAR(255),
                filial VARCHAR(100),
                recurso_humano VARCHAR(100),
                numero_os VARCHAR(100),
                codigo_veiculo VARCHAR(100),
                placa VARCHAR(50),
                codigo_servico VARCHAR(100),
                codigo_recurso_humano VARCHAR(100),
                tempo_gasto VARCHAR(100),
                valor_hora VARCHAR(100),
                status_envio VARCHAR(50) DEFAULT 'PENDENTE',
                retorno_envio TEXT,
                payload_original TEXT,
                campos_brutos TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                deleted_at TIMESTAMP WITH TIME ZONE
            )
        """))

        serv_cols = {
            "empresa": "VARCHAR(100)",
            "usuario": "VARCHAR(255)",
            "senha": "VARCHAR(255)",
            "filial": "VARCHAR(100)",
            "recurso_humano": "VARCHAR(100)",
            "numero_os": "VARCHAR(100)",
            "codigo_veiculo": "VARCHAR(100)",
            "placa": "VARCHAR(50)",
            "codigo_servico": "VARCHAR(100)",
            "codigo_recurso_humano": "VARCHAR(100)",
            "tempo_gasto": "VARCHAR(100)",
            "valor_hora": "VARCHAR(100)",
            "status_envio": "VARCHAR(50) DEFAULT 'PENDENTE'",
            "retorno_envio": "TEXT",
            "payload_original": "TEXT",
            "campos_brutos": "TEXT",
            "created_at": "TIMESTAMP WITH TIME ZONE DEFAULT now()",
            "updated_at": "TIMESTAMP WITH TIME ZONE DEFAULT now()",
            "deleted_at": "TIMESTAMP WITH TIME ZONE",
        }
        for nome, tipo in serv_cols.items():
            conn.execute(text(f"ALTER TABLE ordens_servico_servicos ADD COLUMN IF NOT EXISTS {nome} {tipo}"))
        conn.execute(text("UPDATE ordens_servico_servicos SET status_envio = 'PENDENTE' WHERE status_envio IS NULL"))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS tentativas_envio (
                id SERIAL PRIMARY KEY,
                tipo_registro VARCHAR(20) NOT NULL,
                registro_id INTEGER NOT NULL,
                status_execucao VARCHAR(50) DEFAULT 'PENDENTE',
                status_http VARCHAR(20),
                sucesso BOOLEAN DEFAULT FALSE,
                mensagem TEXT,
                diagnostico TEXT,
                payload_enviado TEXT,
                retorno_recebido TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
            )
        """))


garantir_colunas()

# =========================================================
# HELPERS
# =========================================================

def to_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    value = str(value).strip()
    if value.lower() == "nan":
        return default
    return value


def to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "sim", "s", "yes", "y")


def pick(dados: dict, *names: str, default: str = "") -> str:
    for name in names:
        if "." in name:
            group, key = name.split(".", 1)
            source = dados.get(group) or {}
            value = source.get(key) if isinstance(source, dict) else None
        else:
            value = dados.get(name)
        value = to_str(value)
        if value:
            return value
    return default


def pick_bool(dados: dict, *names: str, default: bool = False) -> bool:
    for name in names:
        value = dados.get(name)
        if value is not None:
            return to_bool(value, default)
    return default


def data_so_data(data_hora: str) -> str:
    data_hora = to_str(data_hora)
    return data_hora.split(" ")[0] if data_hora else ""


def credenciais_from(dados: dict) -> dict:
    usuario = pick(dados, "credenciais.usuario", "credentials.usuario", "usuario", default=FROTAWEB_USUARIO)
    return {
        "empresa": pick(dados, "credenciais.empresa", "credentials.empresa", "empresa", default=FROTAWEB_EMPRESA),
        "usuario": usuario,
        "senha": pick(dados, "credenciais.senha", "credentials.senha", "senha", default=FROTAWEB_SENHA),
        "filial": pick(dados, "credenciais.filial", "credentials.filial", "filial", "branch_code", default=FROTAWEB_FILIAL),
        "recurso_humano": pick(
            dados,
            "credenciais.recurso_humano",
            "credentials.recurso_humano",
            "recurso_humano",
            "resource_code",
            "codigo_recurso_humano",
            default=FROTAWEB_RECURSO_HUMANO or usuario,
        ),
    }


def primeira_linha_mensagem(value: Any, fallback: str = "Sem retorno") -> str:
    texto = mensagem_frotaweb(value, fallback)
    for separador in ("\n", " | "):
        if separador in texto:
            return texto.split(separador, 1)[0].strip()
    return texto.strip()


def validar_credenciais_painel(credenciais: dict, *, origem: str) -> None:
    faltando: list[str] = []
    if not str(credenciais.get("empresa") or "").strip():
        faltando.append("empresa")
    if not str(credenciais.get("usuario") or "").strip():
        faltando.append("usuario")
    if not str(credenciais.get("senha") or "").strip():
        faltando.append("senha")
    if not str(credenciais.get("recurso_humano") or "").strip():
        faltando.append("recurso_humano")
    if faltando:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Credenciais obrigatorias ausentes no envio do {origem}: "
                + ", ".join(faltando)
                + ". Preencha usuario, senha e recurso humano no app antes de enviar."
            ),
        )


def limpar_payload_para_painel(valor: Any) -> Any:
    if isinstance(valor, dict):
        removidos = {"photos", "fotos", "imagens", "data_base64", "base64", "content_type", "filename"}
        novo = {}
        for chave, item in valor.items():
            if str(chave).lower() in removidos:
                continue
            novo[chave] = limpar_payload_para_painel(item)
        return novo
    if isinstance(valor, list):
        return [limpar_payload_para_painel(item) for item in valor if not (isinstance(item, dict) and any(str(k).lower() in {"data_base64", "base64"} for k in item.keys()))]
    return valor


def montar_payload_os(dados: dict) -> dict:
    abertura = pick(dados, "data_hora_abertura", "opening_datetime", "dh_entrada")
    saida = pick(dados, "data_hora_saida", "exit_datetime", "dh_saida")
    hodometro = pick(dados, "hodometro", "odometer", "km_entrada", default="0")
    hodometro_saida = pick(dados, "hodometro_saida", "exit_odometer", "km_saida", default=hodometro)

    return {
        "credenciais": credenciais_from(dados),
        "codigo_veiculo": pick(dados, "codigo_veiculo", "vehicle_code", "cd_veiculo"),
        "descricao_defeito": pick(dados, "descricao_defeito", "defect_description", "observacao", "observacoes"),
        "numero_os": pick(dados, "numero_os", "order_number", default=""),
        "placa": pick(dados, "placa", "plate").upper(),
        "codigo_componente": pick(dados, "codigo_componente", "component_code", default=""),
        "data_abertura": pick(dados, "data_abertura", default=data_so_data(abertura)),
        "data_hora_abertura": abertura,
        "hodometro": hodometro,
        "horimetro_entrada": pick(dados, "horimetro_entrada", "entry_hourmeter", default="0.00"),
        "data_hora_saida": saida,
        "hodometro_saida": hodometro_saida,
        "horimetro_saida": pick(dados, "horimetro_saida", "exit_hourmeter", default="0.00"),
        "data_hora_inicio": pick(dados, "data_hora_inicio", "start_datetime", default=abertura),
        "data_hora_previsao_liberacao": pick(dados, "data_hora_previsao_liberacao", "expected_release_datetime", default=saida),
        "horas_previstas": pick(dados, "horas_previstas", "expected_hours", default="0.00"),
        "horas_realizadas": pick(dados, "horas_realizadas", "actual_hours", default="0.00"),
        "codigo_filial": pick(dados, "codigo_filial", "branch_code", "cd_filial"),
        "codigo_departamento": pick(dados, "codigo_departamento", "department_code", "cd_ccusto"),
        "codigo_oficina": pick(dados, "codigo_oficina", "workshop_code", default=""),
        "codigo_servico": pick(dados, "codigo_servico", "service_code", default=""),
        "codigo_solicitante": pick(dados, "codigo_solicitante", "requester_code", default=""),
        "codigo_motorista": pick(dados, "codigo_motorista", "driver_code", default="0"),
        "numero_ocorrencia": pick(dados, "numero_ocorrencia", "occurrence_number", default="0"),
        "numero_contrato": pick(dados, "numero_contrato", "contract_number", default=""),
        "valor_acrescimo": pick(dados, "valor_acrescimo", "surcharge_value", default="0"),
        "numero_os_retorno": pick(dados, "numero_os_retorno", "return_order_number", default="0"),
        "observacoes": pick(dados, "observacoes", "observations", default=""),
        "investimento": pick_bool(dados, "investimento", "investment", default=False),
        "acidente": pick_bool(dados, "acidente", "accident", default=False),
        "socorro": pick_bool(dados, "socorro", "roadside_assistance", default=False),
        "servico_retorno": pick_bool(dados, "servico_retorno", "return_service", default=False),
        "programada": pick_bool(dados, "programada", "scheduled", default=False),
        "campos_brutos": dados.get("campos_brutos") if isinstance(dados.get("campos_brutos"), dict) else {},
    }


def montar_payload_servico(dados: dict) -> dict:
    return {
        "credenciais": credenciais_from(dados),
        "numero_os": pick(dados, "numero_os", "order_number"),
        "codigo_veiculo": pick(dados, "codigo_veiculo", "vehicle_code", "cd_veiculo"),
        "placa": pick(dados, "placa", "plate").upper(),
        "codigo_servico": pick(dados, "codigo_servico", "service_code", default="0"),
        "codigo_recurso_humano": pick(
            dados,
            "codigo_recurso_humano",
            "resource_code",
            "credenciais.recurso_humano",
            "credentials.recurso_humano",
            default=FROTAWEB_RECURSO_HUMANO,
        ),
        "tempo_gasto": pick(dados, "tempo_gasto", "spent_time", default="000:00"),
        "valor_hora": pick(dados, "valor_hora", "hour_value", default="0"),
        "campos_brutos": dados.get("campos_brutos") if isinstance(dados.get("campos_brutos"), dict) else {},
    }


def payload_os_from_ordem(o: OrdemServico) -> dict:
    return {
        "credenciais": {
            "empresa": to_str(o.empresa),
            "usuario": to_str(o.usuario),
            "senha": to_str(o.senha),
            "filial": to_str(o.filial),
            "recurso_humano": to_str(o.recurso_humano),
        },
        "codigo_veiculo": to_str(o.codigo_veiculo),
        "descricao_defeito": to_str(o.descricao_defeito),
        "numero_os": to_str(o.numero_os),
        "placa": to_str(o.placa).upper(),
        "codigo_componente": to_str(o.codigo_componente),
        "data_abertura": to_str(o.data_abertura),
        "data_hora_abertura": to_str(o.data_hora_abertura),
        "hodometro": to_str(o.hodometro, "0"),
        "horimetro_entrada": to_str(o.horimetro_entrada, "0.00"),
        "data_hora_saida": to_str(o.data_hora_saida),
        "hodometro_saida": to_str(o.hodometro_saida, to_str(o.hodometro, "0")),
        "horimetro_saida": to_str(o.horimetro_saida, "0.00"),
        "data_hora_inicio": to_str(o.data_hora_inicio, to_str(o.data_hora_abertura)),
        "data_hora_previsao_liberacao": to_str(o.data_hora_previsao_liberacao, to_str(o.data_hora_saida)),
        "horas_previstas": to_str(o.horas_previstas, "0.00"),
        "horas_realizadas": to_str(o.horas_realizadas, "0.00"),
        "codigo_filial": to_str(o.codigo_filial),
        "codigo_departamento": to_str(o.codigo_departamento),
        "codigo_oficina": to_str(o.codigo_oficina),
        "codigo_servico": to_str(o.codigo_servico),
        "codigo_solicitante": to_str(o.codigo_solicitante),
        "codigo_motorista": to_str(o.codigo_motorista, "0"),
        "numero_ocorrencia": to_str(o.numero_ocorrencia, "0"),
        "numero_contrato": to_str(o.numero_contrato),
        "valor_acrescimo": to_str(o.valor_acrescimo, "0"),
        "numero_os_retorno": to_str(o.numero_os_retorno, "0"),
        "observacoes": to_str(o.observacoes),
        "investimento": bool(o.investimento),
        "acidente": bool(o.acidente),
        "socorro": bool(o.socorro),
        "servico_retorno": bool(o.servico_retorno),
        "programada": bool(o.programada),
    }


def payload_servico_from_model(s: ServicoOS) -> dict:
    return {
        "credenciais": {
            "empresa": to_str(s.empresa),
            "usuario": to_str(s.usuario),
            "senha": to_str(s.senha),
            "filial": to_str(s.filial),
            "recurso_humano": to_str(s.recurso_humano),
        },
        "numero_os": to_str(s.numero_os),
        "codigo_veiculo": to_str(s.codigo_veiculo),
        "placa": to_str(s.placa).upper(),
        "codigo_servico": to_str(s.codigo_servico, "0"),
        "codigo_recurso_humano": to_str(s.codigo_recurso_humano),
        "tempo_gasto": to_str(s.tempo_gasto, "000:00"),
        "valor_hora": to_str(s.valor_hora, "0"),
    }


def post_json(url: str, payload: dict) -> dict:
    diagnostics: dict[str, Any] = {
        "url": url,
        "tentativas": 0,
        "status_http": None,
        "wakeup_executado": False,
    }
    if HTTP_WAKEUP_ENABLED:
        health_url = url.rsplit("/", 1)[0] + "/health"
        diagnostics["wakeup_executado"] = True
        diagnostics["health_url"] = health_url
        try:
            requests.get(health_url, timeout=30, verify=HTTP_VERIFY_TLS)
            time.sleep(HTTP_WAKEUP_DELAY_SECONDS)
        except Exception as exc:
            diagnostics["wakeup_erro"] = str(exc)

    resp = None
    last_error = None
    for tentativa in range(1, HTTP_MAX_RETRIES + 1):
        diagnostics["tentativas"] = tentativa
        try:
            resp = requests.post(url, json=payload, timeout=120, verify=HTTP_VERIFY_TLS)
        except Exception as exc:
            last_error = exc
            diagnostics["ultimo_erro_conexao"] = str(exc)
            if tentativa >= HTTP_MAX_RETRIES:
                raise HTTPException(
                    status_code=500,
                    detail={
                        "url": url,
                        "mensagem_frotaweb": f"Erro ao conectar na integracao apos {tentativa} tentativa(s): {str(exc)}",
                        "diagnostico": diagnostics,
                    },
                )
            time.sleep(HTTP_RETRY_BACKOFF_SECONDS * tentativa)
            continue

        diagnostics["status_http"] = resp.status_code
        if resp.status_code not in HTTP_RETRYABLE_STATUS_CODES:
            break

        retry_after = HTTP_RETRY_BACKOFF_SECONDS * tentativa
        if resp.status_code == 429:
            retry_after_header = str(resp.headers.get("Retry-After") or "").strip()
            try:
                retry_after = max(1, int(retry_after_header))
            except Exception:
                retry_after = HTTP_RETRY_BACKOFF_SECONDS * tentativa

        if tentativa >= HTTP_MAX_RETRIES:
            break

        diagnostics["ultima_espera_segundos"] = retry_after
        logger.warning(
            "Recebido %s ao enviar para %s; aguardando %ss antes da nova tentativa",
            resp.status_code,
            url,
            retry_after,
        )
        time.sleep(retry_after)

    if resp is None:
        raise HTTPException(
            status_code=500,
            detail={
                "url": url,
                "mensagem_frotaweb": f"Erro ao conectar: {str(last_error)}",
                "diagnostico": diagnostics,
            },
        )

    try:
        retorno = resp.json()
    except Exception:
        retorno = {"raw": resp.text}

    if resp.status_code not in (200, 201):
        if resp.status_code == 429:
            raise HTTPException(
                status_code=429,
                detail={
                    "url": url,
                    "mensagem_frotaweb": "Too Many Requests. O painel tentou novamente, mas a API ainda estava limitando as requisicoes. Aguarde alguns segundos e reenvie.",
                    "retorno": mascarar_senha(retorno),
                    "diagnostico": diagnostics,
                },
            )
        if resp.status_code in {502, 503, 504}:
            raise HTTPException(
                status_code=resp.status_code,
                detail={
                    "url": url,
                    "mensagem_frotaweb": "A integracao ficou indisponivel temporariamente. O painel tentou novamente, mas o servico continuou retornando erro de gateway. Aguarde alguns segundos e reenvie.",
                    "retorno": mascarar_senha(retorno),
                    "diagnostico": diagnostics,
                },
            )
        raise HTTPException(
            status_code=resp.status_code,
            detail={
                "url": url,
                "mensagem_frotaweb": mensagem_frotaweb(retorno),
                "retorno": mascarar_senha(retorno),
                "diagnostico": diagnostics,
            },
        )

    return retorno




def extrair_numero_os(retorno):
    """
    Tenta encontrar o numero da O.S. no retorno da API FrotaWeb.
    Aceita retornos com chaves diferentes, pois cada integracao pode nomear o campo de um jeito.
    """
    candidatos = {
        "numero_os", "nr_os", "num_os", "os", "codigo_os", "cod_os",
        "order_number", "ordem_servico", "ordem", "id_os", "id"
    }

    def walk(obj):
        if isinstance(obj, dict):
            # Primeiro procura chaves mais provaveis.
            for chave, valor in obj.items():
                if str(chave).lower() in candidatos and valor not in (None, "", 0):
                    texto = str(valor).strip()
                    if texto and texto.lower() not in ("true", "false", "none", "null"):
                        return texto
            # Depois procura em estruturas aninhadas.
            for valor in obj.values():
                encontrado = walk(valor)
                if encontrado:
                    return encontrado
        elif isinstance(obj, list):
            for item in obj:
                encontrado = walk(item)
                if encontrado:
                    return encontrado
        return ""

    return walk(retorno)


def contar_servicos_da_os(db: Session, ordem: OrdemServico):
    numeros = {str(ordem.id)}
    if ordem.numero_os:
        numeros.add(str(ordem.numero_os))

    query = db.query(ServicoOS).filter(ServicoOS.deleted_at.is_(None))
    todos = query.all()

    relacionados = []
    for serv in todos:
        mesmo_numero = str(serv.numero_os or "") in numeros
        mesmo_veiculo = (
            ordem.codigo_veiculo
            and serv.codigo_veiculo
            and str(serv.codigo_veiculo).strip() == str(ordem.codigo_veiculo).strip()
        )
        mesma_placa = (
            ordem.placa
            and serv.placa
            and str(serv.placa).strip().upper() == str(ordem.placa).strip().upper()
        )
        if mesmo_numero or (mesmo_veiculo and mesma_placa):
            relacionados.append(serv)

    total = len(relacionados)
    enviados = len([s for s in relacionados if s.status_envio == "ENVIADA"])
    erros = len([s for s in relacionados if str(s.status_envio or "").startswith("ERRO")])
    pendentes = total - enviados - erros
    return total, enviados, erros, pendentes, relacionados


def enviar_servicos_relacionados(db: Session, ordem: OrdemServico, numero_os_frotaweb: str):
    """
    Depois que a O.S. e enviada com sucesso, tenta enviar automaticamente os servicos vinculados.
    Se algum servico falhar, a O.S. continua ENVIADA, mas o retorno mostra quantos servicos falharam.
    """
    total, enviados_antes, erros_antes, pendentes_antes, relacionados = contar_servicos_da_os(db, ordem)

    enviados_agora = 0
    erros_agora = 0
    detalhes = []

    for serv in relacionados:
        if serv.status_envio == "ENVIADA":
            continue

        if numero_os_frotaweb:
            serv.numero_os = numero_os_frotaweb

        if not serv.codigo_veiculo:
            serv.codigo_veiculo = ordem.codigo_veiculo
        if not serv.placa:
            serv.placa = ordem.placa

        payload_serv = payload_servico_from_model(serv)

        try:
            retorno_serv = post_json(FROTAWEB_SERVICO_URL, payload_serv)
            if resposta_criada_com_sucesso(retorno_serv):
                serv.status_envio = "ENVIADA"
                serv.retorno_envio = mensagem_frotaweb(retorno_serv, "Serviço enviado com sucesso.")
                enviados_agora += 1
                detalhes.append(f"servico_id={serv.id}: ENVIADO")
            else:
                serv.status_envio = "ERRO_ENVIO"
                serv.retorno_envio = mensagem_frotaweb(retorno_serv)
                erros_agora += 1
                detalhes.append(f"servico_id={serv.id}: ERRO")
        except HTTPException as exc:
            serv.status_envio = "ERRO_ENVIO"
            serv.retorno_envio = mensagem_frotaweb(exc.detail)
            erros_agora += 1
            detalhes.append(f"servico_id={serv.id}: ERRO")

    db.commit()

    total_final, enviados_final, erros_final, pendentes_final, _ = contar_servicos_da_os(db, ordem)

    return {
        "total": total_final,
        "enviados": enviados_final,
        "erros": erros_final,
        "pendentes": pendentes_final,
        "enviados_agora": enviados_agora,
        "erros_agora": erros_agora,
        "detalhes": detalhes,
    }


def processar_servico_em_fila(servico_id: int) -> None:
    db = SessionLocal()
    try:
        serv = db.query(ServicoOS).filter(ServicoOS.id == servico_id).filter(ServicoOS.deleted_at.is_(None)).first()
        if not serv:
            return

        serv.status_envio = "PROCESSANDO"
        db.commit()

        payload = payload_servico_from_model(serv)
        try:
            retorno = post_json(FROTAWEB_SERVICO_URL, payload)
        except HTTPException as exc:
            serv.status_envio = "ERRO_ENVIO"
            serv.retorno_envio = mensagem_frotaweb(exc.detail)
            registrar_tentativa(
                db,
                tipo_registro="SERVICO",
                registro_id=serv.id,
                status_execucao="ERRO_ENVIO",
                status_http=str(getattr(exc, "status_code", "")),
                sucesso=False,
                mensagem=serv.retorno_envio,
                diagnostico=exc.detail,
                payload_enviado=payload,
                retorno_recebido=exc.detail,
            )
            db.commit()
            return

        if resposta_criada_com_sucesso(retorno):
            serv.status_envio = "ENVIADA"
            serv.retorno_envio = mensagem_frotaweb(retorno, "Serviço enviado com sucesso.")
            registrar_tentativa(
                db,
                tipo_registro="SERVICO",
                registro_id=serv.id,
                status_execucao="ENVIADA",
                status_http="200",
                sucesso=True,
                mensagem=serv.retorno_envio,
                payload_enviado=payload,
                retorno_recebido=retorno,
            )
        else:
            serv.status_envio = "ERRO_ENVIO"
            serv.retorno_envio = mensagem_frotaweb(retorno)
            registrar_tentativa(
                db,
                tipo_registro="SERVICO",
                registro_id=serv.id,
                status_execucao="ERRO_ENVIO",
                status_http="200",
                sucesso=False,
                mensagem=serv.retorno_envio,
                payload_enviado=payload,
                retorno_recebido=retorno,
            )
        db.commit()
    finally:
        db.close()


def processar_ordem_em_fila(ordem_id: int) -> None:
    db = SessionLocal()
    try:
        ordem = db.query(OrdemServico).filter(OrdemServico.id == ordem_id).filter(OrdemServico.deleted_at.is_(None)).first()
        if not ordem:
            return

        ordem.status_envio = "PROCESSANDO"
        db.commit()

        payload = payload_os_from_ordem(ordem)
        try:
            retorno = post_json(FROTAWEB_OS_URL, payload)
        except HTTPException as exc:
            ordem.status_envio = "ERRO_ENVIO"
            ordem.retorno_envio = mensagem_frotaweb(exc.detail)
            registrar_tentativa(
                db,
                tipo_registro="OS",
                registro_id=ordem.id,
                status_execucao="ERRO_ENVIO",
                status_http=str(getattr(exc, "status_code", "")),
                sucesso=False,
                mensagem=ordem.retorno_envio,
                diagnostico=exc.detail,
                payload_enviado=payload,
                retorno_recebido=exc.detail,
            )
            db.commit()
            return

        if not resposta_criada_com_sucesso(retorno):
            ordem.status_envio = "ERRO_ENVIO"
            ordem.retorno_envio = mensagem_frotaweb(retorno)
            registrar_tentativa(
                db,
                tipo_registro="OS",
                registro_id=ordem.id,
                status_execucao="ERRO_ENVIO",
                status_http="200",
                sucesso=False,
                mensagem=ordem.retorno_envio,
                payload_enviado=payload,
                retorno_recebido=retorno,
            )
            db.commit()
            return

        numero_os_frotaweb = extrair_numero_os(retorno)
        if numero_os_frotaweb:
            ordem.numero_os = numero_os_frotaweb

        resumo_servicos = enviar_servicos_relacionados(db, ordem, numero_os_frotaweb or str(ordem.numero_os or ordem.id))
        ordem.status_envio = "ENVIADA"
        ordem.retorno_envio = (
            f"O.S. enviada com sucesso. "
            f"Nº O.S. FrotaWeb: {numero_os_frotaweb or ordem.numero_os or 'não identificado'}. "
            f"Serviços: {resumo_servicos['enviados']}/{resumo_servicos['total']} enviados, "
            f"{resumo_servicos['erros']} erro(s), {resumo_servicos['pendentes']} pendente(s). "
            f"{mensagem_frotaweb(retorno, 'O.S. enviada com sucesso.')}"
        )
        registrar_tentativa(
            db,
            tipo_registro="OS",
            registro_id=ordem.id,
            status_execucao="ENVIADA",
            status_http="200",
            sucesso=True,
            mensagem=ordem.retorno_envio,
            payload_enviado=payload,
            retorno_recebido=retorno,
            diagnostico={"servicos": resumo_servicos},
        )
        db.commit()
    finally:
        db.close()


def processar_proximo_item_da_fila() -> bool:
    with worker_lock:
        db = SessionLocal()
        try:
            ordem = (
                db.query(OrdemServico)
                .filter(OrdemServico.deleted_at.is_(None))
                .filter(OrdemServico.status_envio == "EM_FILA")
                .order_by(OrdemServico.created_at.asc(), OrdemServico.id.asc())
                .first()
            )
            if ordem:
                ordem_id = ordem.id
                db.expunge(ordem)
                db.close()
                processar_ordem_em_fila(ordem_id)
                return True

            servico = (
                db.query(ServicoOS)
                .filter(ServicoOS.deleted_at.is_(None))
                .filter(ServicoOS.status_envio == "EM_FILA")
                .order_by(ServicoOS.created_at.asc(), ServicoOS.id.asc())
                .first()
            )
            if servico:
                servico_id = servico.id
                db.expunge(servico)
                db.close()
                processar_servico_em_fila(servico_id)
                return True
            return False
        finally:
            if db.is_active:
                db.close()


def loop_worker_fila() -> None:
    logger.info("Modo de envio iniciado com polling de %ss", QUEUE_POLL_SECONDS)
    while not worker_stop_event.is_set():
        try:
            processou = processar_proximo_item_da_fila()
            if processou:
                continue
        except Exception:
            logger.exception("Falha inesperada no worker da fila")
        worker_stop_event.wait(QUEUE_POLL_SECONDS)
    logger.info("Modo de envio encerrado")


def resumo_resultado_envio(status_envio: str, retorno_envio: str, prefixo_ok: str, prefixo_erro: str) -> str:
    mensagem = mensagem_frotaweb(retorno_envio, "")
    if status_envio == "ENVIADA":
        return f"{prefixo_ok} {mensagem}".strip()
    if mensagem:
        return f"{prefixo_erro} {mensagem}".strip()
    return prefixo_erro


def status_class(status: str) -> str:
    if status == "ENVIADA":
        return "status-enviada"
    if status in ("EM_FILA", "PROCESSANDO"):
        return "status-pendente"
    if status.startswith("ERRO"):
        return "status-erro"
    return "status-pendente"


def mascarar_senha(valor: Any) -> Any:
    """
    Remove/mascara senha antes de gravar logs e retornos.
    """
    if isinstance(valor, dict):
        novo = {}
        for chave, item in valor.items():
            if str(chave).lower() == "senha":
                novo[chave] = "***"
            else:
                novo[chave] = mascarar_senha(item)
        return novo
    if isinstance(valor, list):
        return [mascarar_senha(item) for item in valor]
    return valor


def escape_html(valor: Any) -> str:
    if valor is None:
        return ""
    return (
        str(valor)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#039;")
    )


def json_dumps_safe(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def json_loads_safe(value: str, default: Any = None) -> Any:
    try:
        return json.loads(value or "")
    except Exception:
        return default


def registrar_tentativa(
    db: Session,
    *,
    tipo_registro: str,
    registro_id: int,
    status_execucao: str,
    status_http: str = "",
    sucesso: bool = False,
    mensagem: str = "",
    diagnostico: Any = None,
    payload_enviado: Any = None,
    retorno_recebido: Any = None,
) -> None:
    db.add(
        TentativaEnvio(
            tipo_registro=tipo_registro,
            registro_id=registro_id,
            status_execucao=status_execucao,
            status_http=to_str(status_http),
            sucesso=sucesso,
            mensagem=to_str(mensagem),
            diagnostico=json_dumps_safe(mascarar_senha(diagnostico)),
            payload_enviado=json_dumps_safe(mascarar_senha(payload_enviado)),
            retorno_recebido=json_dumps_safe(mascarar_senha(retorno_recebido)),
        )
    )


def tentativas_do_registro(db: Session, tipo_registro: str, registro_id: int) -> list[TentativaEnvio]:
    return (
        db.query(TentativaEnvio)
        .filter(TentativaEnvio.tipo_registro == tipo_registro, TentativaEnvio.registro_id == registro_id)
        .order_by(TentativaEnvio.created_at.desc(), TentativaEnvio.id.desc())
        .all()
    )


def resumo_fila(status: str) -> str:
    mapa = {
        "PENDENTE": "Pendente",
        "EM_FILA": "Em fila",
        "PROCESSANDO": "Processando",
        "ENVIADA": "Enviada",
        "ERRO_ENVIO": "Com erro",
    }
    return mapa.get(status or "PENDENTE", status or "PENDENTE")


def flatten_messages(value: Any) -> list[str]:
    mensagens: list[str] = []
    if isinstance(value, dict):
        for chave in ("message", "detail", "erro", "error"):
            conteudo = value.get(chave)
            if isinstance(conteudo, str) and conteudo.strip():
                mensagens.append(conteudo.strip())
            elif conteudo not in (None, "", [], {}):
                mensagens.extend(flatten_messages(conteudo))
        retorno = value.get("retorno")
        if retorno not in (None, "", [], {}):
            mensagens.extend(flatten_messages(retorno))
        raw = value.get("raw")
        if isinstance(raw, str) and raw.strip():
            mensagens.append(raw.strip())
    elif isinstance(value, list):
        for item in value:
            mensagens.extend(flatten_messages(item))
    elif isinstance(value, str) and value.strip():
        mensagens.append(value.strip())
    # preserva ordem, remove repetidos
    return list(dict.fromkeys(mensagens))


def mensagem_frotaweb(value: Any, fallback: str = "Erro ao enviar ao FrotaWeb.") -> str:
    mensagens = flatten_messages(value)
    if not mensagens:
        return fallback
    return " | ".join(mensagens)


def resposta_criada_com_sucesso(retorno: Any) -> bool:
    if isinstance(retorno, dict) and "created" in retorno:
        return bool(retorno.get("created"))
    return True


def retorno_resumido_html(value: Any, detail_url: str, success: bool = False) -> str:
    resumo = escape_html(primeira_linha_mensagem(value, "Sem retorno"))
    css = "retorno-ok" if success else "retorno-erro"
    return (
        f"<div class='retorno-bloco {css}'>"
        f"<div class='retorno-resumo'>{resumo}</div>"
        f"<div class='retorno-detalhes'><a href='{escape_html(detail_url)}' target='_blank' rel='noopener noreferrer'>Ver detalhes</a></div>"
        f"</div>"
    )


def limpar_base64(data_base64: str) -> str:
    texto = str(data_base64 or "").strip()
    if "," in texto and texto.lower().startswith("data:"):
        return texto.split(",", 1)[1]
    return texto


def salvar_fotos_app(dados: dict) -> list[str]:
    fotos = dados.get("photos") or dados.get("fotos") or dados.get("imagens") or []
    if not isinstance(fotos, list):
        return []

    caminhos: list[str] = []
    for idx, foto in enumerate(fotos, start=1):
        if not isinstance(foto, dict):
            continue

        data_base64 = foto.get("data_base64") or foto.get("base64") or foto.get("data")
        if not data_base64:
            continue

        try:
            raw = base64.b64decode(limpar_base64(str(data_base64)), validate=False)
        except Exception:
            continue

        filename = str(foto.get("filename") or foto.get("nome") or f"foto_os_{idx}.jpg")
        ext = os.path.splitext(filename)[-1].lower()
        if ext not in (".jpg", ".jpeg", ".png", ".webp"):
            ext = ".jpg"

        nome_arquivo = f"os_{int(datetime.now().timestamp() * 1000)}_{idx}{ext}"
        destino = os.path.join(UPLOAD_DIR, nome_arquivo)
        with open(destino, "wb") as f:
            f.write(raw)

        caminhos.append(f"/fotos/{nome_arquivo}")

    return caminhos


def fotos_da_ordem(ordem: OrdemServico) -> list[str]:
    fotos = json_loads_safe(ordem.fotos_app or "", [])
    if isinstance(fotos, list):
        return [str(foto) for foto in fotos if str(foto).strip()]
    return []


def html_fotos_ordem(ordem: OrdemServico) -> str:
    fotos = fotos_da_ordem(ordem)
    if not fotos:
        return "<span class='small'>Sem foto</span>"

    partes = []
    for idx, foto in enumerate(fotos, start=1):
        foto_esc = escape_html(foto)
        partes.append(
            f"<a href='{foto_esc}' target='_blank' title='Foto {idx}'>"
            f"<img src='{foto_esc}' class='foto-thumb' alt='Foto {idx}'>"
            f"</a>"
        )

    return "<div class='foto-grid'>" + "".join(partes) + "</div>"


def payload_original_dict(ordem: OrdemServico) -> dict:
    payload = json_loads_safe(ordem.app_payload or "", None)
    if isinstance(payload, dict):
        return payload

    payload = json_loads_safe(ordem.payload_original or "", None)
    if isinstance(payload, dict):
        return payload

    return {}


def contem(valor: Any, filtro: Any) -> bool:
    texto_filtro = str(filtro or "").strip().lower()
    if not texto_filtro:
        return True
    return texto_filtro in str(valor or "").lower()


def filtrar_ordens(ordens: list[OrdemServico], filtros: dict) -> list[OrdemServico]:
    filtradas = []
    for o in ordens:
        servicos_txt = filtros.get("_servicos_texto", {}).get(o.id, "")
        if not contem(o.id, filtros.get("id")):
            continue
        if not contem(o.status_envio, filtros.get("status_envio")):
            continue
        if not contem(o.numero_os, filtros.get("numero_os")):
            continue
        if not contem(servicos_txt, filtros.get("servicos")):
            continue
        if not contem(o.codigo_veiculo, filtros.get("codigo_veiculo")):
            continue
        if not contem(o.placa, filtros.get("placa")):
            continue
        if not contem(o.hodometro, filtros.get("hodometro")):
            continue
        if not contem(o.data_hora_abertura, filtros.get("data_hora_abertura")):
            continue
        if not contem(o.data_hora_saida, filtros.get("data_hora_saida")):
            continue
        if not contem(o.codigo_filial, filtros.get("codigo_filial")):
            continue
        if not contem(o.codigo_departamento, filtros.get("codigo_departamento")):
            continue
        if not contem(o.descricao_defeito, filtros.get("descricao_defeito")):
            continue
        if not contem(o.retorno_envio, filtros.get("retorno_envio")):
            continue
        if not contem(o.created_at.strftime("%d/%m/%Y %H:%M") if o.created_at else "", filtros.get("created_at")):
            continue
        if filtros.get("fotos") and not fotos_da_ordem(o):
            continue
        filtradas.append(o)
    return filtradas


def ordem_edit_fields() -> list[tuple[str, str, str]]:
    return [
        ("empresa", "Empresa", "text"),
        ("usuario", "Usuário", "text"),
        ("senha", "Senha", "password"),
        ("filial", "Filial login", "text"),
        ("recurso_humano", "Recurso humano", "text"),
        ("codigo_veiculo", "Código veículo", "text"),
        ("descricao_defeito", "Descrição defeito", "textarea"),
        ("numero_os", "Nº O.S. FrotaWeb", "text"),
        ("placa", "Placa", "text"),
        ("codigo_componente", "Código componente", "text"),
        ("data_abertura", "Data abertura", "text"),
        ("data_hora_abertura", "Data/hora abertura", "text"),
        ("hodometro", "Hodômetro entrada", "text"),
        ("horimetro_entrada", "Horímetro entrada", "text"),
        ("data_hora_saida", "Data/hora saída", "text"),
        ("hodometro_saida", "Hodômetro saída", "text"),
        ("horimetro_saida", "Horímetro saída", "text"),
        ("data_hora_inicio", "Data/hora início", "text"),
        ("data_hora_previsao_liberacao", "Previsão liberação", "text"),
        ("horas_previstas", "Horas previstas", "text"),
        ("horas_realizadas", "Horas realizadas", "text"),
        ("codigo_filial", "Código filial", "text"),
        ("codigo_departamento", "Código departamento", "text"),
        ("codigo_oficina", "Código oficina", "text"),
        ("codigo_servico", "Código serviço", "text"),
        ("codigo_solicitante", "Código solicitante", "text"),
        ("codigo_motorista", "Código motorista", "text"),
        ("numero_ocorrencia", "Número ocorrência", "text"),
        ("numero_contrato", "Número contrato", "text"),
        ("valor_acrescimo", "Valor acréscimo", "text"),
        ("numero_os_retorno", "Nº O.S. retorno", "text"),
        ("observacoes", "Observações", "textarea"),
    ]


def render_edit_form_field(ordem: OrdemServico, field_name: str, label: str, field_type: str) -> str:
    value = getattr(ordem, field_name, "") or ""
    if field_type == "textarea":
        return f"""
        <label>{escape_html(label)}
          <textarea name="{field_name}">{escape_html(value)}</textarea>
        </label>
        """
    return f"""
    <label>{escape_html(label)}
      <input type="{field_type}" name="{field_name}" value="{escape_html(value)}">
    </label>
    """


def render_checkbox(name: str, label: str, checked: bool) -> str:
    marcado = "checked" if checked else ""
    return f"""
    <label class="check-line">
      <input type="checkbox" name="{name}" value="true" {marcado}>
      {escape_html(label)}
    </label>
    """


def unique_suggestions(values: list[Any], limit: int = 120) -> list[str]:
    vistos: list[str] = []
    for value in values:
        texto = str(value or "").strip()
        if not texto or texto in vistos:
            continue
        vistos.append(texto)
    return vistos[:limit]


def render_datalist(name: str, options: list[str]) -> str:
    if not options:
        return ""
    itens = "".join(f"<option value='{escape_html(option)}'></option>" for option in options)
    return f"<datalist id='lista-{escape_html(name)}'>{itens}</datalist>"


@app.on_event("startup")
def startup_worker() -> None:
    global worker_thread
    worker_stop_event.clear()
    if worker_thread and worker_thread.is_alive():
        return
    worker_thread = threading.Thread(target=loop_worker_fila, name="painel-os-worker", daemon=True)
    worker_thread.start()


@app.on_event("shutdown")
def shutdown_worker() -> None:
    worker_stop_event.set()


# =========================================================
# HTML
# =========================================================

def html_base(titulo: str, corpo: str, msg: str = "", auto_refresh_seconds: int = 0) -> str:
    return f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      {f'<meta http-equiv="refresh" content="{auto_refresh_seconds}">' if auto_refresh_seconds else ''}
      <title>{titulo}</title>
      <style>
        body {{ font-family: Arial, sans-serif; background:#f3f4f6; margin:0; padding:24px; color:#111827; }}
        .container {{ max-width:1700px; margin:0 auto; }}
        .hero {{ display:flex; justify-content:space-between; gap:18px; align-items:flex-start; margin-bottom:18px; }}
        .hero-copy h1 {{ margin:0 0 6px; font-size:28px; }}
        .hero-copy p {{ margin:0; color:#4b5563; font-size:15px; }}
        .hero-meta {{ text-align:right; color:#6b7280; font-size:12px; }}
        .card {{ background:white; border-radius:14px; padding:18px; box-shadow:0 2px 12px rgba(0,0,0,.08); margin-bottom:18px; }}
        .kpis {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; margin-bottom:18px; }}
        .kpi {{ background:white; border-radius:14px; padding:16px; box-shadow:0 2px 12px rgba(0,0,0,.06); border:1px solid #e5e7eb; }}
        .kpi .label {{ color:#6b7280; font-size:12px; text-transform:uppercase; letter-spacing:.04em; }}
        .kpi .value {{ font-size:28px; font-weight:800; margin-top:8px; }}
        .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:12px; align-items:end; }}
        .filter-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:12px; align-items:end; }}
        label {{ display:block; font-weight:700; margin-bottom:6px; color:#374151; font-size:14px; }}
        input, select {{ width:100%; box-sizing:border-box; padding:10px; border:1px solid #d1d5db; border-radius:8px; }}
        button, .btn {{ background:#2563eb; color:white; border:0; border-radius:8px; padding:10px 14px; cursor:pointer; text-decoration:none; display:inline-block; font-size:14px; font-weight:700; }}
        .btn-green {{ background:#059669; }} .btn-gray {{ background:#4b5563; }} .btn-orange {{ background:#ea580c; }} .btn-red {{ background:#dc2626; }} .btn-blue {{ background:#2563eb; }}
        .acoes {{ display:flex; gap:8px; flex-wrap:wrap; margin-top:14px; }}
        .acoes-coluna {{ display:flex; flex-direction:column; gap:10px; min-width:182px; }}
        .acoes-coluna form, .acoes-coluna a, .acoes-coluna button {{ width:100%; }}
        table {{ width:100%; border-collapse:collapse; background:white; }}
        th, td {{ padding:10px; border-bottom:1px solid #e5e7eb; text-align:left; vertical-align:top; font-size:13px; }}
        th {{ background:#111827; color:white; position:sticky; top:0; }}
        tr:hover {{ background:#f9fafb; }}
        .msg {{ background:#ecfdf5; color:#065f46; padding:12px; border-radius:8px; margin-bottom:14px; font-weight:700; }}
        .status-pendente, .status-enviada, .status-erro {{ display:inline-flex; align-items:center; border-radius:999px; padding:6px 10px; font-weight:800; font-size:12px; }}
        .status-pendente {{ color:#92400e; background:#fffbeb; }}
        .status-enviada {{ color:#065f46; background:#ecfdf5; }}
        .status-erro {{ color:#991b1b; background:#fef2f2; }}
        .small {{ color:#6b7280; font-size:12px; }}
        .subtle {{ color:#6b7280; font-size:13px; }}
        .retorno {{ min-width:320px; max-width:420px; white-space:normal; overflow-wrap:anywhere; }}
        .retorno-bloco {{ border:1px solid #e5e7eb; border-radius:10px; padding:10px 12px; background:#fff; }}
        .retorno-ok {{ border-left:4px solid #059669; }}
        .retorno-erro {{ border-left:4px solid #dc2626; }}
        .retorno-resumo {{ font-weight:700; line-height:1.45; }}
        .retorno-detalhes {{ margin-top:8px; }}
        .retorno-detalhes summary {{ cursor:pointer; color:#2563eb; font-weight:700; }}
        .retorno-detalhes pre {{ white-space:pre-wrap; overflow-wrap:anywhere; background:#111827; color:#f9fafb; border-radius:8px; padding:10px; margin-top:8px; max-height:240px; overflow:auto; }}
        .table-wrap {{ overflow:auto; }}
        .toolbar {{ display:flex; justify-content:space-between; gap:12px; align-items:center; flex-wrap:wrap; }}
        .toolbar .acoes {{ margin-top:0; }}
        .tabs {{ display:flex; gap:10px; flex-wrap:wrap; margin-bottom:18px; }}
        .tab-button {{ background:#e5e7eb; color:#111827; border:1px solid #d1d5db; }}
        .tab-button.active {{ background:#111827; color:#fff; border-color:#111827; }}
        .tab-panel {{ display:none; }}
        .tab-panel.active {{ display:block; }}
        .service-groups {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(320px,1fr)); gap:16px; }}
        .service-group-card {{ background:#fff; border:1px solid #e5e7eb; border-radius:14px; padding:16px; box-shadow:0 2px 12px rgba(0,0,0,.05); }}
        .service-group-header {{ display:flex; justify-content:space-between; gap:12px; align-items:flex-start; margin-bottom:10px; }}
        .service-group-title {{ font-size:18px; font-weight:800; }}
        .service-group-meta {{ color:#4b5563; font-size:13px; }}
        .service-pill {{ display:inline-flex; padding:4px 8px; border-radius:999px; font-size:12px; font-weight:700; background:#eef2ff; color:#3730a3; }}
        .service-list {{ display:flex; flex-direction:column; gap:10px; margin-top:12px; }}
        .service-item {{ border:1px solid #e5e7eb; border-radius:12px; padding:12px; }}
        .service-item-top {{ display:flex; justify-content:space-between; gap:10px; align-items:flex-start; }}
        .service-item-meta {{ color:#6b7280; font-size:12px; margin-top:6px; }}
        .service-item-actions {{ display:flex; gap:8px; flex-wrap:wrap; margin-top:10px; }}
        .hint {{ color:#4b5563; font-size:13px; }}
        .check-cell {{ width:38px; text-align:center; }}
        .check-cell input {{ width:16px; height:16px; }}
        @media (max-width: 980px) {{ .hero {{ flex-direction:column; }} .hero-meta {{ text-align:left; }} body {{ padding:14px; }} }}
      </style>
      <script>
        function toggleGroup(selector, checked) {{
          document.querySelectorAll(selector).forEach((el) => el.checked = checked);
        }}

        function submitSelected(formId, checkboxSelector, hiddenId, emptyMessage, confirmMessage) {{
          const selected = Array.from(document.querySelectorAll(checkboxSelector + ':checked')).map((el) => el.value);
          if (!selected.length) {{
            alert(emptyMessage);
            return false;
          }}
          if (!window.confirm(confirmMessage.replace('{{count}}', selected.length))) {{
            return false;
          }}
          document.getElementById(hiddenId).value = selected.join(',');
          document.getElementById(formId).submit();
          return false;
        }}

        function switchTab(tabName) {{
          document.querySelectorAll('[data-tab-target]').forEach((button) => {{
            button.classList.toggle('active', button.getAttribute('data-tab-target') === tabName);
          }});
          document.querySelectorAll('[data-tab-panel]').forEach((panel) => {{
            panel.classList.toggle('active', panel.getAttribute('data-tab-panel') === tabName);
          }});
        }}
      </script>
    </head>
    <body>
      <div class="container">
        <div class="hero">
          <div class="hero-copy">
            <h1>{titulo}</h1>
            <p>Receba os lancamentos do app, acompanhe a fila e enxergue o retorno exato do FrotaWeb sem ruido tecnico.</p>
          </div>
          <div class="hero-meta">
            <div>O.S.: {FROTAWEB_OS_URL}</div>
            <div>Servicos: {FROTAWEB_SERVICO_URL}</div>
          </div>
        </div>
        {f'<div class="msg">{msg}</div>' if msg else ''}
        {corpo}
      </div>
    </body>
    </html>
    """


def render_painel(ordens, servicos, filtros: Optional[dict] = None, msg: str = "") -> str:
    filtros = filtros or {}

    servicos_por_ordem = {}
    for ordem in ordens:
        relacionados = [
            s for s in servicos
            if str(s.numero_os or "") in (str(ordem.id), str(ordem.numero_os or ""))
            or (
                ordem.codigo_veiculo and s.codigo_veiculo and str(s.codigo_veiculo).strip() == str(ordem.codigo_veiculo).strip()
                and ordem.placa and s.placa and str(s.placa).strip().upper() == str(ordem.placa).strip().upper()
            )
        ]
        servicos_por_ordem[ordem.id] = relacionados

    filtros["_servicos_texto"] = {
        ordem_id: " ".join([str(s.codigo_servico or "") for s in lista])
        for ordem_id, lista in servicos_por_ordem.items()
    }

    ordens_filtradas = filtrar_ordens(ordens, filtros)

    total_os = len(ordens_filtradas)
    total_pendentes = len([o for o in ordens_filtradas if (o.status_envio or 'PENDENTE') in ('PENDENTE', 'EM_FILA', 'PROCESSANDO')])
    total_enviadas = len([o for o in ordens_filtradas if (o.status_envio or '') == 'ENVIADA'])
    total_erros = len([o for o in ordens_filtradas if str(o.status_envio or '').startswith('ERRO')])
    total_fotos = len([o for o in ordens_filtradas if fotos_da_ordem(o)])
    fila_ativa = any((o.status_envio or "") in ("EM_FILA", "PROCESSANDO") for o in ordens) or any(
        (s.status_envio or "") in ("EM_FILA", "PROCESSANDO") for s in servicos
    )
    worker_status = "Imediato"

    datalists = {
        "id": render_datalist("id", unique_suggestions([o.id for o in ordens])),
        "numero_os": render_datalist("numero_os", unique_suggestions([o.numero_os for o in ordens])),
        "servicos": render_datalist("servicos", unique_suggestions([s.codigo_servico for s in servicos])),
        "codigo_veiculo": render_datalist("codigo_veiculo", unique_suggestions([o.codigo_veiculo for o in ordens] + [s.codigo_veiculo for s in servicos])),
        "placa": render_datalist("placa", unique_suggestions([o.placa for o in ordens] + [s.placa for s in servicos])),
        "hodometro": render_datalist("hodometro", unique_suggestions([o.hodometro for o in ordens])),
        "data_hora_abertura": render_datalist("data_hora_abertura", unique_suggestions([o.data_hora_abertura for o in ordens])),
        "data_hora_saida": render_datalist("data_hora_saida", unique_suggestions([o.data_hora_saida for o in ordens])),
        "codigo_filial": render_datalist("codigo_filial", unique_suggestions([o.codigo_filial for o in ordens])),
        "codigo_departamento": render_datalist("codigo_departamento", unique_suggestions([o.codigo_departamento for o in ordens])),
        "descricao_defeito": render_datalist("descricao_defeito", unique_suggestions([o.descricao_defeito for o in ordens], limit=40)),
        "created_at": render_datalist("created_at", unique_suggestions([(o.created_at.strftime('%d/%m/%Y %H:%M') if o.created_at else "") for o in ordens])),
    }

    linhas_os = ""
    for o in ordens_filtradas:
        st = o.status_envio or "PENDENTE"
        retorno_html = retorno_resumido_html(o.retorno_envio or "", f"/painel/detalhes/os/{o.id}", success=st == "ENVIADA")

        serv_rel = servicos_por_ordem.get(o.id, [])
        total_serv = len(serv_rel)
        enviados_serv = len([s for s in serv_rel if s.status_envio == "ENVIADA"])
        erros_serv = len([s for s in serv_rel if str(s.status_envio or "").startswith("ERRO")])
        pend_serv = max(total_serv - enviados_serv - erros_serv, 0)

        servico_resumo = f"{enviados_serv}/{total_serv} enviados"
        if erros_serv:
            servico_resumo += f" | {erros_serv} erro(s)"
        if pend_serv:
            servico_resumo += f" | {pend_serv} pendente(s)"

        acoes = f"""
        <div class="acoes-coluna">
          <a class="btn btn-blue" href="/painel/ordens-servico/editar/{o.id}">Editar</a>
          <form method="post" action="/painel/ordens-servico/deletar/{o.id}" onsubmit="return confirm('Deletar a O.S. {o.id}? Esta ação remove a O.S. do painel.');">
            <button class="btn-red" type="submit">Deletar</button>
          </form>
        """
        if st == "ENVIADA":
            acoes += "<span class='status-enviada'>O.S. enviada</span>"
        elif st in ("EM_FILA", "PROCESSANDO"):
            acoes += "<span class='status-pendente'>Enviando</span>"
        else:
            acoes += f"""
            <form method="post" action="/painel/ordens-servico/enviar/{o.id}" onsubmit="return confirm('Enviar a O.S. {o.id} agora ao FrotaWeb?')">
              <button class="btn-green" type="submit">Enviar agora</button>
            </form>
            """
        acoes += "</div>"

        linhas_os += f"""
        <tr>
          <td class="check-cell"><input type="checkbox" class="ordem-checkbox" value="{o.id}"></td>
          <td><strong>#{o.id}</strong></td>
          <td><span class="{status_class(st)}">{st}</span></td>
          <td>{escape_html(o.numero_os or "—")}</td>
          <td>{escape_html(servico_resumo)}</td>
          <td>{escape_html(o.codigo_veiculo or "")}</td>
          <td>{escape_html(o.placa or "")}</td>
          <td>{escape_html(o.hodometro or "")}</td>
          <td>{escape_html(o.data_hora_abertura or "")}</td>
          <td>{escape_html(o.data_hora_saida or "")}</td>
          <td>{escape_html(o.codigo_filial or "")}</td>
          <td>{escape_html(o.codigo_departamento or "")}</td>
          <td>{escape_html(o.descricao_defeito or "")}</td>
          <td>{html_fotos_ordem(o)}</td>
          <td class="retorno">{retorno_html}</td>
          <td>{o.created_at.strftime('%d/%m/%Y %H:%M') if o.created_at else ''}</td>
          <td>{acoes}</td>
        </tr>
        """

    servicos_agrupados: dict[str, list[Any]] = {}
    for s in servicos:
        chave = str(s.numero_os or "").strip() or "SEM_OS"
        servicos_agrupados.setdefault(chave, []).append(s)

    grupos_servicos_html = ""
    for numero_os, lista in sorted(
        servicos_agrupados.items(),
        key=lambda item: (
            item[0] == "SEM_OS",
            item[0],
        ),
    ):
        primeira = lista[0]
        enviados = sum(1 for item in lista if (item.status_envio or "") == "ENVIADA")
        erros = sum(1 for item in lista if str(item.status_envio or "").startswith("ERRO"))
        pendentes = len(lista) - enviados - erros
        lista_html = ""
        for s in lista:
            st = s.status_envio or "PENDENTE"
            retorno_html = retorno_resumido_html(s.retorno_envio or "", f"/painel/detalhes/servico/{s.id}", success=st == "ENVIADA")
            if st == "ENVIADA":
                acao = "<span class='status-enviada'>Enviado</span>"
            elif st in ("EM_FILA", "PROCESSANDO"):
                acao = "<span class='status-pendente'>Enviando</span>"
            else:
                acao = f"""
                <form method="post" action="/painel/servicos-os/enviar/{s.id}" onsubmit="return confirm('Enviar o servico {s.id} agora ao FrotaWeb?')">
                  <button class="btn-green" type="submit">Enviar agora</button>
                </form>
                """
            lista_html += f"""
            <div class="service-item">
              <div class="service-item-top">
                <div>
                  <div><strong>Servico {escape_html(s.codigo_servico or '')}</strong> <span class="{status_class(st)}">{st}</span></div>
                  <div class="service-item-meta">ID {s.id} | Veiculo {escape_html(s.codigo_veiculo or '')} | Placa {escape_html(s.placa or '')}</div>
                  <div class="service-item-meta">Recurso {escape_html(s.codigo_recurso_humano or '')} | Tempo {escape_html(s.tempo_gasto or '')} | Valor hora {escape_html(s.valor_hora or '')}</div>
                </div>
                <input type="checkbox" class="servico-checkbox" value="{s.id}">
              </div>
              <div style="margin-top:10px;">{retorno_html}</div>
              <div class="service-item-actions">
                {acao}
                <a class="btn btn-blue" href="/painel/detalhes/servico/{s.id}" target="_blank">Ver detalhes</a>
              </div>
            </div>
            """
        grupos_servicos_html += f"""
        <div class="service-group-card">
          <div class="service-group-header">
            <div>
              <div class="service-group-title">O.S. {escape_html(numero_os if numero_os != 'SEM_OS' else 'Sem vinculacao')}</div>
              <div class="service-group-meta">Veiculo {escape_html(primeira.codigo_veiculo or '')} | Placa {escape_html(primeira.placa or '')}</div>
            </div>
            <div class="service-pill">{enviados}/{len(lista)} enviados | {erros} erro(s) | {pendentes} pendente(s)</div>
          </div>
          <div class="service-list">{lista_html}</div>
        </div>
        """

    def fv(nome: str) -> str:
        return escape_html(filtros.get(nome, "") or "")

    corpo = f"""
    <div class="kpis">
      <div class="kpi"><div class="label">Ordens visiveis</div><div class="value">{total_os}</div></div>
      <div class="kpi"><div class="label">Pendentes</div><div class="value">{total_pendentes}</div></div>
      <div class="kpi"><div class="label">Enviadas</div><div class="value">{total_enviadas}</div></div>
      <div class="kpi"><div class="label">Com erro</div><div class="value">{total_erros}</div></div>
      <div class="kpi"><div class="label">Com foto</div><div class="value">{total_fotos}</div></div>
      <div class="kpi"><div class="label">Modo de envio</div><div class="value" style="font-size:22px;">{worker_status}</div></div>
    </div>
    {f'<div class="card"><strong>Enviando.</strong> O painel esta processando um envio agora.</div>' if fila_ativa else ''}
    <div class="card">
      <form method="get" action="/painel/ordens-servico">
        <div class="hint" style="margin-bottom:12px;">Digite parte do valor e escolha uma sugestão da própria base do painel.</div>
        <div class="filter-grid">
          <input name="id" list="lista-id" placeholder="Filtrar ID Painel" value="{fv('id')}">
          <select name="status_envio">
            <option value="" {"selected" if not filtros.get("status_envio") else ""}>Status: Todos</option>
            <option value="PENDENTE" {"selected" if filtros.get("status_envio") == "PENDENTE" else ""}>Pendente</option>
            <option value="EM_FILA" {"selected" if filtros.get("status_envio") == "EM_FILA" else ""}>Em fila</option>
            <option value="PROCESSANDO" {"selected" if filtros.get("status_envio") == "PROCESSANDO" else ""}>Processando</option>
            <option value="ENVIADA" {"selected" if filtros.get("status_envio") == "ENVIADA" else ""}>Enviada</option>
            <option value="ERRO_ENVIO" {"selected" if filtros.get("status_envio") == "ERRO_ENVIO" else ""}>Erro</option>
          </select>
          <input name="numero_os" list="lista-numero_os" placeholder="Filtrar Nº O.S. FrotaWeb" value="{fv('numero_os')}">
          <input name="servicos" list="lista-servicos" placeholder="Filtrar Serviços" value="{fv('servicos')}">
          <input name="codigo_veiculo" list="lista-codigo_veiculo" placeholder="Filtrar Veículo" value="{fv('codigo_veiculo')}">
          <input name="placa" list="lista-placa" placeholder="Filtrar Placa" value="{fv('placa')}">
          <input name="hodometro" list="lista-hodometro" placeholder="Filtrar KM" value="{fv('hodometro')}">
          <input name="data_hora_abertura" list="lista-data_hora_abertura" placeholder="Filtrar Abertura/Data" value="{fv('data_hora_abertura')}">
          <input name="data_hora_saida" list="lista-data_hora_saida" placeholder="Filtrar Saída" value="{fv('data_hora_saida')}">
          <input name="codigo_filial" list="lista-codigo_filial" placeholder="Filtrar Filial" value="{fv('codigo_filial')}">
          <input name="codigo_departamento" list="lista-codigo_departamento" placeholder="Filtrar Departamento" value="{fv('codigo_departamento')}">
          <input name="descricao_defeito" list="lista-descricao_defeito" placeholder="Filtrar Defeito" value="{fv('descricao_defeito')}">
          <input name="retorno_envio" placeholder="Buscar no retorno" value="{fv('retorno_envio')}">
          <input name="created_at" list="lista-created_at" placeholder="Filtrar Criado em" value="{fv('created_at')}">
          <select name="fotos">
            <option value="" {"selected" if not filtros.get("fotos") else ""}>Fotos: Todos</option>
            <option value="com_foto" {"selected" if filtros.get("fotos") == "com_foto" else ""}>Somente com foto</option>
          </select>
        </div>
        {''.join(datalists.values())}
        <div class="acoes">
          <button type="submit">Filtrar</button>
          <a class="btn btn-gray" href="/painel/ordens-servico">Limpar</a>
          <button type="submit" class="btn-red" formmethod="post" formaction="/painel/ordens-servico/limpar-cache" onclick="return confirm('Limpar toda a fila/cache de O.S. e serviços do painel?');">Limpar cache</button>
          <a class="btn btn-gray" href="/docs">Docs</a>
        </div>
      </form>
    </div>
    <div class="tabs">
      <button type="button" class="btn tab-button active" data-tab-target="ordens" onclick="switchTab('ordens')">Ordens de Servico</button>
      <button type="button" class="btn tab-button" data-tab-target="servicos" onclick="switchTab('servicos')">Servicos por O.S.</button>
    </div>
    <div class="tab-panel active" data-tab-panel="ordens">
      <div class="card" style="padding:0;">
        <div style="padding:18px 18px 8px;">
          <div class="toolbar">
            <div>
              <h2 style="margin:0;">Ordens de Servico</h2>
              <div class="subtle">Acompanhe as O.S. principais e deixe os servicos em uma consulta separada, mais limpa para o dia a dia.</div>
            </div>
            <div class="acoes">
              <form id="bulk-queue-os-form" method="post" action="/painel/ordens-servico/enviar-lote">
                <input type="hidden" id="bulk-queue-os-ids" name="ids">
                <button type="button" class="btn-green" onclick="submitSelected('bulk-queue-os-form', '.ordem-checkbox', 'bulk-queue-os-ids', 'Selecione ao menos uma O.S. para enviar.', 'Enviar {{count}} O.S. selecionada(s)?')">Enviar selecionadas</button>
              </form>
              <form id="bulk-delete-os-form" method="post" action="/painel/ordens-servico/deletar-lote">
                <input type="hidden" id="bulk-delete-os-ids" name="ids">
                <button type="button" class="btn-red" onclick="submitSelected('bulk-delete-os-form', '.ordem-checkbox', 'bulk-delete-os-ids', 'Selecione ao menos uma O.S. para excluir.', 'Excluir {{count}} O.S. selecionada(s)?')">Excluir selecionadas</button>
              </form>
            </div>
          </div>
        </div>
        <div class="table-wrap">
          <table><thead><tr><th class="check-cell"><input type="checkbox" onclick="toggleGroup('.ordem-checkbox', this.checked)"></th><th>ID Painel</th><th>Status</th><th>N? O.S. FrotaWeb</th><th>Servicos</th><th>Veiculo</th><th>Placa</th><th>KM</th><th>Abertura</th><th>Saida</th><th>Filial</th><th>Departamento</th><th>Defeito</th><th>Fotos</th><th>Retorno</th><th>Criado em</th><th>Acao</th></tr></thead>
          <tbody>{linhas_os if linhas_os else '<tr><td colspan="17">Nenhuma O.S. encontrada</td></tr>'}</tbody></table>
        </div>
      </div>
    </div>
    <div class="tab-panel" data-tab-panel="servicos">
      <div class="card">
        <div class="toolbar">
          <div>
            <h2 style="margin:0;">Servicos por O.S.</h2>
            <div class="subtle">Cada card agrupa os servicos enviados para a mesma O.S., facilitando a conferencia sem deixar a tela principal poluida.</div>
          </div>
          <div class="acoes">
            <form id="bulk-queue-serv-form" method="post" action="/painel/servicos-os/enviar-lote">
              <input type="hidden" id="bulk-queue-serv-ids" name="ids">
              <button type="button" class="btn-green" onclick="submitSelected('bulk-queue-serv-form', '.servico-checkbox', 'bulk-queue-serv-ids', 'Selecione ao menos um servico para enviar.', 'Enviar {{count}} servico(s) selecionado(s)?')">Enviar selecionados</button>
            </form>
            <form id="bulk-delete-serv-form" method="post" action="/painel/servicos-os/deletar-lote">
              <input type="hidden" id="bulk-delete-serv-ids" name="ids">
              <button type="button" class="btn-red" onclick="submitSelected('bulk-delete-serv-form', '.servico-checkbox', 'bulk-delete-serv-ids', 'Selecione ao menos um servico para excluir.', 'Excluir {{count}} servico(s) selecionado(s)?')">Excluir selecionados</button>
            </form>
          </div>
        </div>
      </div>
      <div class="service-groups">
        {grupos_servicos_html if grupos_servicos_html else '<div class="card">Nenhum servico encontrado.</div>'}
      </div>
    </div>
    """
    return html_base("Painel O.S. Corretiva", corpo, msg, auto_refresh_seconds=QUEUE_POLL_SECONDS if fila_ativa else 0)


# =========================================================
# ROTAS
# =========================================================

@app.get("/")
def home():
    return {"status": "online", "painel": "/painel/ordens-servico", "receber_os": "POST /panel/os", "receber_servico": "POST /panel/os/servicos", "docs": "/docs"}


@app.get("/health")
def health():
    db = SessionLocal()
    try:
        ordens_em_fila = db.query(OrdemServico).filter(OrdemServico.deleted_at.is_(None)).filter(OrdemServico.status_envio.in_(("EM_FILA", "PROCESSANDO"))).count()
        servicos_em_fila = db.query(ServicoOS).filter(ServicoOS.deleted_at.is_(None)).filter(ServicoOS.status_envio.in_(("EM_FILA", "PROCESSANDO"))).count()
        return {
            "status": "ok",
            "worker_alive": bool(worker_thread and worker_thread.is_alive()),
            "queue_poll_seconds": QUEUE_POLL_SECONDS,
            "ordens_em_fila": ordens_em_fila,
            "servicos_em_fila": servicos_em_fila,
        }
    finally:
        db.close()


@app.post("/painel/processar-fila-agora")
def processar_fila_agora():
    try:
        processou = processar_proximo_item_da_fila()
        mensagem = "Fila processada agora." if processou else "Nao havia item em fila para processar."
    except Exception as exc:
        logger.exception("Falha ao processar fila manualmente")
        mensagem = f"Falha ao processar fila: {exc}"
    return RedirectResponse(url=f"/painel/ordens-servico?msg={requests.utils.quote(mensagem)}", status_code=303)


@app.post("/panel/os")
@app.post("/ordens-servico")
async def receber_os(request: Request, db: Session = Depends(get_db)):
    try:
        dados = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON invalido")
    try:
        p = montar_payload_os(dados)
        dados_limpos = limpar_payload_para_painel(mascarar_senha(dados))
        if not p["codigo_veiculo"] and not p["placa"]:
            raise HTTPException(status_code=400, detail="Informe codigo_veiculo ou placa")

        c = p["credenciais"]
        validar_credenciais_painel(c, origem="app/O.S.")
        fotos_recebidas = salvar_fotos_app(dados)
        ordem = OrdemServico(
            empresa=c["empresa"], usuario=c["usuario"], senha=c["senha"], filial=c["filial"], recurso_humano=c["recurso_humano"],
            codigo_veiculo=p["codigo_veiculo"], descricao_defeito=p["descricao_defeito"], numero_os=p["numero_os"], placa=p["placa"],
            codigo_componente=p["codigo_componente"], data_abertura=p["data_abertura"], data_hora_abertura=p["data_hora_abertura"],
            hodometro=p["hodometro"], horimetro_entrada=p["horimetro_entrada"], data_hora_saida=p["data_hora_saida"],
            hodometro_saida=p["hodometro_saida"], horimetro_saida=p["horimetro_saida"], data_hora_inicio=p["data_hora_inicio"],
            data_hora_previsao_liberacao=p["data_hora_previsao_liberacao"], horas_previstas=p["horas_previstas"], horas_realizadas=p["horas_realizadas"],
            codigo_filial=p["codigo_filial"], codigo_departamento=p["codigo_departamento"], codigo_oficina=p["codigo_oficina"], codigo_servico=p["codigo_servico"],
            codigo_solicitante=p["codigo_solicitante"], codigo_motorista=p["codigo_motorista"], numero_ocorrencia=p["numero_ocorrencia"],
            numero_contrato=p["numero_contrato"], valor_acrescimo=p["valor_acrescimo"], numero_os_retorno=p["numero_os_retorno"], observacoes=p["observacoes"],
            investimento=p["investimento"], acidente=p["acidente"], socorro=p["socorro"], servico_retorno=p["servico_retorno"], programada=p["programada"],
            status_envio="PENDENTE",
            retorno_envio="O.S. salva no painel. Aguardando envio manual.",
            payload_original=json_dumps_safe(dados_limpos),
            campos_brutos=json_dumps_safe(p["campos_brutos"]),
            fotos_app=json_dumps_safe(fotos_recebidas),
            app_payload=json_dumps_safe(dados_limpos),
        )
        db.add(ordem)
        db.commit()
        db.refresh(ordem)
        return {
            "ok": True,
            "created": True,
            "id": ordem.id,
            "order_number": str(ordem.id),
            "status_envio": ordem.status_envio,
            "message": "O.S. salva no painel.",
            "fotos_recebidas": len(fotos_recebidas),
            "payload_tryout": payload_os_from_ordem(ordem),
        }
    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        logger.exception("Erro ao salvar O.S. no painel")
        raise HTTPException(status_code=500, detail=f"Erro ao salvar O.S. no painel: {exc}")


@app.post("/panel/os/servicos")
@app.post("/ordens-servico/servicos")
async def receber_servico(request: Request, db: Session = Depends(get_db)):
    try:
        dados = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON invalido")

    try:
        p = montar_payload_servico(dados)
        dados_limpos = limpar_payload_para_painel(mascarar_senha(dados))
        if not p["numero_os"]:
            raise HTTPException(status_code=400, detail="Informe numero_os")
        if not p["codigo_servico"]:
            raise HTTPException(status_code=400, detail="Informe codigo_servico")

        c = p["credenciais"]
        validar_credenciais_painel(c, origem="app/servico")
        serv = ServicoOS(
            empresa=c["empresa"], usuario=c["usuario"], senha=c["senha"], filial=c["filial"], recurso_humano=c["recurso_humano"],
            numero_os=p["numero_os"], codigo_veiculo=p["codigo_veiculo"], placa=p["placa"], codigo_servico=p["codigo_servico"],
            codigo_recurso_humano=p["codigo_recurso_humano"], tempo_gasto=p["tempo_gasto"], valor_hora=p["valor_hora"],
            status_envio="PENDENTE",
            retorno_envio="Servi?o salvo no painel. Aguardando envio manual.",
            payload_original=json_dumps_safe(dados_limpos),
            campos_brutos=json_dumps_safe(p["campos_brutos"]),
        )
        db.add(serv)
        db.commit()
        db.refresh(serv)
        return {"ok": True, "created": True, "id": serv.id, "status_envio": serv.status_envio, "message": "Servi?o salvo no painel.", "payload_tryout": payload_servico_from_model(serv)}
    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        logger.exception("Erro ao salvar servi?o no painel")
        raise HTTPException(status_code=500, detail=f"Erro ao salvar servi?o no painel: {exc}")


@app.get("/ordens-servico")
def listar_ordens(status_envio: Optional[str] = Query(None), db: Session = Depends(get_db)):
    q = db.query(OrdemServico).filter(OrdemServico.deleted_at.is_(None))
    if status_envio:
        q = q.filter(OrdemServico.status_envio == status_envio)
    return [{"id": o.id, "status_envio": o.status_envio, "payload": payload_os_from_ordem(o), "retorno_envio": o.retorno_envio} for o in q.order_by(OrdemServico.created_at.desc()).all()]


@app.get("/ordens-servico/servicos")
def listar_servicos(status_envio: Optional[str] = Query(None), db: Session = Depends(get_db)):
    q = db.query(ServicoOS).filter(ServicoOS.deleted_at.is_(None))
    if status_envio:
        q = q.filter(ServicoOS.status_envio == status_envio)
    return [{"id": s.id, "status_envio": s.status_envio, "payload": payload_servico_from_model(s), "retorno_envio": s.retorno_envio} for s in q.order_by(ServicoOS.created_at.desc()).all()]


@app.get("/painel/ordens-servico", response_class=HTMLResponse)
def painel(
    id: Optional[str] = Query(None),
    status_envio: Optional[str] = Query(None),
    numero_os: Optional[str] = Query(None),
    servicos: Optional[str] = Query(None),
    codigo_veiculo: Optional[str] = Query(None),
    placa: Optional[str] = Query(None),
    hodometro: Optional[str] = Query(None),
    data_hora_abertura: Optional[str] = Query(None),
    data_hora_saida: Optional[str] = Query(None),
    codigo_filial: Optional[str] = Query(None),
    codigo_departamento: Optional[str] = Query(None),
    descricao_defeito: Optional[str] = Query(None),
    retorno_envio: Optional[str] = Query(None),
    created_at: Optional[str] = Query(None),
    fotos: Optional[str] = Query(None),
    msg: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    qo = db.query(OrdemServico).filter(OrdemServico.deleted_at.is_(None))
    qs = db.query(ServicoOS).filter(ServicoOS.deleted_at.is_(None))
    ordens = qo.order_by(OrdemServico.created_at.desc()).all()
    servicos_lista = qs.order_by(ServicoOS.created_at.desc()).all()

    filtros = {
        "id": id or "",
        "status_envio": status_envio or "",
        "numero_os": numero_os or "",
        "servicos": servicos or "",
        "codigo_veiculo": codigo_veiculo or "",
        "placa": placa or "",
        "hodometro": hodometro or "",
        "data_hora_abertura": data_hora_abertura or "",
        "data_hora_saida": data_hora_saida or "",
        "codigo_filial": codigo_filial or "",
        "codigo_departamento": codigo_departamento or "",
        "descricao_defeito": descricao_defeito or "",
        "retorno_envio": retorno_envio or "",
        "created_at": created_at or "",
        "fotos": fotos or "",
    }
    return HTMLResponse(render_painel(ordens, servicos_lista, filtros, msg or ""))



@app.get("/painel/ordens-servico/editar/{ordem_id}", response_class=HTMLResponse)
def editar_ordem(ordem_id: int, db: Session = Depends(get_db)):
    ordem = db.query(OrdemServico).filter(OrdemServico.id == ordem_id).filter(OrdemServico.deleted_at.is_(None)).first()
    if not ordem:
        raise HTTPException(status_code=404, detail="O.S. nao encontrada")

    campos_html = ""
    for field_name, label, field_type in ordem_edit_fields():
        campos_html += render_edit_form_field(ordem, field_name, label, field_type)

    checks_html = ""
    checks_html += render_checkbox("investimento", "Investimento", bool(ordem.investimento))
    checks_html += render_checkbox("acidente", "Acidente", bool(ordem.acidente))
    checks_html += render_checkbox("socorro", "Socorro", bool(ordem.socorro))
    checks_html += render_checkbox("servico_retorno", "Serviço retorno", bool(ordem.servico_retorno))
    checks_html += render_checkbox("programada", "Programada", bool(ordem.programada))

    payload_original = json.dumps(payload_original_dict(ordem), ensure_ascii=False, indent=2)

    corpo = f"""
    <div class="card">
      <h2>Fotos recebidas do aplicativo</h2>
      {html_fotos_ordem(ordem)}
    </div>

    <form method="post" action="/painel/ordens-servico/editar/{ordem.id}">
      <div class="card">
        <h2>Descrição e campos da O.S.</h2>
        <p class="small">Edite aqui todos os campos recebidos do aplicativo antes de enviar ao FrotaWeb.</p>
        <div class="grid">{campos_html}</div>
        <h3>Marcadores</h3>
        <div class="acoes">{checks_html}</div>
      </div>

      <div class="card">
        <h2>JSON original recebido do app</h2>
        <p class="small">Campo de conferência. Pode ser editado, mas os campos acima são os usados para enviar ao FrotaWeb.</p>
        <textarea name="app_payload" style="min-height:260px;font-family:monospace;">{escape_html(payload_original)}</textarea>
      </div>

      <div class="acoes">
        <button type="submit" class="btn-green">Salvar alterações</button>
        <a href="/painel/ordens-servico" class="btn btn-gray">Voltar</a>
      </div>
    </form>

    <div class="card">
      <form method="post" action="/painel/ordens-servico/deletar/{ordem.id}" onsubmit="return confirm('Deletar a O.S. {ordem.id}?');">
        <button type="submit" class="btn-red">Deletar O.S.</button>
      </form>
    </div>
    """
    return HTMLResponse(html_base(f"Editar O.S. {ordem.id}", corpo))


@app.post("/painel/ordens-servico/editar/{ordem_id}")
async def salvar_edicao_ordem(ordem_id: int, request: Request, db: Session = Depends(get_db)):
    ordem = db.query(OrdemServico).filter(OrdemServico.id == ordem_id).filter(OrdemServico.deleted_at.is_(None)).first()
    if not ordem:
        raise HTTPException(status_code=404, detail="O.S. nao encontrada")

    form = await request.form()

    for field_name, _, _ in ordem_edit_fields():
        if field_name in form:
            setattr(ordem, field_name, str(form.get(field_name) or "").strip())

    ordem.investimento = form.get("investimento") == "true"
    ordem.acidente = form.get("acidente") == "true"
    ordem.socorro = form.get("socorro") == "true"
    ordem.servico_retorno = form.get("servico_retorno") == "true"
    ordem.programada = form.get("programada") == "true"

    app_payload_text = str(form.get("app_payload") or "").strip()
    if app_payload_text:
        try:
            ordem.app_payload = json_dumps_safe(json.loads(app_payload_text))
        except Exception:
            ordem.app_payload = app_payload_text

    ordem.retorno_envio = "O.S. editada no painel. Aguardando envio manual."
    ordem.status_envio = "PENDENTE"
    db.commit()

    return RedirectResponse(url=f"/painel/ordens-servico/editar/{ordem.id}", status_code=303)


@app.post("/painel/ordens-servico/deletar/{ordem_id}")
def deletar_ordem(ordem_id: int, db: Session = Depends(get_db)):
    ordem = db.query(OrdemServico).filter(OrdemServico.id == ordem_id).filter(OrdemServico.deleted_at.is_(None)).first()
    if not ordem:
        raise HTTPException(status_code=404, detail="O.S. nao encontrada")

    ordem.deleted_at = func.now()

    servicos = db.query(ServicoOS).filter(
        (ServicoOS.numero_os == str(ordem.id)) | (ServicoOS.numero_os == str(ordem.numero_os or ""))
    ).all()
    for serv in servicos:
        serv.deleted_at = func.now()

    db.commit()
    return RedirectResponse(url="/painel/ordens-servico?msg=O.S.%20deletada%20com%20sucesso", status_code=303)


def parse_ids_lote(ids_texto: str) -> list[int]:
    ids: list[int] = []
    for parte in str(ids_texto or "").split(","):
        parte = parte.strip()
        if not parte:
            continue
        try:
            ids.append(int(parte))
        except ValueError:
            continue
    return ids


@app.post("/painel/ordens-servico/deletar-lote")
async def deletar_ordens_lote(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    ids = parse_ids_lote(form.get("ids"))
    if not ids:
        return RedirectResponse(url="/painel/ordens-servico?msg=Nenhuma%20O.S.%20selecionada%20para%20exclusao", status_code=303)

    ordens = db.query(OrdemServico).filter(OrdemServico.id.in_(ids)).filter(OrdemServico.deleted_at.is_(None)).all()
    ordem_ids_str = {str(o.id) for o in ordens}
    numeros_os = {str(o.numero_os or "") for o in ordens if o.numero_os}
    for ordem in ordens:
        ordem.deleted_at = func.now()

    servicos = db.query(ServicoOS).filter(ServicoOS.deleted_at.is_(None)).all()
    for serv in servicos:
        if str(serv.numero_os or "") in ordem_ids_str or str(serv.numero_os or "") in numeros_os:
            serv.deleted_at = func.now()

    db.commit()
    return RedirectResponse(url=f"/painel/ordens-servico?msg={len(ordens)}%20O.S.%20excluida(s)%20em%20lote", status_code=303)


@app.post("/painel/ordens-servico/enviar-lote")
async def enfileirar_ordens_lote(
    request: Request,
    db: Session = Depends(get_db),
):
    form = await request.form()
    ids = parse_ids_lote(form.get("ids"))
    if not ids:
        return RedirectResponse(url="/painel/ordens-servico?msg=Nenhuma%20O.S.%20selecionada%20para%20enfileirar", status_code=303)

    ordens = db.query(OrdemServico).filter(OrdemServico.id.in_(ids)).filter(OrdemServico.deleted_at.is_(None)).all()
    total = 0
    sucesso = 0
    erro = 0
    ordem_ids = []
    for ordem in ordens:
        if ordem.status_envio == "ENVIADA":
            continue
        ordem.status_envio = "PROCESSANDO"
        ordem.retorno_envio = "Enviando O.S. ao FrotaWeb..."
        ordem_ids.append(ordem.id)
        total += 1
    db.commit()

    for ordem_id in ordem_ids:
        processar_ordem_em_fila(ordem_id)

    for ordem_id in ordem_ids:
        atual = db.query(OrdemServico).filter(OrdemServico.id == ordem_id).first()
        if atual and atual.status_envio == "ENVIADA":
            sucesso += 1
        else:
            erro += 1

    mensagem = f"{sucesso} O.S. enviada(s) com sucesso"
    if erro:
        mensagem += f" e {erro} com erro"
    if total == 0:
        mensagem = "Nenhuma O.S. elegivel para envio"
    return RedirectResponse(url=f"/painel/ordens-servico?msg={requests.utils.quote(mensagem)}", status_code=303)


@app.post("/painel/servicos-os/deletar-lote")
async def deletar_servicos_lote(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    ids = parse_ids_lote(form.get("ids"))
    if not ids:
        return RedirectResponse(url="/painel/ordens-servico?msg=Nenhum%20servico%20selecionado%20para%20exclusao", status_code=303)

    servicos = db.query(ServicoOS).filter(ServicoOS.id.in_(ids)).filter(ServicoOS.deleted_at.is_(None)).all()
    for serv in servicos:
        serv.deleted_at = func.now()

    db.commit()
    return RedirectResponse(url=f"/painel/ordens-servico?msg={len(servicos)}%20servico(s)%20excluido(s)%20em%20lote", status_code=303)


@app.post("/painel/servicos-os/enviar-lote")
async def enfileirar_servicos_lote(
    request: Request,
    db: Session = Depends(get_db),
):
    form = await request.form()
    ids = parse_ids_lote(form.get("ids"))
    if not ids:
        return RedirectResponse(url="/painel/ordens-servico?msg=Nenhum%20servico%20selecionado%20para%20enfileirar", status_code=303)

    servicos = db.query(ServicoOS).filter(ServicoOS.id.in_(ids)).filter(ServicoOS.deleted_at.is_(None)).all()
    total = 0
    sucesso = 0
    erro = 0
    servico_ids = []
    for serv in servicos:
        if serv.status_envio == "ENVIADA":
            continue
        serv.status_envio = "PROCESSANDO"
        serv.retorno_envio = "Enviando servico ao FrotaWeb..."
        servico_ids.append(serv.id)
        total += 1
    db.commit()

    for servico_id in servico_ids:
        processar_servico_em_fila(servico_id)

    for servico_id in servico_ids:
        atual = db.query(ServicoOS).filter(ServicoOS.id == servico_id).first()
        if atual and atual.status_envio == "ENVIADA":
            sucesso += 1
        else:
            erro += 1

    mensagem = f"{sucesso} servico(s) enviado(s) com sucesso"
    if erro:
        mensagem += f" e {erro} com erro"
    if total == 0:
        mensagem = "Nenhum servico elegivel para envio"
    return RedirectResponse(url=f"/painel/ordens-servico?msg={requests.utils.quote(mensagem)}", status_code=303)


@app.post("/painel/ordens-servico/limpar-cache")
def limpar_cache_ordens(db: Session = Depends(get_db)):
    db.query(ServicoOS).filter(ServicoOS.deleted_at.is_(None)).update(
        {ServicoOS.deleted_at: func.now()},
        synchronize_session=False,
    )
    db.query(OrdemServico).filter(OrdemServico.deleted_at.is_(None)).update(
        {OrdemServico.deleted_at: func.now()},
        synchronize_session=False,
    )
    db.commit()
    return RedirectResponse(url="/painel/ordens-servico?msg=Cache%20do%20painel%20limpo%20com%20sucesso", status_code=303)


@app.get("/painel/detalhes/{tipo}/{registro_id}", response_class=HTMLResponse)
def ver_detalhes_retorno(tipo: str, registro_id: int, db: Session = Depends(get_db)):
    if tipo == "os":
        registro = db.query(OrdemServico).filter(OrdemServico.id == registro_id).first()
        if not registro:
            raise HTTPException(status_code=404, detail="O.S. nao encontrada")
        titulo = f"Detalhes da O.S. {registro.id}"
        retorno = registro.retorno_envio or ""
        payload_original = mascarar_senha(json_loads_safe(registro.payload_original or "", registro.payload_original))
        payload_app = mascarar_senha(json_loads_safe(registro.app_payload or "", registro.app_payload))
        tentativas = tentativas_do_registro(db, "OS", registro.id)
    elif tipo == "servico":
        registro = db.query(ServicoOS).filter(ServicoOS.id == registro_id).first()
        if not registro:
            raise HTTPException(status_code=404, detail="Servico nao encontrado")
        titulo = f"Detalhes do Servico {registro.id}"
        retorno = registro.retorno_envio or ""
        payload_original = mascarar_senha(json_loads_safe(registro.payload_original or "", registro.payload_original))
        payload_app = payload_original
        tentativas = tentativas_do_registro(db, "SERVICO", registro.id)
    else:
        raise HTTPException(status_code=404, detail="Tipo de detalhe invalido")

    historico = ""
    if tentativas:
        blocos = []
        for tentativa in tentativas:
            blocos.append(
                f"<div class='card'>"
                f"<strong>{escape_html(tentativa.status_execucao or '')}</strong> "
                f"<span class='small'>HTTP {escape_html(tentativa.status_http or '-')} | "
                f"{tentativa.created_at.strftime('%d/%m/%Y %H:%M:%S') if tentativa.created_at else ''}</span>"
                f"<div style='margin-top:8px'>{escape_html(tentativa.mensagem or '')}</div>"
                f"<details style='margin-top:8px'><summary>Diagnostico</summary><pre>{escape_html(tentativa.diagnostico or '')}</pre></details>"
                f"</div>"
            )
        historico = "<div class='card'><h3 style='margin-top:0;'>Historico de tentativas</h3>" + "".join(blocos) + "</div>"

    corpo = f"""
    <div class="card">
      <h2 style="margin-top:0;">{escape_html(titulo)}</h2>
      <p class="subtle">Mensagem resumida do FrotaWeb</p>
      <div class="retorno-bloco {'retorno-ok' if 'ERRO' not in str(retorno) else 'retorno-erro'}">
        <div class="retorno-resumo">{escape_html(mensagem_frotaweb(retorno, 'Sem retorno'))}</div>
      </div>
    </div>
    <div class="card">
      <h3 style="margin-top:0;">Retorno completo salvo no painel</h3>
      <pre>{escape_html(json_dumps_safe(mascarar_senha(json_loads_safe(retorno, retorno))))}</pre>
    </div>
    <div class="card">
      <h3 style="margin-top:0;">Payload original</h3>
      <pre>{escape_html(json_dumps_safe(payload_original))}</pre>
    </div>
    <div class="card">
      <h3 style="margin-top:0;">Payload do app</h3>
      <pre>{escape_html(json_dumps_safe(payload_app))}</pre>
    </div>
    {historico}
    <div class="acoes">
      <a class="btn btn-gray" href="/painel/ordens-servico" target="_self">Voltar ao painel</a>
    </div>
    """
    return HTMLResponse(html_base(titulo, corpo))


@app.post("/painel/ordens-servico/enviar/{ordem_id}")
def enviar_os(ordem_id: int, db: Session = Depends(get_db)):
    ordem = db.query(OrdemServico).filter(OrdemServico.id == ordem_id).filter(OrdemServico.deleted_at.is_(None)).first()
    if not ordem:
        raise HTTPException(status_code=404, detail="O.S. nao encontrada")

    ordem.status_envio = "PROCESSANDO"
    ordem.retorno_envio = "Enviando O.S. ao FrotaWeb..."
    db.commit()
    processar_ordem_em_fila(ordem.id)
    db.refresh(ordem)
    msg = resumo_resultado_envio(ordem.status_envio or "", ordem.retorno_envio or "", f"O.S. {ordem.id} enviada.", f"O.S. {ordem.id} com erro.")
    return RedirectResponse(url=f"/painel/ordens-servico?msg={requests.utils.quote(msg)}", status_code=303)


@app.post("/painel/servicos-os/enviar/{servico_id}")
def enviar_servico(servico_id: int, db: Session = Depends(get_db)):
    serv = db.query(ServicoOS).filter(ServicoOS.id == servico_id).filter(ServicoOS.deleted_at.is_(None)).first()
    if not serv:
        raise HTTPException(status_code=404, detail="Servico nao encontrado")

    serv.status_envio = "PROCESSANDO"
    serv.retorno_envio = "Enviando servico ao FrotaWeb..."
    db.commit()
    processar_servico_em_fila(serv.id)
    db.refresh(serv)
    msg = resumo_resultado_envio(serv.status_envio or "", serv.retorno_envio or "", f"Servico {serv.id} enviado.", f"Servico {serv.id} com erro.")
    return RedirectResponse(url=f"/painel/ordens-servico?msg={requests.utils.quote(msg)}", status_code=303)


@app.post("/ordens-servico/{ordem_id}/enviar-frotaweb")
def enviar_os_json(ordem_id: int, db: Session = Depends(get_db)):
    ordem = db.query(OrdemServico).filter(OrdemServico.id == ordem_id).filter(OrdemServico.deleted_at.is_(None)).first()
    if not ordem:
        raise HTTPException(status_code=404, detail="O.S. nao encontrada")
    payload = payload_os_from_ordem(ordem)
    retorno = post_json(FROTAWEB_OS_URL, payload)
    if not resposta_criada_com_sucesso(retorno):
        ordem.status_envio = "ERRO_ENVIO"
        ordem.retorno_envio = mensagem_frotaweb(retorno)
        db.commit()
        raise HTTPException(status_code=502, detail=mensagem_frotaweb(retorno))

    numero_os_frotaweb = extrair_numero_os(retorno)
    if numero_os_frotaweb:
        ordem.numero_os = numero_os_frotaweb

    resumo_servicos = enviar_servicos_relacionados(db, ordem, numero_os_frotaweb or str(ordem.numero_os or ordem.id))

    ordem.status_envio = "ENVIADA"
    ordem.retorno_envio = (
        f"O.S. enviada com sucesso. Nº O.S. FrotaWeb: {numero_os_frotaweb or ordem.numero_os or 'não identificado'}. "
        f"Serviços: {resumo_servicos['enviados']}/{resumo_servicos['total']} enviados, "
        f"{resumo_servicos['erros']} erro(s), {resumo_servicos['pendentes']} pendente(s). "
        f"{mensagem_frotaweb(retorno, 'O.S. enviada com sucesso.')}"
    )
    db.commit()
    return {
        "ok": True,
        "numero_os_frotaweb": numero_os_frotaweb or ordem.numero_os,
        "servicos": resumo_servicos,
        "payload_enviado": payload,
        "retorno": retorno
    }


@app.post("/ordens-servico/servicos/{servico_id}/enviar-frotaweb")
def enviar_servico_json(servico_id: int, db: Session = Depends(get_db)):
    serv = db.query(ServicoOS).filter(ServicoOS.id == servico_id).filter(ServicoOS.deleted_at.is_(None)).first()
    if not serv:
        raise HTTPException(status_code=404, detail="Serviço nao encontrado")
    payload = payload_servico_from_model(serv)
    retorno = post_json(FROTAWEB_SERVICO_URL, payload)
    if not resposta_criada_com_sucesso(retorno):
        serv.status_envio = "ERRO_ENVIO"
        serv.retorno_envio = mensagem_frotaweb(retorno)
        db.commit()
        raise HTTPException(status_code=502, detail=mensagem_frotaweb(retorno))
    serv.status_envio = "ENVIADA"
    serv.retorno_envio = mensagem_frotaweb(retorno, "Serviço enviado com sucesso.")
    db.commit()
    return {"ok": True, "payload_enviado": payload, "retorno": retorno}
