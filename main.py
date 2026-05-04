import os
from typing import Any, Generator, Optional

import requests
from fastapi import FastAPI, Depends, Request, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse
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

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

app = FastAPI(
    title="Painel O.S. Corretiva - Tryout FrotaWeb",
    version="4.0.0",
    description="Recebe O.S. e serviços do app, salva no painel e envia manualmente ao FrotaWeb nos formatos de tryout.",
)

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
        "campos_brutos": {},
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
        "campos_brutos": {},
    }


def post_json(url: str, payload: dict) -> dict:
    try:
        resp = requests.post(url, json=payload, timeout=120)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro ao conectar: {str(exc)}")

    try:
        retorno = resp.json()
    except Exception:
        retorno = {"raw": resp.text}

    if resp.status_code not in (200, 201):
        raise HTTPException(status_code=resp.status_code, detail={"url": url, "payload_enviado": payload, "retorno": retorno})

    return retorno


def status_class(status: str) -> str:
    if status == "ENVIADA":
        return "status-enviada"
    if status.startswith("ERRO"):
        return "status-erro"
    return "status-pendente"

# =========================================================
# HTML
# =========================================================

def html_base(titulo: str, corpo: str, msg: str = "") -> str:
    return f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>{titulo}</title>
      <style>
        body {{ font-family: Arial, sans-serif; background:#f3f4f6; margin:0; padding:24px; color:#111827; }}
        .container {{ max-width:1700px; margin:0 auto; }}
        .card {{ background:white; border-radius:14px; padding:18px; box-shadow:0 2px 12px rgba(0,0,0,.08); margin-bottom:18px; }}
        .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:12px; align-items:end; }}
        label {{ display:block; font-weight:700; margin-bottom:6px; color:#374151; font-size:14px; }}
        input, select {{ width:100%; box-sizing:border-box; padding:10px; border:1px solid #d1d5db; border-radius:8px; }}
        button, .btn {{ background:#2563eb; color:white; border:0; border-radius:8px; padding:10px 14px; cursor:pointer; text-decoration:none; display:inline-block; font-size:14px; }}
        .btn-green {{ background:#059669; }} .btn-gray {{ background:#4b5563; }} .btn-orange {{ background:#ea580c; }}
        .acoes {{ display:flex; gap:8px; flex-wrap:wrap; margin-top:14px; }}
        table {{ width:100%; border-collapse:collapse; background:white; }}
        th, td {{ padding:10px; border-bottom:1px solid #e5e7eb; text-align:left; vertical-align:top; font-size:13px; }}
        th {{ background:#111827; color:white; }}
        tr:hover {{ background:#f9fafb; }}
        .msg {{ background:#ecfdf5; color:#065f46; padding:12px; border-radius:8px; margin-bottom:14px; font-weight:700; }}
        .status-pendente {{ color:#b45309; font-weight:800; }} .status-enviada {{ color:#047857; font-weight:800; }} .status-erro {{ color:#b91c1c; font-weight:800; }}
        .small {{ color:#6b7280; font-size:12px; }} .retorno {{ max-width:260px; white-space:normal; overflow-wrap:anywhere; }}
      </style>
    </head>
    <body>
      <div class="container">
        <h1>{titulo}</h1>
        <div class="small">O.S.: {FROTAWEB_OS_URL} | Serviços: {FROTAWEB_SERVICO_URL}</div>
        {f'<div class="msg">{msg}</div>' if msg else ''}
        {corpo}
      </div>
    </body>
    </html>
    """


def render_painel(ordens, servicos, filtro_status: str = "", msg: str = "") -> str:
    linhas_os = ""
    for o in ordens:
        st = o.status_envio or "PENDENTE"
        ret = (o.retorno_envio or "")
        if len(ret) > 200:
            ret = ret[:200] + "..."
        if st == "ENVIADA":
            acao = "<span class='status-enviada'>Enviada</span>"
        else:
            acao = f"""
            <form method="post" action="/painel/ordens-servico/enviar/{o.id}" onsubmit="return confirm('Enviar O.S. {o.id} ao FrotaWeb?')">
              <button class="btn-green" type="submit">Enviar O.S.</button>
            </form>
            """
        linhas_os += f"""
        <tr>
          <td>{o.id}</td><td><span class="{status_class(st)}">{st}</span></td><td>{o.codigo_veiculo or ""}</td><td>{o.placa or ""}</td>
          <td>{o.hodometro or ""}</td><td>{o.data_hora_abertura or ""}</td><td>{o.data_hora_saida or ""}</td>
          <td>{o.codigo_filial or ""}</td><td>{o.codigo_departamento or ""}</td><td>{o.descricao_defeito or ""}</td>
          <td class="retorno">{ret}</td><td>{o.created_at.strftime("%d/%m/%Y %H:%M") if o.created_at else ""}</td><td>{acao}</td>
        </tr>
        """

    linhas_serv = ""
    for s in servicos:
        st = s.status_envio or "PENDENTE"
        ret = (s.retorno_envio or "")
        if len(ret) > 200:
            ret = ret[:200] + "..."
        if st == "ENVIADA":
            acao = "<span class='status-enviada'>Enviado</span>"
        else:
            acao = f"""
            <form method="post" action="/painel/servicos-os/enviar/{s.id}" onsubmit="return confirm('Enviar serviço {s.id} ao FrotaWeb?')">
              <button class="btn-green" type="submit">Enviar Serviço</button>
            </form>
            """
        linhas_serv += f"""
        <tr>
          <td>{s.id}</td><td><span class="{status_class(st)}">{st}</span></td><td>{s.numero_os or ""}</td><td>{s.codigo_veiculo or ""}</td><td>{s.placa or ""}</td>
          <td>{s.codigo_servico or ""}</td><td>{s.codigo_recurso_humano or ""}</td><td>{s.tempo_gasto or ""}</td><td>{s.valor_hora or ""}</td>
          <td class="retorno">{ret}</td><td>{s.created_at.strftime("%d/%m/%Y %H:%M") if s.created_at else ""}</td><td>{acao}</td>
        </tr>
        """

    corpo = f"""
    <div class="card">
      <form method="get" action="/painel/ordens-servico">
        <div class="grid">
          <div>
            <label>Status</label>
            <select name="status_envio">
              <option value="" {"selected" if not filtro_status else ""}>Todos</option>
              <option value="PENDENTE" {"selected" if filtro_status == "PENDENTE" else ""}>Pendente</option>
              <option value="ENVIADA" {"selected" if filtro_status == "ENVIADA" else ""}>Enviada</option>
              <option value="ERRO_ENVIO" {"selected" if filtro_status == "ERRO_ENVIO" else ""}>Erro</option>
            </select>
          </div>
        </div>
        <div class="acoes"><button type="submit">Filtrar</button><a class="btn btn-gray" href="/painel/ordens-servico">Limpar</a><a class="btn btn-gray" href="/docs">Docs</a></div>
      </form>
    </div>
    <div class="card" style="padding:0; overflow:auto;">
      <h2 style="padding:18px; margin:0;">Ordens de Serviço</h2>
      <table><thead><tr><th>ID</th><th>Status</th><th>Veículo</th><th>Placa</th><th>KM</th><th>Abertura</th><th>Saída</th><th>Filial</th><th>Departamento</th><th>Defeito</th><th>Retorno</th><th>Criado em</th><th>Ação</th></tr></thead>
      <tbody>{linhas_os if linhas_os else '<tr><td colspan="13">Nenhuma O.S. encontrada</td></tr>'}</tbody></table>
    </div>
    <div class="card" style="padding:0; overflow:auto;">
      <h2 style="padding:18px; margin:0;">Serviços da O.S.</h2>
      <table><thead><tr><th>ID</th><th>Status</th><th>Nº O.S.</th><th>Veículo</th><th>Placa</th><th>Cód. Serviço</th><th>Recurso</th><th>Tempo</th><th>Valor Hora</th><th>Retorno</th><th>Criado em</th><th>Ação</th></tr></thead>
      <tbody>{linhas_serv if linhas_serv else '<tr><td colspan="12">Nenhum serviço encontrado</td></tr>'}</tbody></table>
    </div>
    """
    return html_base("Painel O.S. Corretiva", corpo, msg)

# =========================================================
# ROTAS
# =========================================================

@app.get("/")
def home():
    return {"status": "online", "painel": "/painel/ordens-servico", "receber_os": "POST /panel/os", "receber_servico": "POST /panel/os/servicos", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/panel/os")
@app.post("/ordens-servico")
async def receber_os(request: Request, db: Session = Depends(get_db)):
    try:
        dados = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON invalido")

    p = montar_payload_os(dados)
    if not p["codigo_veiculo"] and not p["placa"]:
        raise HTTPException(status_code=400, detail="Informe codigo_veiculo ou placa")

    c = p["credenciais"]
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
        status_envio="PENDENTE", retorno_envio="O.S. salva no painel. Aguardando envio manual.", payload_original=str(dados), campos_brutos=str(p["campos_brutos"]),
    )
    db.add(ordem)
    db.commit()
    db.refresh(ordem)
    return {"ok": True, "created": True, "id": ordem.id, "order_number": str(ordem.id), "status_envio": ordem.status_envio, "message": "O.S. salva no painel.", "payload_tryout": payload_os_from_ordem(ordem)}


@app.post("/panel/os/servicos")
@app.post("/ordens-servico/servicos")
async def receber_servico(request: Request, db: Session = Depends(get_db)):
    try:
        dados = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON invalido")

    p = montar_payload_servico(dados)
    if not p["numero_os"]:
        raise HTTPException(status_code=400, detail="Informe numero_os")
    if not p["codigo_servico"]:
        raise HTTPException(status_code=400, detail="Informe codigo_servico")

    c = p["credenciais"]
    serv = ServicoOS(
        empresa=c["empresa"], usuario=c["usuario"], senha=c["senha"], filial=c["filial"], recurso_humano=c["recurso_humano"],
        numero_os=p["numero_os"], codigo_veiculo=p["codigo_veiculo"], placa=p["placa"], codigo_servico=p["codigo_servico"],
        codigo_recurso_humano=p["codigo_recurso_humano"], tempo_gasto=p["tempo_gasto"], valor_hora=p["valor_hora"],
        status_envio="PENDENTE", retorno_envio="Serviço salvo no painel. Aguardando envio manual.", payload_original=str(dados), campos_brutos=str(p["campos_brutos"]),
    )
    db.add(serv)
    db.commit()
    db.refresh(serv)
    return {"ok": True, "created": True, "id": serv.id, "status_envio": serv.status_envio, "message": "Serviço salvo no painel.", "payload_tryout": payload_servico_from_model(serv)}


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
def painel(status_envio: Optional[str] = Query(None), msg: Optional[str] = Query(None), db: Session = Depends(get_db)):
    qo = db.query(OrdemServico).filter(OrdemServico.deleted_at.is_(None))
    qs = db.query(ServicoOS).filter(ServicoOS.deleted_at.is_(None))
    if status_envio:
        qo = qo.filter(OrdemServico.status_envio == status_envio)
        qs = qs.filter(ServicoOS.status_envio == status_envio)
    return HTMLResponse(render_painel(qo.order_by(OrdemServico.created_at.desc()).all(), qs.order_by(ServicoOS.created_at.desc()).all(), status_envio or "", msg or ""))


@app.post("/painel/ordens-servico/enviar/{ordem_id}")
def enviar_os(ordem_id: int, db: Session = Depends(get_db)):
    ordem = db.query(OrdemServico).filter(OrdemServico.id == ordem_id).filter(OrdemServico.deleted_at.is_(None)).first()
    if not ordem:
        raise HTTPException(status_code=404, detail="O.S. nao encontrada")
    payload = payload_os_from_ordem(ordem)
    try:
        retorno = post_json(FROTAWEB_OS_URL, payload)
    except HTTPException as exc:
        ordem.status_envio = "ERRO_ENVIO"
        ordem.retorno_envio = str(exc.detail)
        db.commit()
        return RedirectResponse(url=f"/painel/ordens-servico?msg=Erro ao enviar O.S. {ordem.id}", status_code=303)
    ordem.status_envio = "ENVIADA"
    ordem.retorno_envio = str(retorno)
    db.commit()
    return RedirectResponse(url=f"/painel/ordens-servico?msg=O.S. {ordem.id} enviada com sucesso", status_code=303)


@app.post("/painel/servicos-os/enviar/{servico_id}")
def enviar_servico(servico_id: int, db: Session = Depends(get_db)):
    serv = db.query(ServicoOS).filter(ServicoOS.id == servico_id).filter(ServicoOS.deleted_at.is_(None)).first()
    if not serv:
        raise HTTPException(status_code=404, detail="Serviço nao encontrado")
    payload = payload_servico_from_model(serv)
    try:
        retorno = post_json(FROTAWEB_SERVICO_URL, payload)
    except HTTPException as exc:
        serv.status_envio = "ERRO_ENVIO"
        serv.retorno_envio = str(exc.detail)
        db.commit()
        return RedirectResponse(url=f"/painel/ordens-servico?msg=Erro ao enviar serviço {serv.id}", status_code=303)
    serv.status_envio = "ENVIADA"
    serv.retorno_envio = str(retorno)
    db.commit()
    return RedirectResponse(url=f"/painel/ordens-servico?msg=Serviço {serv.id} enviado com sucesso", status_code=303)


@app.post("/ordens-servico/{ordem_id}/enviar-frotaweb")
def enviar_os_json(ordem_id: int, db: Session = Depends(get_db)):
    ordem = db.query(OrdemServico).filter(OrdemServico.id == ordem_id).filter(OrdemServico.deleted_at.is_(None)).first()
    if not ordem:
        raise HTTPException(status_code=404, detail="O.S. nao encontrada")
    payload = payload_os_from_ordem(ordem)
    retorno = post_json(FROTAWEB_OS_URL, payload)
    ordem.status_envio = "ENVIADA"
    ordem.retorno_envio = str(retorno)
    db.commit()
    return {"ok": True, "payload_enviado": payload, "retorno": retorno}


@app.post("/ordens-servico/servicos/{servico_id}/enviar-frotaweb")
def enviar_servico_json(servico_id: int, db: Session = Depends(get_db)):
    serv = db.query(ServicoOS).filter(ServicoOS.id == servico_id).filter(ServicoOS.deleted_at.is_(None)).first()
    if not serv:
        raise HTTPException(status_code=404, detail="Serviço nao encontrado")
    payload = payload_servico_from_model(serv)
    retorno = post_json(FROTAWEB_SERVICO_URL, payload)
    serv.status_envio = "ENVIADA"
    serv.retorno_envio = str(retorno)
    db.commit()
    return {"ok": True, "payload_enviado": payload, "retorno": retorno}
