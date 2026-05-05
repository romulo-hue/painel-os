import os
import json
import base64
from typing import Any, Generator, Optional

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

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

app = FastAPI(
    title="Painel O.S. Corretiva - Tryout FrotaWeb",
    version="4.0.0",
    description="Recebe O.S. e serviços do app, salva no painel e envia manualmente ao FrotaWeb nos formatos de tryout.",
)

UPLOAD_DIR = os.getenv("UPLOAD_DIR", "uploads/fotos")
os.makedirs(UPLOAD_DIR, exist_ok=True)
app.mount("/fotos", StaticFiles(directory=UPLOAD_DIR), name="fotos")

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
        raise HTTPException(
            status_code=resp.status_code,
            detail={
                "url": url,
                "mensagem_frotaweb": mensagem_frotaweb(retorno),
                "retorno": mascarar_senha(retorno),
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


def status_class(status: str) -> str:
    if status == "ENVIADA":
        return "status-enviada"
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
    resumo = escape_html(mensagem_frotaweb(value, "Sem retorno"))
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
        @media (max-width: 980px) {{ .hero {{ flex-direction:column; }} .hero-meta {{ text-align:left; }} body {{ padding:14px; }} }}
      </style>
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
    total_pendentes = len([o for o in ordens_filtradas if (o.status_envio or 'PENDENTE') == 'PENDENTE'])
    total_enviadas = len([o for o in ordens_filtradas if (o.status_envio or '') == 'ENVIADA'])
    total_erros = len([o for o in ordens_filtradas if str(o.status_envio or '').startswith('ERRO')])
    total_fotos = len([o for o in ordens_filtradas if fotos_da_ordem(o)])

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
        else:
            acoes += f"""
            <form method="post" action="/painel/ordens-servico/enviar/{o.id}" onsubmit="return confirm('Enviar O.S. {o.id} ao FrotaWeb?')">
              <button class="btn-green" type="submit">Enviar O.S. + Serviços</button>
            </form>
            """
        acoes += "</div>"

        linhas_os += f"""
        <tr>
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

    linhas_serv = ""
    for s in servicos:
        st = s.status_envio or "PENDENTE"
        retorno_html = retorno_resumido_html(s.retorno_envio or "", f"/painel/detalhes/servico/{s.id}", success=st == "ENVIADA")
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
          <td><strong>#{s.id}</strong></td><td><span class="{status_class(st)}">{st}</span></td><td>{escape_html(s.numero_os or '')}</td><td>{escape_html(s.codigo_veiculo or '')}</td><td>{escape_html(s.placa or '')}</td>
          <td>{escape_html(s.codigo_servico or '')}</td><td>{escape_html(s.codigo_recurso_humano or '')}</td><td>{escape_html(s.tempo_gasto or '')}</td><td>{escape_html(s.valor_hora or '')}</td>
          <td class="retorno">{retorno_html}</td><td>{s.created_at.strftime('%d/%m/%Y %H:%M') if s.created_at else ''}</td><td>{acao}</td>
        </tr>
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
    </div>
    <div class="card">
      <form method="get" action="/painel/ordens-servico">
        <div class="filter-grid">
          <input name="id" placeholder="Filtrar ID Painel" value="{fv('id')}">
          <select name="status_envio">
            <option value="" {"selected" if not filtros.get("status_envio") else ""}>Status: Todos</option>
            <option value="PENDENTE" {"selected" if filtros.get("status_envio") == "PENDENTE" else ""}>Pendente</option>
            <option value="ENVIADA" {"selected" if filtros.get("status_envio") == "ENVIADA" else ""}>Enviada</option>
            <option value="ERRO_ENVIO" {"selected" if filtros.get("status_envio") == "ERRO_ENVIO" else ""}>Erro</option>
          </select>
          <input name="numero_os" placeholder="Filtrar N? O.S. FrotaWeb" value="{fv('numero_os')}">
          <input name="servicos" placeholder="Filtrar Servi?os" value="{fv('servicos')}">
          <input name="codigo_veiculo" placeholder="Filtrar Ve?culo" value="{fv('codigo_veiculo')}">
          <input name="placa" placeholder="Filtrar Placa" value="{fv('placa')}">
          <input name="hodometro" placeholder="Filtrar KM" value="{fv('hodometro')}">
          <input name="data_hora_abertura" placeholder="Filtrar Abertura/Data" value="{fv('data_hora_abertura')}">
          <input name="data_hora_saida" placeholder="Filtrar Sa?da" value="{fv('data_hora_saida')}">
          <input name="codigo_filial" placeholder="Filtrar Filial" value="{fv('codigo_filial')}">
          <input name="codigo_departamento" placeholder="Filtrar Departamento" value="{fv('codigo_departamento')}">
          <input name="descricao_defeito" placeholder="Filtrar Defeito" value="{fv('descricao_defeito')}">
          <input name="retorno_envio" placeholder="Buscar no retorno" value="{fv('retorno_envio')}">
          <input name="created_at" placeholder="Filtrar Criado em" value="{fv('created_at')}">
          <select name="fotos">
            <option value="" {"selected" if not filtros.get("fotos") else ""}>Fotos: Todos</option>
            <option value="com_foto" {"selected" if filtros.get("fotos") == "com_foto" else ""}>Somente com foto</option>
          </select>
        </div>
        <div class="acoes">
          <button type="submit">Filtrar</button>
          <a class="btn btn-gray" href="/painel/ordens-servico">Limpar</a>
          <form method="post" action="/painel/ordens-servico/limpar-cache" onsubmit="return confirm('Limpar toda a fila/cache de O.S. e serviços do painel?');">
            <button type="submit" class="btn-red">Limpar cache</button>
          </form>
          <a class="btn btn-gray" href="/docs">Docs</a>
        </div>
      </form>
    </div>
    <div class="card" style="padding:0;">
      <div style="padding:18px 18px 8px;">
        <h2 style="margin:0;">Ordens de Serviço</h2>
        <div class="subtle">A coluna de retorno mostra primeiro a mensagem exata do FrotaWeb e deixa o detalhe tecnico recolhido.</div>
      </div>
      <div class="table-wrap">
        <table><thead><tr><th>ID Painel</th><th>Status</th><th>Nº O.S. FrotaWeb</th><th>Serviços</th><th>Veículo</th><th>Placa</th><th>KM</th><th>Abertura</th><th>Saída</th><th>Filial</th><th>Departamento</th><th>Defeito</th><th>Fotos</th><th>Retorno</th><th>Criado em</th><th>Ação</th></tr></thead>
        <tbody>{linhas_os if linhas_os else '<tr><td colspan="16">Nenhuma O.S. encontrada</td></tr>'}</tbody></table>
      </div>
    </div>
    <div class="card" style="padding:0;">
      <div style="padding:18px 18px 8px;">
        <h2 style="margin:0;">Serviços da O.S.</h2>
        <div class="subtle">Serviços com erro mostram a mensagem do FrotaWeb sem expor senha ou payload bruto.</div>
      </div>
      <div class="table-wrap">
        <table><thead><tr><th>ID</th><th>Status</th><th>Nº O.S.</th><th>Veículo</th><th>Placa</th><th>Cód. Serviço</th><th>Recurso</th><th>Tempo</th><th>Valor Hora</th><th>Retorno</th><th>Criado em</th><th>Ação</th></tr></thead>
        <tbody>{linhas_serv if linhas_serv else '<tr><td colspan="12">Nenhum serviço encontrado</td></tr>'}</tbody></table>
      </div>
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
        payload_original=json_dumps_safe(mascarar_senha(dados)),
        campos_brutos=json_dumps_safe(p["campos_brutos"]),
        fotos_app=json_dumps_safe(fotos_recebidas),
        app_payload=json_dumps_safe(dados),
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
    elif tipo == "servico":
        registro = db.query(ServicoOS).filter(ServicoOS.id == registro_id).first()
        if not registro:
            raise HTTPException(status_code=404, detail="Serviço nao encontrado")
        titulo = f"Detalhes do Serviço {registro.id}"
        retorno = registro.retorno_envio or ""
        payload_original = mascarar_senha(json_loads_safe(registro.payload_original or "", registro.payload_original))
        payload_app = payload_original
    else:
        raise HTTPException(status_code=404, detail="Tipo de detalhe inválido")

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

    payload = payload_os_from_ordem(ordem)

    try:
        retorno = post_json(FROTAWEB_OS_URL, payload)
    except HTTPException as exc:
        ordem.status_envio = "ERRO_ENVIO"
        ordem.retorno_envio = mensagem_frotaweb(exc.detail)
        db.commit()
        return RedirectResponse(url=f"/painel/ordens-servico?msg=Erro ao enviar O.S. {ordem.id}", status_code=303)

    if not resposta_criada_com_sucesso(retorno):
        ordem.status_envio = "ERRO_ENVIO"
        ordem.retorno_envio = mensagem_frotaweb(retorno)
        db.commit()
        return RedirectResponse(url=f"/painel/ordens-servico?msg=Erro ao enviar O.S. {ordem.id}", status_code=303)

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
    db.commit()

    msg = (
        f"O.S. {ordem.id} enviada com sucesso. "
        f"Nº FrotaWeb: {numero_os_frotaweb or ordem.numero_os or 'não identificado'}. "
        f"Serviços: {resumo_servicos['enviados']}/{resumo_servicos['total']} enviados."
    )
    if resumo_servicos["erros"]:
        msg += f" {resumo_servicos['erros']} serviço(s) com erro."

    return RedirectResponse(url=f"/painel/ordens-servico?msg={msg}", status_code=303)


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
        serv.retorno_envio = mensagem_frotaweb(exc.detail)
        db.commit()
        return RedirectResponse(url=f"/painel/ordens-servico?msg=Erro ao enviar serviço {serv.id}", status_code=303)
    if resposta_criada_com_sucesso(retorno):
        serv.status_envio = "ENVIADA"
        serv.retorno_envio = mensagem_frotaweb(retorno, "Serviço enviado com sucesso.")
        db.commit()
        return RedirectResponse(url=f"/painel/ordens-servico?msg=Serviço {serv.id} enviado com sucesso", status_code=303)
    serv.status_envio = "ERRO_ENVIO"
    serv.retorno_envio = mensagem_frotaweb(retorno)
    db.commit()
    return RedirectResponse(url=f"/painel/ordens-servico?msg=Erro ao enviar serviço {serv.id}", status_code=303)


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
