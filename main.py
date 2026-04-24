import os
from io import BytesIO
from typing import Optional

import pandas as pd
import requests
from fastapi import FastAPI, Depends, Query, Request, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, RedirectResponse
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, func
from sqlalchemy.orm import sessionmaker, declarative_base, Session

# =========================================================
# CONFIG
# =========================================================

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não configurada no ambiente")

INTEGRACAO_OUTRA_API_URL = os.getenv(
    "INTEGRACAO_OUTRA_API_URL",
    "https://integracao-frotaweb.onrender.com/frotaweb/os-corretiva",
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

app = FastAPI(title="Painel de Ordens de Serviço")

# =========================================================
# DATABASE
# =========================================================

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# =========================================================
# MODELS
# =========================================================

class OrdemServico(Base):
    __tablename__ = "ordens_servico"

    id = Column(Integer, primary_key=True, index=True)
    usuario = Column(String(255), nullable=True)
    filial_login = Column(String(255), nullable=True)
    cd_empresa = Column(String(100), nullable=True)
    cd_veiculo = Column(String(100), nullable=True)
    placa = Column(String(50), nullable=True)
    dh_entrada = Column(String(100), nullable=True)
    km_entrada = Column(String(100), nullable=True)
    dh_saida = Column(String(100), nullable=True)
    km_saida = Column(String(100), nullable=True)
    dh_inicio = Column(String(100), nullable=True)
    dh_prev = Column(String(100), nullable=True)
    cd_filial = Column(String(100), nullable=True)
    cd_ccusto = Column(String(100), nullable=True)
    observacao = Column(Text, nullable=True)
    cd_servico = Column(String(100), nullable=True)
    cd_servicos = Column(Text, nullable=True)
    try_out = Column(String(255), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)


class VeiculoReferencia(Base):
    __tablename__ = "veiculos_referencia"

    id = Column(Integer, primary_key=True, index=True)
    placa = Column(String(20), nullable=False, unique=True, index=True)
    cd_veiculo = Column(String(100), nullable=False)
    cd_filial = Column(String(100), nullable=False)
    cd_ccusto = Column(String(100), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class UsuarioApp(Base):
    __tablename__ = "usuarios_app"

    id = Column(Integer, primary_key=True, index=True)
    matricula = Column(String(50), nullable=False, unique=True, index=True)
    senha = Column(String(255), nullable=False)
    cpf = Column(String(20), nullable=True)
    nome_completo = Column(String(255), nullable=False)
    funcao = Column(String(100), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

# =========================================================
# INIT DB
# =========================================================

Base.metadata.create_all(bind=engine)

# =========================================================
# HELPERS
# =========================================================

def to_str(value, default=""):
    if value is None:
        return default
    text = str(value).strip()
    if text.lower() == "nan":
        return default
    return text


def normalizar_placa(placa: str) -> str:
    return to_str(placa).upper().replace("-", "").replace(" ", "")


def normalize_cd_servicos(value):
    if isinstance(value, list):
        return [to_str(v) for v in value if to_str(v)]
    if value is None:
        return []
    valor = to_str(value)
    return [valor] if valor else []


def montar_payload(dados: dict) -> dict:
    return {
        "usuario": to_str(dados.get("usuario")),
        "senha": to_str(dados.get("senha")),
        "filial_login": to_str(dados.get("filial_login")),
        "cd_empresa": to_str(dados.get("cd_empresa")),
        "cd_veiculo": to_str(dados.get("cd_veiculo")),
        "placa": to_str(dados.get("placa")).upper(),
        "dh_entrada": to_str(dados.get("dh_entrada")),
        "km_entrada": to_str(dados.get("km_entrada")),
        "dh_saida": to_str(dados.get("dh_saida")),
        "km_saida": to_str(dados.get("km_saida")),
        "dh_inicio": to_str(dados.get("dh_inicio")),
        "dh_prev": to_str(dados.get("dh_prev")),
        "cd_filial": to_str(dados.get("cd_filial")),
        "cd_ccusto": to_str(dados.get("cd_ccusto")),
        "observacao": to_str(dados.get("observacao")),
        "cd_servico": to_str(dados.get("cd_servico")),
        "cd_servicos": normalize_cd_servicos(dados.get("cd_servicos")),
        "try_out": to_str(dados.get("try_out")),
    }


def enviar_para_outra_api(payload: dict):
    payload_api = dict(payload)
    payload_api.pop("try_out", None)

    try:
        resp = requests.post(
            INTEGRACAO_OUTRA_API_URL,
            json=payload_api,
            timeout=120,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao conectar na outra API: {str(e)}")

    try:
        retorno = resp.json()
    except Exception:
        retorno = {"raw": resp.text}

    if resp.status_code not in (200, 201):
        raise HTTPException(
            status_code=resp.status_code,
            detail={
                "payload_enviado": payload_api,
                "retorno": retorno,
            },
        )

    return retorno


def aplicar_filtros_ordens(query, usuario, placa, cd_veiculo, try_out):
    query = query.filter(OrdemServico.deleted_at.is_(None))

    if usuario:
        query = query.filter(OrdemServico.usuario.ilike(f"%{usuario}%"))

    if placa:
        query = query.filter(OrdemServico.placa.ilike(f"%{placa}%"))

    if cd_veiculo:
        query = query.filter(OrdemServico.cd_veiculo.ilike(f"%{cd_veiculo}%"))

    if try_out:
        query = query.filter(OrdemServico.try_out.ilike(f"%{try_out}%"))

    return query


def buscar_veiculo_db(db: Session, placa: str):
    placa_limpa = normalizar_placa(placa)

    return (
        db.query(VeiculoReferencia)
        .filter(func.replace(func.replace(func.upper(VeiculoReferencia.placa), "-", ""), " ", "") == placa_limpa)
        .first()
    )

# =========================================================
# HTML ORDENS
# =========================================================

def render_html(ordens, filtros):
    linhas = ""

    for item in ordens:
        linhas += f"""
        <tr>
            <td>{item.id}</td>
            <td>{item.usuario or ""}</td>
            <td>{item.placa or ""}</td>
            <td>{item.cd_veiculo or ""}</td>
            <td>{item.cd_filial or ""}</td>
            <td>{item.cd_servico or ""}</td>
            <td>{item.try_out or ""}</td>
            <td>{item.observacao or ""}</td>
            <td>{item.created_at.strftime("%d/%m/%Y %H:%M") if item.created_at else ""}</td>
        </tr>
        """

    return f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8" />
        <title>Painel de Ordens de Serviço</title>
        <style>
            body {{ font-family: Arial, sans-serif; background: #f4f6f9; margin: 0; padding: 24px; }}
            .container {{ max-width: 1600px; margin: 0 auto; }}
            .card {{ background: white; border-radius: 12px; padding: 20px; box-shadow: 0 2px 12px rgba(0,0,0,0.08); margin-bottom: 20px; }}
            .filtros {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; align-items: end; }}
            label {{ font-size: 14px; color: #374151; display: block; margin-bottom: 6px; }}
            input {{ width: 100%; padding: 10px; border: 1px solid #d1d5db; border-radius: 8px; box-sizing: border-box; }}
            .acoes {{ display: flex; gap: 10px; flex-wrap: wrap; margin-top: 16px; }}
            button, .btn {{ background: #2563eb; color: white; border: none; padding: 10px 16px; border-radius: 8px; cursor: pointer; text-decoration: none; display: inline-block; font-size: 14px; }}
            .btn-secondary {{ background: #059669; }}
            .btn-json {{ background: #7c3aed; }}
            .btn-orange {{ background: #ea580c; }}
            .btn-dark {{ background: #111827; }}
            table {{ width: 100%; border-collapse: collapse; background: white; }}
            th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #e5e7eb; vertical-align: top; font-size: 14px; }}
            th {{ background: #111827; color: white; }}
            .topbar {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 18px; flex-wrap: wrap; gap: 12px; }}
            .badge {{ background: #e0f2fe; color: #0369a1; padding: 8px 12px; border-radius: 999px; font-size: 14px; font-weight: bold; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="topbar">
                <h1>Painel de Ordens de Serviço</h1>
                <div class="badge">Total de registros: {len(ordens)}</div>
            </div>

            <div class="card">
                <div class="acoes" style="margin-top:0;">
                    <a class="btn btn-orange" href="/painel/veiculos">Cadastro de Veículos</a>
                    <a class="btn btn-dark" href="/painel/usuarios">Cadastro de Usuários</a>
                </div>
            </div>

            <div class="card">
                <form method="get" action="/painel/ordens-servico">
                    <div class="filtros">
                        <div>
                            <label>Usuário</label>
                            <input type="text" name="usuario" value="{filtros.get("usuario", "")}">
                        </div>
                        <div>
                            <label>Placa</label>
                            <input type="text" name="placa" value="{filtros.get("placa", "")}">
                        </div>
                        <div>
                            <label>Código do veículo</label>
                            <input type="text" name="cd_veiculo" value="{filtros.get("cd_veiculo", "")}">
                        </div>
                        <div>
                            <label>Try Out</label>
                            <input type="text" name="try_out" value="{filtros.get("try_out", "")}">
                        </div>
                    </div>

                    <div class="acoes">
                        <button type="submit">Filtrar</button>
                        <a class="btn btn-secondary" href="/painel/ordens-servico/exportar/xlsx?usuario={filtros.get("usuario", "")}&placa={filtros.get("placa", "")}&cd_veiculo={filtros.get("cd_veiculo", "")}&try_out={filtros.get("try_out", "")}">Exportar XLSX</a>
                        <a class="btn btn-json" href="/painel/ordens-servico/exportar/json?usuario={filtros.get("usuario", "")}&placa={filtros.get("placa", "")}&cd_veiculo={filtros.get("cd_veiculo", "")}&try_out={filtros.get("try_out", "")}">Exportar JSON</a>
                    </div>
                </form>
            </div>

            <div class="card" style="padding:0; overflow:auto;">
                <table>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Usuário</th>
                            <th>Placa</th>
                            <th>CD Veículo</th>
                            <th>CD Filial</th>
                            <th>CD Serviço</th>
                            <th>Try Out</th>
                            <th>Observação</th>
                            <th>Criado em</th>
                        </tr>
                    </thead>
                    <tbody>
                        {linhas if linhas else '<tr><td colspan="9">Nenhum registro encontrado</td></tr>'}
                    </tbody>
                </table>
            </div>
        </div>
    </body>
    </html>
    """

# =========================================================
# HTML VEÍCULOS
# =========================================================

def render_veiculos_html(veiculos, busca="", mensagem=""):
    linhas = ""

    for item in veiculos:
        linhas += f"""
        <tr>
            <td>{item.id}</td>
            <td>{item.placa}</td>
            <td>{item.cd_veiculo}</td>
            <td>{item.cd_filial}</td>
            <td>{item.cd_ccusto}</td>
            <td>{item.created_at.strftime("%d/%m/%Y %H:%M") if item.created_at else ""}</td>
            <td>
                <form method="post" action="/painel/veiculos/excluir/{item.id}" onsubmit="return confirm('Excluir este veículo?')">
                    <button class="btn-danger" type="submit">Excluir</button>
                </form>
            </td>
        </tr>
        """

    return render_base_html(
        titulo="Cadastro de Veículos",
        mensagem=mensagem,
        corpo=f"""
        <div class="acoes">
            <a class="btn btn-gray" href="/painel/ordens-servico">Voltar para O.S.</a>
            <a class="btn btn-dark" href="/painel/usuarios">Cadastro de Usuários</a>
        </div>

        <div class="card">
            <h2>Adicionar ou atualizar veículo</h2>
            <form method="post" action="/painel/veiculos/adicionar">
                <div class="grid">
                    <div><label>Placa</label><input name="placa" placeholder="Ex.: SBB2B33" required></div>
                    <div><label>Código de frota</label><input name="cd_veiculo" placeholder="Ex.: 12040" required></div>
                    <div><label>Filial da O.S.</label><input name="cd_filial" placeholder="Ex.: 1" required></div>
                    <div><label>Centro de custo</label><input name="cd_ccusto" placeholder="Ex.: 420119" required></div>
                </div>
                <div class="acoes"><button type="submit">Salvar veículo</button></div>
            </form>
        </div>

        <div class="card">
            <h2>Importar lote via XLSX</h2>
            <form method="post" action="/painel/veiculos/importar-xlsx" enctype="multipart/form-data">
                <input type="file" name="arquivo" accept=".xlsx" required>
                <div class="acoes"><button class="btn-green" type="submit">Importar XLSX</button></div>
            </form>
            <p class="hint">Colunas obrigatórias: <b>placa</b>, <b>cd_veiculo</b>, <b>cd_filial</b>, <b>cd_ccusto</b>.</p>
        </div>

        <div class="card">
            <h2>Pesquisar</h2>
            <form method="get" action="/painel/veiculos">
                <div class="grid">
                    <div><label>Buscar por placa</label><input name="busca" value="{busca}" placeholder="Digite uma placa"></div>
                </div>
                <div class="acoes">
                    <button type="submit">Buscar</button>
                    <a class="btn btn-gray" href="/painel/veiculos">Limpar</a>
                </div>
            </form>
        </div>

        <div class="card" style="padding:0; overflow:auto;">
            <table>
                <thead>
                    <tr>
                        <th>ID</th><th>Placa</th><th>Código Frota</th><th>Filial O.S.</th><th>Centro Custo</th><th>Criado em</th><th>Ação</th>
                    </tr>
                </thead>
                <tbody>{linhas if linhas else '<tr><td colspan="7">Nenhum veículo cadastrado</td></tr>'}</tbody>
            </table>
        </div>
        """,
    )

# =========================================================
# HTML USUÁRIOS
# =========================================================

def render_usuarios_html(usuarios, busca="", mensagem=""):
    linhas = ""

    for item in usuarios:
        linhas += f"""
        <tr>
            <td>{item.id}</td>
            <td>{item.matricula}</td>
            <td>{item.nome_completo}</td>
            <td>{item.cpf or ""}</td>
            <td>{item.funcao}</td>
            <td>{item.created_at.strftime("%d/%m/%Y %H:%M") if item.created_at else ""}</td>
            <td>
                <form method="post" action="/painel/usuarios/excluir/{item.id}" onsubmit="return confirm('Excluir este usuário?')">
                    <button class="btn-danger" type="submit">Excluir</button>
                </form>
            </td>
        </tr>
        """

    return render_base_html(
        titulo="Cadastro de Usuários",
        mensagem=mensagem,
        corpo=f"""
        <div class="acoes">
            <a class="btn btn-gray" href="/painel/ordens-servico">Voltar para O.S.</a>
            <a class="btn btn-orange" href="/painel/veiculos">Cadastro de Veículos</a>
        </div>

        <div class="card">
            <h2>Adicionar ou atualizar usuário</h2>
            <form method="post" action="/painel/usuarios/adicionar">
                <div class="grid">
                    <div><label>Matrícula</label><input name="matricula" required></div>
                    <div><label>Senha</label><input name="senha" required></div>
                    <div><label>CPF opcional</label><input name="cpf"></div>
                    <div><label>Nome completo</label><input name="nome_completo" required></div>
                    <div><label>Função</label><input name="funcao" placeholder="Ex.: Mecânico" required></div>
                </div>
                <div class="acoes"><button type="submit">Salvar usuário</button></div>
            </form>
        </div>

        <div class="card">
            <h2>Importar lote via XLSX</h2>
            <form method="post" action="/painel/usuarios/importar-xlsx" enctype="multipart/form-data">
                <input type="file" name="arquivo" accept=".xlsx" required>
                <div class="acoes"><button class="btn-green" type="submit">Importar XLSX</button></div>
            </form>
            <p class="hint">Colunas obrigatórias: <b>matricula</b>, <b>senha</b>, <b>nome_completo</b>, <b>funcao</b>. Coluna opcional: <b>cpf</b>.</p>
        </div>

        <div class="card">
            <h2>Pesquisar</h2>
            <form method="get" action="/painel/usuarios">
                <div class="grid">
                    <div><label>Buscar por matrícula ou nome</label><input name="busca" value="{busca}" placeholder="Digite matrícula ou nome"></div>
                </div>
                <div class="acoes">
                    <button type="submit">Buscar</button>
                    <a class="btn btn-gray" href="/painel/usuarios">Limpar</a>
                </div>
            </form>
        </div>

        <div class="card" style="padding:0; overflow:auto;">
            <table>
                <thead>
                    <tr>
                        <th>ID</th><th>Matrícula</th><th>Nome completo</th><th>CPF</th><th>Função</th><th>Criado em</th><th>Ação</th>
                    </tr>
                </thead>
                <tbody>{linhas if linhas else '<tr><td colspan="7">Nenhum usuário cadastrado</td></tr>'}</tbody>
            </table>
        </div>
        """,
    )


def render_base_html(titulo: str, corpo: str, mensagem: str = ""):
    return f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8" />
        <title>{titulo}</title>
        <style>
            body {{ font-family: Arial, sans-serif; background: #f4f6f9; margin: 0; padding: 24px; }}
            .container {{ max-width: 1400px; margin: 0 auto; }}
            .card {{ background: white; border-radius: 12px; padding: 20px; box-shadow: 0 2px 12px rgba(0,0,0,0.08); margin-bottom: 20px; }}
            .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; align-items: end; }}
            label {{ font-size: 14px; color: #374151; display: block; margin-bottom: 6px; font-weight: bold; }}
            input {{ width: 100%; padding: 10px; border: 1px solid #d1d5db; border-radius: 8px; box-sizing: border-box; }}
            button, .btn {{ background: #2563eb; color: white; border: none; padding: 10px 16px; border-radius: 8px; cursor: pointer; text-decoration: none; display: inline-block; font-size: 14px; }}
            .btn-green {{ background: #059669; }}
            .btn-danger {{ background: #dc2626; }}
            .btn-gray {{ background: #374151; }}
            .btn-orange {{ background: #ea580c; }}
            .btn-dark {{ background: #111827; }}
            .acoes {{ display: flex; gap: 10px; flex-wrap: wrap; margin: 16px 0; }}
            table {{ width: 100%; border-collapse: collapse; background: white; }}
            th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #e5e7eb; vertical-align: top; font-size: 14px; }}
            th {{ background: #111827; color: white; }}
            .msg {{ background: #ecfdf5; color: #065f46; padding: 12px; border-radius: 8px; margin-bottom: 16px; font-weight: bold; }}
            .hint {{ color: #6b7280; font-size: 14px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>{titulo}</h1>
            {f'<div class="msg">{mensagem}</div>' if mensagem else ''}
            {corpo}
        </div>
    </body>
    </html>
    """

# =========================================================
# ROTAS BÁSICAS
# =========================================================

@app.get("/")
def home():
    return {
        "message": "API de Ordens de Serviço online",
        "painel_ordens": "/painel/ordens-servico",
        "painel_veiculos": "/painel/veiculos",
        "painel_usuarios": "/painel/usuarios",
        "docs": "/docs",
    }


@app.get("/health")
def health():
    return {"status": "ok"}

# =========================================================
# ROTAS ORDENS DE SERVIÇO
# =========================================================

@app.post("/ordens-servico")
async def criar_ordem_servico(request: Request, db: Session = Depends(get_db)):
    try:
        dados = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido")

    payload = montar_payload(dados)
    retorno_integracao = enviar_para_outra_api(payload)

    nova = OrdemServico(
        usuario=payload["usuario"],
        filial_login=payload["filial_login"],
        cd_empresa=payload["cd_empresa"],
        cd_veiculo=payload["cd_veiculo"],
        placa=payload["placa"],
        dh_entrada=payload["dh_entrada"],
        km_entrada=payload["km_entrada"],
        dh_saida=payload["dh_saida"],
        km_saida=payload["km_saida"],
        dh_inicio=payload["dh_inicio"],
        dh_prev=payload["dh_prev"],
        cd_filial=payload["cd_filial"],
        cd_ccusto=payload["cd_ccusto"],
        observacao=payload["observacao"],
        cd_servico=payload["cd_servico"],
        cd_servicos=",".join(payload["cd_servicos"]),
        try_out=payload["try_out"],
    )

    db.add(nova)
    db.commit()
    db.refresh(nova)

    return {
        "ok": True,
        "id": nova.id,
        "message": "Ordem de serviço criada com sucesso e enviada para a outra API",
        "payload_enviado": payload,
        "retorno_integracao": retorno_integracao,
    }


@app.get("/ordens-servico")
def listar_ordens_servico(
    usuario: Optional[str] = Query(None),
    placa: Optional[str] = Query(None),
    cd_veiculo: Optional[str] = Query(None),
    try_out: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    query = db.query(OrdemServico)
    query = aplicar_filtros_ordens(query, usuario, placa, cd_veiculo, try_out)
    registros = query.order_by(OrdemServico.created_at.desc()).all()

    return [
        {
            "id": item.id,
            "usuario": item.usuario,
            "filial_login": item.filial_login,
            "cd_empresa": item.cd_empresa,
            "cd_veiculo": item.cd_veiculo,
            "placa": item.placa,
            "dh_entrada": item.dh_entrada,
            "km_entrada": item.km_entrada,
            "dh_saida": item.dh_saida,
            "km_saida": item.km_saida,
            "dh_inicio": item.dh_inicio,
            "dh_prev": item.dh_prev,
            "cd_filial": item.cd_filial,
            "cd_ccusto": item.cd_ccusto,
            "observacao": item.observacao,
            "cd_servico": item.cd_servico,
            "cd_servicos": item.cd_servicos.split(",") if item.cd_servicos else [],
            "try_out": item.try_out,
            "created_at": item.created_at.isoformat() if item.created_at else None,
            "updated_at": item.updated_at.isoformat() if item.updated_at else None,
        }
        for item in registros
    ]


@app.get("/painel/ordens-servico", response_class=HTMLResponse)
def painel_ordens_servico(
    usuario: Optional[str] = Query(None),
    placa: Optional[str] = Query(None),
    cd_veiculo: Optional[str] = Query(None),
    try_out: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    query = db.query(OrdemServico)
    query = aplicar_filtros_ordens(query, usuario, placa, cd_veiculo, try_out)
    ordens = query.order_by(OrdemServico.created_at.desc()).all()

    filtros = {
        "usuario": usuario or "",
        "placa": placa or "",
        "cd_veiculo": cd_veiculo or "",
        "try_out": try_out or "",
    }

    return HTMLResponse(content=render_html(ordens, filtros))


@app.get("/painel/ordens-servico/exportar/xlsx")
def exportar_ordens_xlsx(
    usuario: Optional[str] = Query(None),
    placa: Optional[str] = Query(None),
    cd_veiculo: Optional[str] = Query(None),
    try_out: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    query = db.query(OrdemServico)
    query = aplicar_filtros_ordens(query, usuario, placa, cd_veiculo, try_out)
    registros = query.order_by(OrdemServico.created_at.desc()).all()

    dados = []
    for item in registros:
        dados.append({
            "ID": item.id,
            "Usuário": item.usuario,
            "Filial Login": item.filial_login,
            "CD Empresa": item.cd_empresa,
            "CD Veículo": item.cd_veiculo,
            "Placa": item.placa,
            "DH Entrada": item.dh_entrada,
            "KM Entrada": item.km_entrada,
            "DH Saída": item.dh_saida,
            "KM Saída": item.km_saida,
            "DH Início": item.dh_inicio,
            "DH Prev": item.dh_prev,
            "CD Filial": item.cd_filial,
            "CD CCusto": item.cd_ccusto,
            "Observação": item.observacao,
            "CD Serviço": item.cd_servico,
            "CD Serviços": item.cd_servicos,
            "Try Out": item.try_out,
            "Criado em": item.created_at.strftime("%d/%m/%Y %H:%M") if item.created_at else "",
        })

    df = pd.DataFrame(dados)
    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="OrdensServico")

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=ordens_servico.xlsx"},
    )


@app.get("/painel/ordens-servico/exportar/json")
def exportar_ordens_json(
    usuario: Optional[str] = Query(None),
    placa: Optional[str] = Query(None),
    cd_veiculo: Optional[str] = Query(None),
    try_out: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    query = db.query(OrdemServico)
    query = aplicar_filtros_ordens(query, usuario, placa, cd_veiculo, try_out)
    registros = query.order_by(OrdemServico.created_at.desc()).all()

    return JSONResponse(content=[
        {
            "id": item.id,
            "usuario": item.usuario,
            "filial_login": item.filial_login,
            "cd_empresa": item.cd_empresa,
            "cd_veiculo": item.cd_veiculo,
            "placa": item.placa,
            "dh_entrada": item.dh_entrada,
            "km_entrada": item.km_entrada,
            "dh_saida": item.dh_saida,
            "km_saida": item.km_saida,
            "dh_inicio": item.dh_inicio,
            "dh_prev": item.dh_prev,
            "cd_filial": item.cd_filial,
            "cd_ccusto": item.cd_ccusto,
            "observacao": item.observacao,
            "cd_servico": item.cd_servico,
            "cd_servicos": item.cd_servicos.split(",") if item.cd_servicos else [],
            "try_out": item.try_out,
            "created_at": item.created_at.isoformat() if item.created_at else None,
            "updated_at": item.updated_at.isoformat() if item.updated_at else None,
        }
        for item in registros
    ])

# =========================================================
# ROTAS VEÍCULOS
# =========================================================

@app.get("/veiculos/placa/{placa}")
def buscar_veiculo_por_placa(placa: str, db: Session = Depends(get_db)):
    veiculo = buscar_veiculo_db(db, placa)

    if not veiculo:
        raise HTTPException(status_code=404, detail="Veículo não encontrado")

    return {
        "id": veiculo.id,
        "placa": veiculo.placa,
        "cd_veiculo": veiculo.cd_veiculo,
        "cd_filial": veiculo.cd_filial,
        "cd_ccusto": veiculo.cd_ccusto,
    }


@app.get("/veiculos")
def listar_veiculos(busca: Optional[str] = Query(None), db: Session = Depends(get_db)):
    query = db.query(VeiculoReferencia)

    if busca:
        query = query.filter(VeiculoReferencia.placa.ilike(f"%{busca}%"))

    veiculos = query.order_by(VeiculoReferencia.placa.asc()).all()

    return [
        {
            "id": item.id,
            "placa": item.placa,
            "cd_veiculo": item.cd_veiculo,
            "cd_filial": item.cd_filial,
            "cd_ccusto": item.cd_ccusto,
            "created_at": item.created_at.isoformat() if item.created_at else None,
        }
        for item in veiculos
    ]


@app.get("/painel/veiculos", response_class=HTMLResponse)
def painel_veiculos(busca: Optional[str] = Query(None), msg: Optional[str] = Query(None), db: Session = Depends(get_db)):
    query = db.query(VeiculoReferencia)

    if busca:
        query = query.filter(VeiculoReferencia.placa.ilike(f"%{busca}%"))

    veiculos = query.order_by(VeiculoReferencia.placa.asc()).all()

    return HTMLResponse(content=render_veiculos_html(veiculos, busca or "", msg or ""))


@app.post("/painel/veiculos/adicionar")
async def adicionar_veiculo(request: Request, db: Session = Depends(get_db)):
    form = await request.form()

    placa = to_str(form.get("placa")).upper()
    cd_veiculo = to_str(form.get("cd_veiculo"))
    cd_filial = to_str(form.get("cd_filial"))
    cd_ccusto = to_str(form.get("cd_ccusto"))

    if not placa or not cd_veiculo or not cd_filial or not cd_ccusto:
        raise HTTPException(status_code=400, detail="Todos os campos são obrigatórios")

    existente = buscar_veiculo_db(db, placa)

    if existente:
        existente.placa = placa
        existente.cd_veiculo = cd_veiculo
        existente.cd_filial = cd_filial
        existente.cd_ccusto = cd_ccusto
        mensagem = "Veículo atualizado com sucesso"
    else:
        db.add(VeiculoReferencia(
            placa=placa,
            cd_veiculo=cd_veiculo,
            cd_filial=cd_filial,
            cd_ccusto=cd_ccusto,
        ))
        mensagem = "Veículo cadastrado com sucesso"

    db.commit()

    return RedirectResponse(url=f"/painel/veiculos?msg={mensagem}", status_code=303)


@app.post("/painel/veiculos/excluir/{veiculo_id}")
def excluir_veiculo(veiculo_id: int, db: Session = Depends(get_db)):
    veiculo = db.query(VeiculoReferencia).filter(VeiculoReferencia.id == veiculo_id).first()

    if veiculo:
        db.delete(veiculo)
        db.commit()

    return RedirectResponse(url="/painel/veiculos?msg=Veículo excluído com sucesso", status_code=303)


@app.post("/painel/veiculos/importar-xlsx")
async def importar_veiculos_xlsx(arquivo: UploadFile = File(...), db: Session = Depends(get_db)):
    if not arquivo.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Envie um arquivo .xlsx")

    conteudo = await arquivo.read()

    try:
        df = pd.read_excel(BytesIO(conteudo))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao ler XLSX: {str(e)}")

    df.columns = [str(c).strip().lower() for c in df.columns]

    colunas_obrigatorias = {"placa", "cd_veiculo", "cd_filial", "cd_ccusto"}

    if not colunas_obrigatorias.issubset(set(df.columns)):
        raise HTTPException(status_code=400, detail="O XLSX precisa ter as colunas: placa, cd_veiculo, cd_filial, cd_ccusto")

    total_processado = 0
    total_criado = 0
    total_atualizado = 0

    for _, row in df.iterrows():
        placa = to_str(row.get("placa")).upper()
        cd_veiculo = to_str(row.get("cd_veiculo"))
        cd_filial = to_str(row.get("cd_filial"))
        cd_ccusto = to_str(row.get("cd_ccusto"))

        if not placa or not cd_veiculo or not cd_filial or not cd_ccusto:
            continue

        existente = buscar_veiculo_db(db, placa)

        if existente:
            existente.placa = placa
            existente.cd_veiculo = cd_veiculo
            existente.cd_filial = cd_filial
            existente.cd_ccusto = cd_ccusto
            total_atualizado += 1
        else:
            db.add(VeiculoReferencia(
                placa=placa,
                cd_veiculo=cd_veiculo,
                cd_filial=cd_filial,
                cd_ccusto=cd_ccusto,
            ))
            total_criado += 1

        total_processado += 1

    db.commit()

    msg = f"Importação concluída: {total_processado} processados, {total_criado} criados, {total_atualizado} atualizados"

    return RedirectResponse(url=f"/painel/veiculos?msg={msg}", status_code=303)

# =========================================================
# ROTAS USUÁRIOS
# =========================================================

@app.post("/usuarios/login")
async def login_usuario_app(request: Request, db: Session = Depends(get_db)):
    try:
        dados = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido")

    matricula = to_str(dados.get("matricula"))
    senha = to_str(dados.get("senha"))

    usuario = (
        db.query(UsuarioApp)
        .filter(UsuarioApp.matricula == matricula)
        .filter(UsuarioApp.senha == senha)
        .first()
    )

    if not usuario:
        raise HTTPException(status_code=401, detail="Matrícula ou senha inválida")

    return {
        "ok": True,
        "usuario": {
            "id": usuario.id,
            "matricula": usuario.matricula,
            "cpf": usuario.cpf,
            "nome_completo": usuario.nome_completo,
            "funcao": usuario.funcao,
        },
    }


@app.get("/usuarios")
def listar_usuarios(busca: Optional[str] = Query(None), db: Session = Depends(get_db)):
    query = db.query(UsuarioApp)

    if busca:
        query = query.filter(
            (UsuarioApp.matricula.ilike(f"%{busca}%")) |
            (UsuarioApp.nome_completo.ilike(f"%{busca}%"))
        )

    usuarios = query.order_by(UsuarioApp.nome_completo.asc()).all()

    return [
        {
            "id": item.id,
            "matricula": item.matricula,
            "cpf": item.cpf,
            "nome_completo": item.nome_completo,
            "funcao": item.funcao,
            "created_at": item.created_at.isoformat() if item.created_at else None,
        }
        for item in usuarios
    ]


@app.get("/painel/usuarios", response_class=HTMLResponse)
def painel_usuarios(busca: Optional[str] = Query(None), msg: Optional[str] = Query(None), db: Session = Depends(get_db)):
    query = db.query(UsuarioApp)

    if busca:
        query = query.filter(
            (UsuarioApp.matricula.ilike(f"%{busca}%")) |
            (UsuarioApp.nome_completo.ilike(f"%{busca}%"))
        )

    usuarios = query.order_by(UsuarioApp.nome_completo.asc()).all()

    return HTMLResponse(content=render_usuarios_html(usuarios, busca or "", msg or ""))


@app.post("/painel/usuarios/adicionar")
async def adicionar_usuario(request: Request, db: Session = Depends(get_db)):
    form = await request.form()

    matricula = to_str(form.get("matricula"))
    senha = to_str(form.get("senha"))
    cpf = to_str(form.get("cpf"))
    nome_completo = to_str(form.get("nome_completo"))
    funcao_usuario = to_str(form.get("funcao"))

    if not matricula or not senha or not nome_completo or not funcao_usuario:
        raise HTTPException(status_code=400, detail="Matrícula, senha, nome completo e função são obrigatórios")

    existente = db.query(UsuarioApp).filter(UsuarioApp.matricula == matricula).first()

    if existente:
        existente.senha = senha
        existente.cpf = cpf
        existente.nome_completo = nome_completo
        existente.funcao = funcao_usuario
        mensagem = "Usuário atualizado com sucesso"
    else:
        db.add(UsuarioApp(
            matricula=matricula,
            senha=senha,
            cpf=cpf,
            nome_completo=nome_completo,
            funcao=funcao_usuario,
        ))
        mensagem = "Usuário cadastrado com sucesso"

    db.commit()

    return RedirectResponse(url=f"/painel/usuarios?msg={mensagem}", status_code=303)


@app.post("/painel/usuarios/excluir/{usuario_id}")
def excluir_usuario(usuario_id: int, db: Session = Depends(get_db)):
    usuario = db.query(UsuarioApp).filter(UsuarioApp.id == usuario_id).first()

    if usuario:
        db.delete(usuario)
        db.commit()

    return RedirectResponse(url="/painel/usuarios?msg=Usuário excluído com sucesso", status_code=303)


@app.post("/painel/usuarios/importar-xlsx")
async def importar_usuarios_xlsx(arquivo: UploadFile = File(...), db: Session = Depends(get_db)):
    if not arquivo.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Envie um arquivo .xlsx")

    conteudo = await arquivo.read()

    try:
        df = pd.read_excel(BytesIO(conteudo))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao ler XLSX: {str(e)}")

    df.columns = [str(c).strip().lower() for c in df.columns]

    colunas_obrigatorias = {"matricula", "senha", "nome_completo", "funcao"}

    if not colunas_obrigatorias.issubset(set(df.columns)):
        raise HTTPException(status_code=400, detail="O XLSX precisa ter as colunas: matricula, senha, nome_completo, funcao. A coluna cpf é opcional.")

    total_processado = 0
    total_criado = 0
    total_atualizado = 0

    for _, row in df.iterrows():
        matricula = to_str(row.get("matricula"))
        senha = to_str(row.get("senha"))
        cpf = to_str(row.get("cpf"))
        nome_completo = to_str(row.get("nome_completo"))
        funcao_usuario = to_str(row.get("funcao"))

        if not matricula or not senha or not nome_completo or not funcao_usuario:
            continue

        existente = db.query(UsuarioApp).filter(UsuarioApp.matricula == matricula).first()

        if existente:
            existente.senha = senha
            existente.cpf = cpf
            existente.nome_completo = nome_completo
            existente.funcao = funcao_usuario
            total_atualizado += 1
        else:
            db.add(UsuarioApp(
                matricula=matricula,
                senha=senha,
                cpf=cpf,
                nome_completo=nome_completo,
                funcao=funcao_usuario,
            ))
            total_criado += 1

        total_processado += 1

    db.commit()

    msg = f"Importação concluída: {total_processado} processados, {total_criado} criados, {total_atualizado} atualizados"

    return RedirectResponse(url=f"/painel/usuarios?msg={msg}", status_code=303)
