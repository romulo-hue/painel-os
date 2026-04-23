import os
from io import BytesIO
from datetime import datetime
from typing import Optional

import pandas as pd
import requests
from fastapi import FastAPI, Depends, Query, Request, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, func
from sqlalchemy.orm import sessionmaker, declarative_base, Session

# =========================================================
# CONFIG
# =========================================================

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não configurada no ambiente")

# URL da API externa
INTEGRACAO_OUTRA_API_URL = os.getenv(
    "INTEGRACAO_OUTRA_API_URL",
    "https://sua-outra-api.onrender.com/os-corretiva",
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
# MODEL
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
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    deleted_at = Column(DateTime(timezone=True), nullable=True)

# =========================================================
# INIT DB
# =========================================================

Base.metadata.create_all(bind=engine)

# =========================================================
# HTML TEMPLATE INLINE
# =========================================================

def render_html(ordens, filtros):
    linhas = ""

    for item in ordens:
        linhas += f'''
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
        '''

    html = f'''
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Painel de Ordens de Serviço</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                background: #f4f6f9;
                margin: 0;
                padding: 24px;
            }}
            .container {{
                max-width: 1600px;
                margin: 0 auto;
            }}
            h1 {{
                margin-bottom: 20px;
                color: #1f2937;
            }}
            .card {{
                background: white;
                border-radius: 12px;
                padding: 20px;
                box-shadow: 0 2px 12px rgba(0,0,0,0.08);
                margin-bottom: 20px;
            }}
            .filtros {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
                gap: 12px;
                align-items: end;
            }}
            label {{
                font-size: 14px;
                color: #374151;
                display: block;
                margin-bottom: 6px;
            }}
            input {{
                width: 100%;
                padding: 10px;
                border: 1px solid #d1d5db;
                border-radius: 8px;
                box-sizing: border-box;
            }}
            .acoes {{
                display: flex;
                gap: 10px;
                flex-wrap: wrap;
                margin-top: 16px;
            }}
            button, .btn {{
                background: #2563eb;
                color: white;
                border: none;
                padding: 10px 16px;
                border-radius: 8px;
                cursor: pointer;
                text-decoration: none;
                display: inline-block;
                font-size: 14px;
            }}
            .btn-secondary {{
                background: #059669;
            }}
            .btn-json {{
                background: #7c3aed;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                background: white;
                border-radius: 12px;
                overflow: hidden;
            }}
            th, td {{
                padding: 12px;
                text-align: left;
                border-bottom: 1px solid #e5e7eb;
                vertical-align: top;
                font-size: 14px;
            }}
            th {{
                background: #111827;
                color: white;
            }}
            tr:hover {{
                background: #f9fafb;
            }}
            .topbar {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 18px;
                flex-wrap: wrap;
                gap: 12px;
            }}
            .badge {{
                background: #e0f2fe;
                color: #0369a1;
                padding: 8px 12px;
                border-radius: 999px;
                font-size: 14px;
                font-weight: bold;
            }}
            @media (max-width: 768px) {{
                body {{
                    padding: 12px;
                }}
                th, td {{
                    font-size: 12px;
                    padding: 8px;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="topbar">
                <h1>Painel de Ordens de Serviço</h1>
                <div class="badge">Total de registros: {len(ordens)}</div>
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
    '''
    return html

# =========================================================
# HELPERS
# =========================================================

def aplicar_filtros(
    query,
    usuario: Optional[str],
    placa: Optional[str],
    cd_veiculo: Optional[str],
    try_out: Optional[str],
):
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


def to_str(value, default=""):
    if value is None:
        return default
    return str(value).strip()


def normalize_cd_servicos(value):
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [str(value).strip()]


def montar_payload(dados: dict) -> dict:
    return {
        "usuario": to_str(dados.get("usuario")),
        "senha": to_str(dados.get("senha")),
        "filial_login": to_str(dados.get("filial_login")),
        "cd_empresa": to_str(dados.get("cd_empresa")),
        "cd_veiculo": to_str(dados.get("cd_veiculo")),
        "placa": to_str(dados.get("placa")),
        "dh_entrada": to_str(dados.get("dh_entrada")),
        "km_entrada": to_str(dados.get("km_entrada")),
        "dh_saida": to_str(dados.get("dh_saida")),
        "km_saida": to_str(dados.get("km_saida")),
        "dh_inicio": to_str(dados.get("dh_inicio")),
        "dh_prev": to_str(dados.get("dh_prev")),
        "cd_filial": to_str(dados.get("cd_filial")),
        "cd_ccusto": to_str(dados.get("cd_ccusto")),
        "observacao": dados.get("observacao", ""),
        "cd_servico": to_str(dados.get("cd_servico")),
        "cd_servicos": normalize_cd_servicos(dados.get("cd_servicos")),
        "try_out": to_str(dados.get("try_out")),
    }


def enviar_para_outra_api(payload: dict):
    try:
        resp = requests.post(
            INTEGRACAO_OUTRA_API_URL,
            json=payload,
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
                "payload_enviado": payload,
                "retorno": retorno,
            }
        )

    return retorno

# =========================================================
# ROTAS
# =========================================================

@app.get("/")
def home():
    return {
        "message": "API de Ordens de Serviço online",
        "painel": "/painel/ordens-servico",
        "docs": "/docs",
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ordens-servico")
async def criar_ordem_servico(request: Request, db: Session = Depends(get_db)):
    try:
        dados = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido")

    payload = montar_payload(dados)

    try:
        retorno_integracao = enviar_para_outra_api(payload)
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)

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
    query = aplicar_filtros(query, usuario, placa, cd_veiculo, try_out)

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
    query = aplicar_filtros(query, usuario, placa, cd_veiculo, try_out)
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
    query = aplicar_filtros(query, usuario, placa, cd_veiculo, try_out)
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
    query = aplicar_filtros(query, usuario, placa, cd_veiculo, try_out)
    registros = query.order_by(OrdemServico.created_at.desc()).all()

    dados = []
    for item in registros:
        dados.append({
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
        })

    return JSONResponse(content=dados)
