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

# URL da API externa que recebe a OS no novo formato
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
    veiculo = Column(String(255), nullable=False)
    placa = Column(String(50), nullable=True)
    mecanico_nome = Column(String(255), nullable=False)
    servicos_realizados = Column(Text, nullable=True)
    observacao = Column(Text, nullable=True)
    data_execucao = Column(DateTime(timezone=True), nullable=False)

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
            <td>{item.veiculo or ""}</td>
            <td>{item.placa or ""}</td>
            <td>{item.mecanico_nome or ""}</td>
            <td>{item.servicos_realizados or ""}</td>
            <td>{item.observacao or ""}</td>
            <td>{item.data_execucao.strftime("%d/%m/%Y %H:%M") if item.data_execucao else ""}</td>
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
                max-width: 1400px;
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
                            <label>Data início</label>
                            <input type="datetime-local" name="data_inicio" value="{filtros.get("data_inicio", "")}">
                        </div>
                        <div>
                            <label>Data fim</label>
                            <input type="datetime-local" name="data_fim" value="{filtros.get("data_fim", "")}">
                        </div>
                        <div>
                            <label>Mecânico</label>
                            <input type="text" name="mecanico_nome" value="{filtros.get("mecanico_nome", "")}">
                        </div>
                        <div>
                            <label>Placa</label>
                            <input type="text" name="placa" value="{filtros.get("placa", "")}">
                        </div>
                    </div>

                    <div class="acoes">
                        <button type="submit">Filtrar</button>
                        <a class="btn btn-secondary" href="/painel/ordens-servico/exportar/xlsx?data_inicio={filtros.get("data_inicio_iso", "")}&data_fim={filtros.get("data_fim_iso", "")}&mecanico_nome={filtros.get("mecanico_nome", "")}&placa={filtros.get("placa", "")}">Exportar XLSX</a>
                        <a class="btn btn-json" href="/painel/ordens-servico/exportar/json?data_inicio={filtros.get("data_inicio_iso", "")}&data_fim={filtros.get("data_fim_iso", "")}&mecanico_nome={filtros.get("mecanico_nome", "")}&placa={filtros.get("placa", "")}">Exportar JSON</a>
                    </div>
                </form>
            </div>

            <div class="card" style="padding:0; overflow:auto;">
                <table>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Veículo</th>
                            <th>Placa</th>
                            <th>Mecânico</th>
                            <th>Serviços Realizados</th>
                            <th>Observação</th>
                            <th>Data Execução</th>
                            <th>Criado em</th>
                        </tr>
                    </thead>
                    <tbody>
                        {linhas if linhas else '<tr><td colspan="8">Nenhum registro encontrado</td></tr>'}
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

def parse_datetime_local(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None

    formatos = [
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]

    for fmt in formatos:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def aplicar_filtros(
    query,
    data_inicio: Optional[datetime],
    data_fim: Optional[datetime],
    mecanico_nome: Optional[str],
    placa: Optional[str],
):
    query = query.filter(OrdemServico.deleted_at.is_(None))

    if data_inicio:
        query = query.filter(OrdemServico.data_execucao >= data_inicio)

    if data_fim:
        query = query.filter(OrdemServico.data_execucao <= data_fim)

    if mecanico_nome:
        query = query.filter(OrdemServico.mecanico_nome.ilike(f"%{mecanico_nome}%"))

    if placa:
        query = query.filter(OrdemServico.placa.ilike(f"%{placa}%"))

    return query


def to_str(value, default=""):
    if value is None:
        return default
    return str(value).strip()


def montar_payload_outra_api(dados: dict) -> dict:
    payload = dados.get("payload_outra_api") or {}

    # aceita try_out tanto na raiz quanto dentro do payload_outra_api
    try_out = dados.get("try_out", payload.get("try_out", ""))

    return {
        "usuario": to_str(payload.get("usuario")),
        "senha": to_str(payload.get("senha")),
        "filial_login": to_str(payload.get("filial_login")),
        "cd_empresa": to_str(payload.get("cd_empresa")),
        "cd_veiculo": to_str(payload.get("cd_veiculo")),
        "placa": to_str(payload.get("placa", dados.get("placa"))),
        "dh_entrada": to_str(payload.get("dh_entrada")),
        "km_entrada": to_str(payload.get("km_entrada")),
        "dh_saida": to_str(payload.get("dh_saida")),
        "km_saida": to_str(payload.get("km_saida")),
        "dh_inicio": to_str(payload.get("dh_inicio")),
        "dh_prev": to_str(payload.get("dh_prev")),
        "cd_filial": to_str(payload.get("cd_filial")),
        "cd_ccusto": to_str(payload.get("cd_ccusto")),
        "observacao": to_str(payload.get("observacao", dados.get("observacao", ""))),
        "cd_servico": to_str(payload.get("cd_servico")),
        "cd_servicos": payload.get("cd_servicos", []),
        "try_out": try_out,
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
                "message": "Erro retornado pela outra API",
                "payload_enviado": payload,
                "retorno": retorno,
            },
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


@app.post("/integracoes/outra-api/os-corretiva")
async def integrar_outra_api(request: Request):
    try:
        dados = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido")

    payload = montar_payload_outra_api(dados)
    retorno = enviar_para_outra_api(payload)

    return {
        "ok": True,
        "payload_enviado": payload,
        "retorno_integracao": retorno,
    }


@app.post("/ordens-servico")
async def criar_ordem_servico(request: Request, db: Session = Depends(get_db)):
    try:
        dados = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido")

    veiculo = to_str(dados.get("veiculo"))
    placa = to_str(dados.get("placa"))
    mecanico_nome = to_str(dados.get("mecanico_nome"))
    servicos_realizados = to_str(dados.get("servicos_realizados"))
    observacao = to_str(dados.get("observacao"))
    data_execucao_raw = dados.get("data_execucao")

    if not veiculo:
        raise HTTPException(status_code=400, detail="Campo veiculo é obrigatório")

    if not mecanico_nome:
        raise HTTPException(status_code=400, detail="Campo mecanico_nome é obrigatório")

    if not data_execucao_raw:
        raise HTTPException(status_code=400, detail="Campo data_execucao é obrigatório")

    try:
        data_execucao = datetime.fromisoformat(str(data_execucao_raw).replace("Z", "+00:00"))
    except Exception:
        raise HTTPException(status_code=400, detail="data_execucao inválida")

    nova = OrdemServico(
        veiculo=veiculo,
        placa=placa,
        mecanico_nome=mecanico_nome,
        servicos_realizados=servicos_realizados,
        observacao=observacao,
        data_execucao=data_execucao,
    )

    db.add(nova)
    db.commit()
    db.refresh(nova)

    enviar_integracao = dados.get("enviar_para_outra_api", False)
    payload_outra_api = dados.get("payload_outra_api")

    if enviar_integracao or payload_outra_api:
        payload = montar_payload_outra_api(dados)

        try:
            retorno_integracao = enviar_para_outra_api(payload)
            return {
                "id": nova.id,
                "message": "Ordem de serviço criada com sucesso e enviada para a outra API",
                "payload_enviado": payload,
                "integracao": retorno_integracao,
            }
        except HTTPException as e:
            return {
                "id": nova.id,
                "message": "Ordem criada localmente, mas a integração falhou",
                "payload_enviado": payload,
                "erro_integracao": e.detail,
            }

    return {
        "id": nova.id,
        "message": "Ordem de serviço criada com sucesso",
    }


@app.get("/ordens-servico")
def listar_ordens_servico(
    data_inicio: Optional[str] = Query(None),
    data_fim: Optional[str] = Query(None),
    mecanico_nome: Optional[str] = Query(None),
    placa: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    dt_inicio = parse_datetime_local(data_inicio)
    dt_fim = parse_datetime_local(data_fim)

    query = db.query(OrdemServico)
    query = aplicar_filtros(query, dt_inicio, dt_fim, mecanico_nome, placa)

    registros = query.order_by(OrdemServico.data_execucao.desc()).all()

    return [
        {
            "id": item.id,
            "veiculo": item.veiculo,
            "placa": item.placa,
            "mecanico_nome": item.mecanico_nome,
            "servicos_realizados": item.servicos_realizados,
            "observacao": item.observacao,
            "data_execucao": item.data_execucao.isoformat() if item.data_execucao else None,
            "created_at": item.created_at.isoformat() if item.created_at else None,
            "updated_at": item.updated_at.isoformat() if item.updated_at else None,
        }
        for item in registros
    ]


@app.get("/painel/ordens-servico", response_class=HTMLResponse)
def painel_ordens_servico(
    data_inicio: Optional[str] = Query(None),
    data_fim: Optional[str] = Query(None),
    mecanico_nome: Optional[str] = Query(None),
    placa: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    dt_inicio = parse_datetime_local(data_inicio)
    dt_fim = parse_datetime_local(data_fim)

    query = db.query(OrdemServico)
    query = aplicar_filtros(query, dt_inicio, dt_fim, mecanico_nome, placa)
    ordens = query.order_by(OrdemServico.data_execucao.desc()).all()

    filtros = {
        "data_inicio": data_inicio or "",
        "data_fim": data_fim or "",
        "data_inicio_iso": data_inicio or "",
        "data_fim_iso": data_fim or "",
        "mecanico_nome": mecanico_nome or "",
        "placa": placa or "",
    }

    return HTMLResponse(content=render_html(ordens, filtros))


@app.get("/painel/ordens-servico/exportar/xlsx")
def exportar_ordens_xlsx(
    data_inicio: Optional[str] = Query(None),
    data_fim: Optional[str] = Query(None),
    mecanico_nome: Optional[str] = Query(None),
    placa: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    dt_inicio = parse_datetime_local(data_inicio)
    dt_fim = parse_datetime_local(data_fim)

    query = db.query(OrdemServico)
    query = aplicar_filtros(query, dt_inicio, dt_fim, mecanico_nome, placa)
    registros = query.order_by(OrdemServico.data_execucao.desc()).all()

    dados = []
    for item in registros:
        dados.append({
            "ID": item.id,
            "Veículo": item.veiculo,
            "Placa": item.placa,
            "Mecânico": item.mecanico_nome,
            "Serviços Realizados": item.servicos_realizados,
            "Observação": item.observacao,
            "Data Execução": item.data_execucao.strftime("%d/%m/%Y %H:%M") if item.data_execucao else "",
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
    data_inicio: Optional[str] = Query(None),
    data_fim: Optional[str] = Query(None),
    mecanico_nome: Optional[str] = Query(None),
    placa: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    dt_inicio = parse_datetime_local(data_inicio)
    dt_fim = parse_datetime_local(data_fim)

    query = db.query(OrdemServico)
    query = aplicar_filtros(query, dt_inicio, dt_fim, mecanico_nome, placa)
    registros = query.order_by(OrdemServico.data_execucao.desc()).all()

    dados = []
    for item in registros:
        dados.append({
            "id": item.id,
            "veiculo": item.veiculo,
            "placa": item.placa,
            "mecanico_nome": item.mecanico_nome,
            "servicos_realizados": item.servicos_realizados,
            "observacao": item.observacao,
            "data_execucao": item.data_execucao.isoformat() if item.data_execucao else None,
            "created_at": item.created_at.isoformat() if item.created_at else None,
            "updated_at": item.updated_at.isoformat() if item.updated_at else None,
        })

    return JSONResponse(content=dados)
