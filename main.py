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

# 🔥 SUA API DE INTEGRAÇÃO (ALTERE AQUI SE NECESSÁRIO)
INTEGRACAO_FROTAWEB_URL = "https://SEU-SERVICO.onrender.com/frotaweb/os-corretiva"

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
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)

Base.metadata.create_all(bind=engine)

# =========================================================
# HELPERS
# =========================================================

def parse_datetime_local(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None

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

# =========================================================
# 🔥 NOVA ROTA INTEGRADA COM FROTAWEB
# =========================================================

@app.post("/integracoes/frotaweb/os-corretiva")
async def integrar_frotaweb(request: Request):
    try:
        dados = await request.json()
    except:
        raise HTTPException(status_code=400, detail="JSON inválido")

    try:
        resp = requests.post(
            INTEGRACAO_FROTAWEB_URL,
            json=dados,
            timeout=120
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao conectar na integração: {str(e)}")

    try:
        retorno = resp.json()
    except:
        raise HTTPException(status_code=500, detail="Erro ao interpretar resposta da integração")

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=retorno)

    return {
        "ok": True,
        "resumo": retorno.get("resumo"),
        "resumo_texto": retorno.get("resumo_texto"),
        "os": retorno.get("os"),
        "servicos": retorno.get("servicos")
    }

# =========================================================
# CRIAR ORDEM LOCAL + INTEGRAR
# =========================================================

@app.post("/ordens-servico")
async def criar_ordem_servico(request: Request, db: Session = Depends(get_db)):
    dados = await request.json()

    nova = OrdemServico(
        veiculo=dados.get("veiculo"),
        placa=dados.get("placa"),
        mecanico_nome=dados.get("mecanico_nome"),
        servicos_realizados=dados.get("servicos_realizados"),
        observacao=dados.get("observacao"),
        data_execucao=datetime.fromisoformat(dados.get("data_execucao").replace("Z", "+00:00"))
    )

    db.add(nova)
    db.commit()
    db.refresh(nova)

    # 🔥 ENVIA PARA FROTAWEB
    try:
        resp = requests.post(
            INTEGRACAO_FROTAWEB_URL,
            json=dados,
            timeout=120
        )
        retorno = resp.json()
    except Exception as e:
        return {
            "id": nova.id,
            "warning": "Salvo local, mas falhou integração",
            "erro": str(e)
        }

    return {
        "id": nova.id,
        "integracao": retorno
    }
