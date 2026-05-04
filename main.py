import os
from typing import Generator, Optional, Any

import requests
from fastapi import FastAPI, Depends, Request, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, func, text
from sqlalchemy.orm import sessionmaker, declarative_base, Session

# =========================================================
# CONFIG
# =========================================================

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL nao configurada no ambiente")

INTEGRACAO_FROTAWEB_URL = os.getenv(
    "INTEGRACAO_FROTAWEB_URL",
    "https://integracao-frotaweb.onrender.com/frotaweb/os-corretiva",
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

app = FastAPI(
    title="Painel O.S. Corretiva",
    version="1.0.0",
    description="Painel simples para receber O.S. do app, conferir e enviar manualmente ao FrotaWeb.",
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

    # Login/credenciais usadas no envio manual ao FrotaWeb
    usuario = Column(String(255), nullable=True)
    senha_frotaweb = Column(String(255), nullable=True)
    filial_login = Column(String(255), nullable=True)
    cd_empresa = Column(String(100), nullable=True)

    # Dados da O.S.
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

    # Controle do painel
    status_envio = Column(String(50), nullable=False, default="PENDENTE")
    retorno_envio = Column(Text, nullable=True)
    payload_original = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)


Base.metadata.create_all(bind=engine)


def garantir_colunas():
    """Garante colunas em bancos ja existentes no Render."""
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS usuario VARCHAR(255)"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS senha_frotaweb VARCHAR(255)"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS filial_login VARCHAR(255)"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS cd_empresa VARCHAR(100)"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS cd_veiculo VARCHAR(100)"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS placa VARCHAR(50)"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS dh_entrada VARCHAR(100)"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS km_entrada VARCHAR(100)"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS dh_saida VARCHAR(100)"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS km_saida VARCHAR(100)"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS dh_inicio VARCHAR(100)"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS dh_prev VARCHAR(100)"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS cd_filial VARCHAR(100)"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS cd_ccusto VARCHAR(100)"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS observacao TEXT"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS cd_servico VARCHAR(100)"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS cd_servicos TEXT"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS try_out VARCHAR(255)"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS status_envio VARCHAR(50) DEFAULT 'PENDENTE'"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS retorno_envio TEXT"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS payload_original TEXT"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE DEFAULT now()"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()"))
        conn.execute(text("ALTER TABLE ordens_servico ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP WITH TIME ZONE"))
        conn.execute(text("UPDATE ordens_servico SET status_envio = 'PENDENTE' WHERE status_envio IS NULL"))


garantir_colunas()

# =========================================================
# HELPERS
# =========================================================

def to_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text_value = str(value).strip()
    if text_value.lower() == "nan":
        return default
    return text_value


def normalizar_lista(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [to_str(v) for v in value if to_str(v)]
    text_value = to_str(value)
    if not text_value:
        return []
    if "," in text_value:
        return [item.strip() for item in text_value.split(",") if item.strip()]
    return [text_value]


def pick_value(dados: dict, *nomes: str, default: str = "") -> str:
    for nome in nomes:
        if "." in nome:
            grupo, chave = nome.split(".", 1)
            origem = dados.get(grupo) or {}
            valor = origem.get(chave) if isinstance(origem, dict) else None
        else:
            valor = dados.get(nome)
        text_value = to_str(valor)
        if text_value:
            return text_value
    return default


def montar_payload_painel(dados: dict) -> dict:
    """
    Aceita:
    - JSON do app Android: vehicle_code, plate, opening_datetime, odometer...
    - JSON ja mapeado do painel: cd_veiculo, placa, dh_entrada...
    - credenciais em credenciais.* ou credentials.*
    """

    usuario = pick_value(dados, "usuario", "credenciais.usuario", "credentials.usuario")
    senha = pick_value(dados, "senha", "credenciais.senha", "credentials.senha")
    filial_login = pick_value(dados, "filial_login", "credenciais.filial", "credentials.filial", "branch_code")
    cd_empresa = pick_value(dados, "cd_empresa", "empresa", "credenciais.empresa", "credentials.empresa", default="1")

    cd_veiculo = pick_value(dados, "cd_veiculo", "codigo_veiculo", "vehicle_code")
    placa = pick_value(dados, "placa", "plate").upper()

    dh_entrada = pick_value(dados, "dh_entrada", "data_hora_abertura", "opening_datetime")
    km_entrada = pick_value(dados, "km_entrada", "hodometro", "odometer")
    dh_saida = pick_value(dados, "dh_saida", "data_hora_saida", "exit_datetime")
    km_saida = pick_value(dados, "km_saida", "hodometro_saida", "exit_odometer", "odometer")
    dh_inicio = pick_value(dados, "dh_inicio", "data_hora_inicio", "start_datetime", "opening_datetime")
    dh_prev = pick_value(dados, "dh_prev", "data_hora_previsao_liberacao", "expected_release_datetime", "exit_datetime")

    cd_filial = pick_value(dados, "cd_filial", "codigo_filial", "branch_code", "filial")
    cd_ccusto = pick_value(dados, "cd_ccusto", "codigo_departamento", "department_code")

    observacao = pick_value(dados, "observacao", "observacoes", "observations")
    defeito = pick_value(dados, "descricao_defeito", "defect_description")
    if defeito:
        observacao = (observacao + " | " if observacao else "") + defeito

    cd_servico = pick_value(dados, "cd_servico", "codigo_servico", "service_code")
    cd_servicos = dados.get("cd_servicos")
    if cd_servicos is None:
        cd_servicos = dados.get("codigo_servicos")
    if cd_servicos is None and cd_servico:
        cd_servicos = [cd_servico]

    return {
        "usuario": usuario,
        "senha": senha,
        "filial_login": filial_login,
        "cd_empresa": cd_empresa,
        "cd_veiculo": cd_veiculo,
        "placa": placa,
        "dh_entrada": dh_entrada,
        "km_entrada": km_entrada,
        "dh_saida": dh_saida,
        "km_saida": km_saida,
        "dh_inicio": dh_inicio,
        "dh_prev": dh_prev,
        "cd_filial": cd_filial,
        "cd_ccusto": cd_ccusto,
        "observacao": observacao,
        "cd_servico": cd_servico,
        "cd_servicos": normalizar_lista(cd_servicos),
        "try_out": pick_value(dados, "try_out", default="PENDENTE_PAINEL"),
    }


def montar_payload_frotaweb(ordem: OrdemServico) -> dict:
    return {
        "usuario": to_str(ordem.usuario),
        "senha": to_str(ordem.senha_frotaweb),
        "filial_login": to_str(ordem.filial_login),
        "cd_empresa": to_str(ordem.cd_empresa),
        "cd_veiculo": to_str(ordem.cd_veiculo),
        "placa": to_str(ordem.placa).upper(),
        "dh_entrada": to_str(ordem.dh_entrada),
        "km_entrada": to_str(ordem.km_entrada),
        "dh_saida": to_str(ordem.dh_saida),
        "km_saida": to_str(ordem.km_saida),
        "dh_inicio": to_str(ordem.dh_inicio),
        "dh_prev": to_str(ordem.dh_prev),
        "cd_filial": to_str(ordem.cd_filial),
        "cd_ccusto": to_str(ordem.cd_ccusto),
        "observacao": to_str(ordem.observacao),
        "cd_servico": to_str(ordem.cd_servico),
        "cd_servicos": normalizar_lista(ordem.cd_servicos),
        "try_out": to_str(ordem.try_out),
    }


def enviar_para_frotaweb(payload: dict) -> dict:
    payload_api = dict(payload)
    payload_api.pop("try_out", None)

    try:
        response = requests.post(
            INTEGRACAO_FROTAWEB_URL,
            json=payload_api,
            timeout=120,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro ao conectar na API FrotaWeb: {str(exc)}")

    try:
        retorno = response.json()
    except Exception:
        retorno = {"raw": response.text}

    if response.status_code not in (200, 201):
        raise HTTPException(
            status_code=response.status_code,
            detail={"payload_enviado": payload_api, "retorno": retorno},
        )

    return retorno


def aplicar_filtros(query, usuario: Optional[str], placa: Optional[str], cd_veiculo: Optional[str], status_envio: Optional[str]):
    query = query.filter(OrdemServico.deleted_at.is_(None))
    if usuario:
        query = query.filter(OrdemServico.usuario.ilike(f"%{usuario}%"))
    if placa:
        query = query.filter(OrdemServico.placa.ilike(f"%{placa}%"))
    if cd_veiculo:
        query = query.filter(OrdemServico.cd_veiculo.ilike(f"%{cd_veiculo}%"))
    if status_envio:
        query = query.filter(OrdemServico.status_envio == status_envio)
    return query


def ordem_to_dict(ordem: OrdemServico) -> dict:
    return {
        "id": ordem.id,
        "usuario": ordem.usuario,
        "filial_login": ordem.filial_login,
        "cd_empresa": ordem.cd_empresa,
        "cd_veiculo": ordem.cd_veiculo,
        "placa": ordem.placa,
        "dh_entrada": ordem.dh_entrada,
        "km_entrada": ordem.km_entrada,
        "dh_saida": ordem.dh_saida,
        "km_saida": ordem.km_saida,
        "dh_inicio": ordem.dh_inicio,
        "dh_prev": ordem.dh_prev,
        "cd_filial": ordem.cd_filial,
        "cd_ccusto": ordem.cd_ccusto,
        "observacao": ordem.observacao,
        "cd_servico": ordem.cd_servico,
        "cd_servicos": normalizar_lista(ordem.cd_servicos),
        "try_out": ordem.try_out,
        "status_envio": ordem.status_envio,
        "retorno_envio": ordem.retorno_envio,
        "created_at": ordem.created_at.isoformat() if ordem.created_at else None,
        "updated_at": ordem.updated_at.isoformat() if ordem.updated_at else None,
    }

# =========================================================
# HTML
# =========================================================

def html_base(titulo: str, corpo: str, mensagem: str = "") -> str:
    return f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{titulo}</title>
        <style>
            body {{ font-family: Arial, sans-serif; background:#f3f4f6; margin:0; padding:24px; color:#111827; }}
            .container {{ max-width:1600px; margin:0 auto; }}
            .topbar {{ display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; }}
            .card {{ background:#fff; border-radius:14px; padding:18px; box-shadow:0 2px 12px rgba(0,0,0,.08); margin-bottom:18px; }}
            .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:12px; align-items:end; }}
            label {{ display:block; font-weight:700; margin-bottom:6px; color:#374151; font-size:14px; }}
            input, select {{ width:100%; box-sizing:border-box; padding:10px; border:1px solid #d1d5db; border-radius:8px; }}
            button, .btn {{ background:#2563eb; color:white; border:0; border-radius:8px; padding:10px 14px; cursor:pointer; text-decoration:none; display:inline-block; font-size:14px; }}
            .btn-green {{ background:#059669; }}
            .btn-red {{ background:#dc2626; }}
            .btn-gray {{ background:#4b5563; }}
            .btn-orange {{ background:#ea580c; }}
            .acoes {{ display:flex; gap:8px; flex-wrap:wrap; margin-top:14px; }}
            table {{ width:100%; border-collapse:collapse; background:white; }}
            th, td {{ padding:10px; border-bottom:1px solid #e5e7eb; text-align:left; vertical-align:top; font-size:13px; }}
            th {{ background:#111827; color:white; position:sticky; top:0; }}
            tr:hover {{ background:#f9fafb; }}
            .msg {{ background:#ecfdf5; color:#065f46; padding:12px; border-radius:8px; margin-bottom:14px; font-weight:700; }}
            .status-pendente {{ color:#b45309; font-weight:800; }}
            .status-enviada {{ color:#047857; font-weight:800; }}
            .status-erro {{ color:#b91c1c; font-weight:800; }}
            .small {{ color:#6b7280; font-size:12px; }}
            .retorno {{ max-width:260px; white-space:normal; overflow-wrap:anywhere; color:#374151; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="topbar">
                <div>
                    <h1>{titulo}</h1>
                    <div class="small">App salva no painel. Envio ao FrotaWeb somente pelo botao Enviar FrotaWeb.</div>
                </div>
                <a class="btn btn-gray" href="/docs">Docs</a>
            </div>
            {f'<div class="msg">{mensagem}</div>' if mensagem else ''}
            {corpo}
        </div>
    </body>
    </html>
    """


def render_painel_ordens(ordens: list[OrdemServico], filtros: dict, mensagem: str = "") -> str:
    linhas = ""
    for ordem in ordens:
        status = ordem.status_envio or "PENDENTE"
        status_class = "status-pendente"
        if status == "ENVIADA":
            status_class = "status-enviada"
        elif status.startswith("ERRO"):
            status_class = "status-erro"

        retorno = ordem.retorno_envio or ""
        if len(retorno) > 250:
            retorno = retorno[:250] + "..."

        if status == "ENVIADA":
            acao = "<span class='status-enviada'>Enviada</span>"
        else:
            acao = f"""
            <form method="post" action="/painel/ordens-servico/enviar/{ordem.id}"
                  onsubmit="return confirm('Enviar a O.S. {ordem.id} para o FrotaWeb agora?');">
                <button class="btn-green" type="submit">Enviar FrotaWeb</button>
            </form>
            """

        linhas += f"""
        <tr>
            <td>{ordem.id}</td>
            <td><span class="{status_class}">{status}</span></td>
            <td>{ordem.usuario or ""}</td>
            <td>{ordem.placa or ""}</td>
            <td>{ordem.cd_veiculo or ""}</td>
            <td>{ordem.cd_filial or ""}</td>
            <td>{ordem.cd_ccusto or ""}</td>
            <td>{ordem.km_entrada or ""}</td>
            <td>{ordem.dh_entrada or ""}</td>
            <td>{ordem.dh_saida or ""}</td>
            <td>{ordem.observacao or ""}</td>
            <td class="retorno">{retorno}</td>
            <td>{ordem.created_at.strftime("%d/%m/%Y %H:%M") if ordem.created_at else ""}</td>
            <td>{acao}</td>
        </tr>
        """

    corpo = f"""
    <div class="card">
        <form method="get" action="/painel/ordens-servico">
            <div class="grid">
                <div>
                    <label>Status</label>
                    <select name="status_envio">
                        <option value="" {"selected" if not filtros.get("status_envio") else ""}>Todos</option>
                        <option value="PENDENTE" {"selected" if filtros.get("status_envio") == "PENDENTE" else ""}>Pendente</option>
                        <option value="ENVIADA" {"selected" if filtros.get("status_envio") == "ENVIADA" else ""}>Enviada</option>
                        <option value="ERRO_ENVIO" {"selected" if filtros.get("status_envio") == "ERRO_ENVIO" else ""}>Erro</option>
                    </select>
                </div>
                <div><label>Usuario</label><input name="usuario" value="{filtros.get("usuario", "")}"></div>
                <div><label>Placa</label><input name="placa" value="{filtros.get("placa", "")}"></div>
                <div><label>Codigo veiculo</label><input name="cd_veiculo" value="{filtros.get("cd_veiculo", "")}"></div>
            </div>
            <div class="acoes">
                <button type="submit">Filtrar</button>
                <a class="btn btn-gray" href="/painel/ordens-servico">Limpar</a>
                <a class="btn btn-orange" href="/ordens-servico">Ver JSON</a>
            </div>
        </form>
    </div>

    <div class="card" style="padding:0; overflow:auto;">
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Status</th>
                    <th>Usuario</th>
                    <th>Placa</th>
                    <th>CD Veiculo</th>
                    <th>CD Filial</th>
                    <th>CD Custo</th>
                    <th>KM</th>
                    <th>Entrada</th>
                    <th>Saida</th>
                    <th>Observacao</th>
                    <th>Retorno</th>
                    <th>Criado em</th>
                    <th>Acao</th>
                </tr>
            </thead>
            <tbody>{linhas if linhas else '<tr><td colspan="14">Nenhuma O.S. encontrada</td></tr>'}</tbody>
        </table>
    </div>
    """

    return html_base("Painel de O.S. Corretiva", corpo, mensagem)

# =========================================================
# ROTAS
# =========================================================

@app.get("/")
def home():
    return {
        "status": "online",
        "painel": "/painel/ordens-servico",
        "receber_os_app": "POST /panel/os",
        "receber_os_api": "POST /ordens-servico",
        "enviar_frotaweb": "POST /painel/ordens-servico/enviar/{ordem_id}",
        "docs": "/docs",
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ordens-servico")
async def criar_ordem_servico(request: Request, db: Session = Depends(get_db)):
    """
    Salva a O.S. no painel com status PENDENTE.
    Nao envia automaticamente para o FrotaWeb.
    """
    try:
        dados = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON invalido")

    payload = montar_payload_painel(dados)

    if not payload["placa"] and not payload["cd_veiculo"]:
        raise HTTPException(status_code=400, detail="Informe placa ou cd_veiculo")

    nova = OrdemServico(
        usuario=payload["usuario"],
        senha_frotaweb=payload["senha"],
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
        status_envio="PENDENTE",
        retorno_envio="O.S. salva no painel. Aguardando conferencia e envio manual ao FrotaWeb.",
        payload_original=str(dados),
    )

    db.add(nova)
    db.commit()
    db.refresh(nova)

    return {
        "ok": True,
        "created": True,
        "accepted": True,
        "id": nova.id,
        "panel_id": str(nova.id),
        "order_number": str(nova.id),
        "status_envio": nova.status_envio,
        "message": "O.S. salva no painel para conferencia. Nao foi enviada ao FrotaWeb.",
        "painel": "/painel/ordens-servico",
    }


@app.post("/panel/os")
async def criar_ordem_servico_app(request: Request, db: Session = Depends(get_db)):
    """
    Rota para o app Android.
    O app deve chamar POST /panel/os.
    """
    return await criar_ordem_servico(request=request, db=db)


@app.get("/ordens-servico")
def listar_ordens_servico(
    usuario: Optional[str] = Query(None),
    placa: Optional[str] = Query(None),
    cd_veiculo: Optional[str] = Query(None),
    status_envio: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    query = aplicar_filtros(db.query(OrdemServico), usuario, placa, cd_veiculo, status_envio)
    registros = query.order_by(OrdemServico.created_at.desc()).all()
    return [ordem_to_dict(item) for item in registros]


@app.get("/painel/ordens-servico", response_class=HTMLResponse)
def painel_ordens_servico(
    usuario: Optional[str] = Query(None),
    placa: Optional[str] = Query(None),
    cd_veiculo: Optional[str] = Query(None),
    status_envio: Optional[str] = Query(None),
    msg: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    query = aplicar_filtros(db.query(OrdemServico), usuario, placa, cd_veiculo, status_envio)
    ordens = query.order_by(OrdemServico.created_at.desc()).all()

    filtros = {
        "usuario": usuario or "",
        "placa": placa or "",
        "cd_veiculo": cd_veiculo or "",
        "status_envio": status_envio or "",
    }

    return HTMLResponse(render_painel_ordens(ordens, filtros, msg or ""))


@app.post("/painel/ordens-servico/enviar/{ordem_id}")
def enviar_ordem_para_frotaweb(ordem_id: int, db: Session = Depends(get_db)):
    ordem = (
        db.query(OrdemServico)
        .filter(OrdemServico.id == ordem_id)
        .filter(OrdemServico.deleted_at.is_(None))
        .first()
    )

    if not ordem:
        raise HTTPException(status_code=404, detail="O.S. nao encontrada")

    payload = montar_payload_frotaweb(ordem)

    try:
        retorno = enviar_para_frotaweb(payload)
    except HTTPException as exc:
        ordem.status_envio = "ERRO_ENVIO"
        ordem.retorno_envio = str(exc.detail)
        db.commit()
        return RedirectResponse(
            url=f"/painel/ordens-servico?msg=Erro ao enviar O.S. {ordem.id} ao FrotaWeb",
            status_code=303,
        )

    ordem.status_envio = "ENVIADA"
    ordem.retorno_envio = str(retorno)
    db.commit()

    return RedirectResponse(
        url=f"/painel/ordens-servico?msg=O.S. {ordem.id} enviada ao FrotaWeb com sucesso",
        status_code=303,
    )


@app.post("/ordens-servico/{ordem_id}/enviar-frotaweb")
def enviar_ordem_para_frotaweb_json(ordem_id: int, db: Session = Depends(get_db)):
    ordem = (
        db.query(OrdemServico)
        .filter(OrdemServico.id == ordem_id)
        .filter(OrdemServico.deleted_at.is_(None))
        .first()
    )

    if not ordem:
        raise HTTPException(status_code=404, detail="O.S. nao encontrada")

    payload = montar_payload_frotaweb(ordem)
    retorno = enviar_para_frotaweb(payload)

    ordem.status_envio = "ENVIADA"
    ordem.retorno_envio = str(retorno)
    db.commit()

    return {
        "ok": True,
        "id": ordem.id,
        "status_envio": ordem.status_envio,
        "payload_enviado": payload,
        "retorno_integracao": retorno,
    }


@app.delete("/ordens-servico/{ordem_id}")
def excluir_ordem(ordem_id: int, db: Session = Depends(get_db)):
    ordem = db.query(OrdemServico).filter(OrdemServico.id == ordem_id).first()
    if not ordem:
        raise HTTPException(status_code=404, detail="O.S. nao encontrada")

    ordem.deleted_at = func.now()
    db.commit()
    return {"ok": True, "message": f"O.S. {ordem_id} removida do painel"}
