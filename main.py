"""
VJ Trading Dashboard — FastAPI v3
"""
from fastapi import FastAPI, Depends, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session as DBSession
import json, os, base64

from database import engine, get_db, Base
from models import Session as SessionModel
from schemas import SessionCreate, TokenRequest, TokenResponse, MessageResponse
from auth import authenticate_user, create_access_token, get_current_user, require_admin
from csv_parser import parse_orderbook_csv
from seed import seed

Base.metadata.create_all(bind=engine)

# ── COLUMN MIGRATIONS (safe ADD COLUMN for existing deployments) ──
def _migrate():
    """Add new columns to existing sessions table without dropping data."""
    new_cols = [
        ("journal",                "TEXT"),
        ("strikes_json",           "TEXT DEFAULT '[]'"),
        ("charges_breakdown_json", "TEXT DEFAULT '{}'"),
        ("time_pnl_json",          "TEXT DEFAULT '{}'"),
        ("journal_charts_json",    "TEXT DEFAULT '[]'"),
    ]
    with engine.connect() as conn:
        for col, definition in new_cols:
            try:
                conn.execute(__import__("sqlalchemy").text(
                    f"ALTER TABLE sessions ADD COLUMN {col} {definition}"
                ))
                conn.commit()
            except Exception:
                pass  # column already exists — ignore

_migrate()
seed()

app = FastAPI(title="VJ Trading Dashboard", version="3.0.0",
              docs_url="/api/docs", openapi_url="/api/openapi.json")

app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

@app.get("/", include_in_schema=False)
def serve_dashboard():
    """Serve the dashboard frontend. index.html is the single source of truth."""
    for candidate in [
        os.path.join(STATIC_DIR, "index.html"),
        os.path.join(BASE_DIR, "index.html"),
        os.path.join(BASE_DIR, "public", "index.html"),
    ]:
        if os.path.exists(candidate):
            return FileResponse(candidate, media_type="text/html")
    raise HTTPException(
        status_code=500,
        detail="index.html not deployed. Ensure it sits next to main.py in the build output."
    )

@app.get("/health", tags=["Health"])
def health():
    return {"status": "ok"}

# ── AUTH ──
@app.post("/auth/token", response_model=TokenResponse, tags=["Auth"])
def login(body: TokenRequest):
    user = authenticate_user(body.username, body.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    token = create_access_token({"sub": user["username"], "role": user["role"]})
    return {"access_token": token}

@app.get("/auth/me", tags=["Auth"])
def get_me(user: dict = Depends(get_current_user)):
    return {"username": user["username"], "role": user["role"]}

# ── SESSIONS ──
@app.get("/sessions", tags=["Sessions"])
def get_sessions(db: DBSession = Depends(get_db), user: dict = Depends(get_current_user)):
    rows = db.query(SessionModel).order_by(SessionModel.id).all()
    return [_row_to_dict(r) for r in rows]

@app.get("/sessions/{session_id}", tags=["Sessions"])
def get_session(session_id: str, db: DBSession = Depends(get_db), user: dict = Depends(get_current_user)):
    row = db.query(SessionModel).filter(SessionModel.id == session_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")
    return _row_to_dict(row)

@app.post("/sessions", tags=["Sessions"])
def create_or_update_session(body: SessionCreate, db: DBSession = Depends(get_db),
                              user: dict = Depends(require_admin)):
    existing = db.query(SessionModel).filter(SessionModel.id == body.id).first()
    data = {
        "id": body.id, "date": body.date, "full": body.full,
        "index_name": body.index_name, "dte": body.dte,
        "vix": body.vix, "capital": body.capital,
        "gross_pnl": body.gross_pnl, "net_pnl": body.net_pnl,
        "net_roi": body.net_roi, "gross_roi": body.gross_roi,
        "ce_pnl": body.ce_pnl, "pe_pnl": body.pe_pnl,
        "charges": body.charges, "executed": body.executed,
        "rejected": body.rejected, "mt": body.mt,
        "carry_out": body.carry_out, "note": body.note,
        "journal": body.journal,
        "peer_rois_json":   json.dumps(body.peer_rois),
        "scores_json":      json.dumps(body.scores.dict()),
        "violations_json":  json.dumps(body.violations),
        "strengths_json":   json.dumps(body.strengths),
    }
    if existing:
        for k, v in data.items():
            setattr(existing, k, v)
        db.commit()
        return {"message": "Session updated", "id": body.id}
    else:
        db.add(SessionModel(**data))
        db.commit()
        return {"message": "Session created", "id": body.id}

@app.delete("/sessions/{session_id}", tags=["Sessions"])
def delete_session(session_id: str, db: DBSession = Depends(get_db),
                   user: dict = Depends(require_admin)):
    row = db.query(SessionModel).filter(SessionModel.id == session_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")
    db.delete(row)
    db.commit()
    return {"message": f"Session {session_id} deleted"}

# ── CSV UPLOAD ──
@app.post("/upload-csv/{session_id}", tags=["CSV"])
async def upload_csv(session_id: str, file: UploadFile = File(...),
                     db: DBSession = Depends(get_db), user: dict = Depends(require_admin)):
    filename = file.filename or ""
    csv_bytes = await file.read()
    csv_text = None
    for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
        try:
            csv_text = csv_bytes.decode(enc); break
        except Exception:
            continue
    if csv_text is None:
        raise HTTPException(status_code=400, detail="Could not decode CSV file")
    result = parse_orderbook_csv(csv_text)
    if "error" in result:
        raise HTTPException(status_code=422, detail=result["error"])
    row = db.query(SessionModel).filter(SessionModel.id == session_id).first()
    if not row:
        row = SessionModel(id=session_id, date=session_id, full=session_id,
                           index_name=result["index_name"])
        db.add(row)
    row.csv_data               = csv_text
    row.csv_filename           = filename
    row.gross_pnl              = result["gross_pnl"]
    row.net_pnl                = result["net_pnl"]
    row.ce_pnl                 = result["ce_pnl"]
    row.pe_pnl                 = result["pe_pnl"]
    row.charges                = result["total_charges"]
    row.executed               = result["executed"]
    row.rejected               = result["rejected"]
    row.index_name             = result["index_name"]
    row.strikes_json           = json.dumps(result["strikes"])
    row.charges_breakdown_json = json.dumps(result["charges_breakdown"])
    row.time_pnl_json          = json.dumps(result.get("time_pnl", {}))
    if row.capital and row.capital > 0:
        row.net_roi   = round(result["net_pnl"]   / row.capital * 100, 4)
        row.gross_roi = round(result["gross_pnl"] / row.capital * 100, 4)
    db.commit()
    return {
        "session_id": session_id, "gross_pnl": result["gross_pnl"],
        "net_pnl": result["net_pnl"], "ce_pnl": result["ce_pnl"],
        "pe_pnl": result["pe_pnl"], "charges_breakdown": result["charges_breakdown"],
        "total_charges": result["total_charges"], "executed": result["executed"],
        "rejected": result["rejected"], "strikes": result["strikes"],
        "carry_positions": result["carry_positions"], "warnings": result["warnings"],
        "message": f"Parsed {result['executed']} orders, {len(result['strikes'])} strikes"
    }

# ── CHART UPLOAD ──
@app.post("/upload-chart/{session_id}", tags=["Charts"])
async def upload_chart(session_id: str, file: UploadFile = File(...),
                       db: DBSession = Depends(get_db), user: dict = Depends(require_admin)):
    img_bytes = await file.read()
    if len(img_bytes) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Image too large (max 10MB)")
    b64  = base64.b64encode(img_bytes).decode("utf-8")
    ext  = (file.filename or "chart.png").rsplit(".", 1)[-1].lower()
    mime = f"image/{ext}" if ext in ("jpg","jpeg","png","webp","gif") else "image/png"
    row  = db.query(SessionModel).filter(SessionModel.id == session_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")
    row.chart_image = f"data:{mime};base64,{b64}"
    db.commit()
    return {"message": "Chart uploaded", "session_id": session_id}

@app.delete("/upload-chart/{session_id}", tags=["Charts"])
def delete_chart(session_id: str, db: DBSession = Depends(get_db),
                 user: dict = Depends(require_admin)):
    row = db.query(SessionModel).filter(SessionModel.id == session_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")
    row.chart_image = None
    db.commit()
    return {"message": "Chart deleted"}

# ── JOURNAL MULTI-CHARTS ──
@app.post("/journal-chart/{session_id}", tags=["Charts"])
async def add_journal_chart(session_id: str, file: UploadFile = File(...),
                            db: DBSession = Depends(get_db), user: dict = Depends(require_admin)):
    img_bytes = await file.read()
    if len(img_bytes) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Image too large (max 10MB)")
    b64  = base64.b64encode(img_bytes).decode("utf-8")
    ext  = (file.filename or "chart.png").rsplit(".", 1)[-1].lower()
    mime = f"image/{ext}" if ext in ("jpg","jpeg","png","webp","gif") else "image/png"
    data_uri = f"data:{mime};base64,{b64}"
    row = db.query(SessionModel).filter(SessionModel.id == session_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")
    charts = json.loads(row.journal_charts_json or "[]")
    charts.append(data_uri)
    row.journal_charts_json = json.dumps(charts)
    db.commit()
    return {"message": "Chart added", "count": len(charts)}

@app.delete("/journal-chart/{session_id}/{chart_idx}", tags=["Charts"])
def delete_journal_chart(session_id: str, chart_idx: int,
                         db: DBSession = Depends(get_db), user: dict = Depends(require_admin)):
    row = db.query(SessionModel).filter(SessionModel.id == session_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")
    charts = json.loads(row.journal_charts_json or "[]")
    if chart_idx < 0 or chart_idx >= len(charts):
        raise HTTPException(status_code=404, detail="Chart index out of range")
    charts.pop(chart_idx)
    row.journal_charts_json = json.dumps(charts)
    db.commit()
    return {"message": "Chart deleted", "count": len(charts)}

# ── AI COMMENTARY (Grok / xAI) ──
@app.post("/ai-commentary/{session_id}", tags=["AI"])
async def generate_ai_commentary(session_id: str, db: DBSession = Depends(get_db),
                                  user: dict = Depends(require_admin)):
    import httpx
    api_key = os.environ.get("GROK_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="GROK_API_KEY not configured on server")
    row = db.query(SessionModel).filter(SessionModel.id == session_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")
    scores     = json.loads(row.scores_json or "{}")
    violations = json.loads(row.violations_json or "[]")
    strengths  = json.loads(row.strengths_json or "[]")
    strikes    = json.loads(row.strikes_json or "[]")
    strikes_summary = ""
    if strikes:
        strikes_summary = "\nTop strikes: " + ", ".join(
            f"{s['symbol']} P&L ₹{s['realized']:,.0f}" for s in strikes[:3])
    prompt = f"""Indian index options trading coach analyzing Vijay's session.
SESSION: {row.full} — {row.index_name} Expiry
VIX: {row.vix} | Capital: ₹{(row.capital or 0)/1e7:.2f}Cr | MT: {row.mt}/10
Gross P&L: ₹{(row.gross_pnl or 0):,.0f} | Net P&L: ₹{(row.net_pnl or 0):,.0f} | ROI: {row.net_roi or 0:.3f}%
CE: ₹{(row.ce_pnl or 0):,.0f} | PE: ₹{(row.pe_pnl or 0):,.0f} | Charges: ₹{(row.charges or 0):,.0f}{strikes_summary}
Violations: {violations} | Strengths: {strengths}
Journal: {row.journal or "Not provided"}
Give sharp 3-paragraph commentary: (1) what the numbers say (2) one thing to fix (3) next session strike guidance."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            "https://api.x.ai/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}",
                     "Content-Type": "application/json"},
            json={"model": "grok-3-mini", "max_tokens": 700,
                  "messages": [{"role": "user", "content": prompt}]}
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Grok API error: {resp.text}")
    row.ai_commentary = resp.json()["choices"][0]["message"]["content"]
    db.commit()
    return {"session_id": session_id, "commentary": row.ai_commentary}

# ── HELPERS ──
def _row_to_dict(r):
    return {
        "id": r.id, "date": r.date, "full": r.full,
        "index": r.index_name, "dte": r.dte,
        "vix": r.vix, "capital": r.capital,
        "gross_pnl": r.gross_pnl, "net_pnl": r.net_pnl,
        "net_roi": r.net_roi, "gross_roi": r.gross_roi,
        "ce_pnl": r.ce_pnl, "pe_pnl": r.pe_pnl,
        "charges": r.charges, "executed": r.executed,
        "rejected": r.rejected, "mt": r.mt,
        "carry_out": r.carry_out, "note": r.note, "journal": r.journal,
        "peer_rois":   json.loads(r.peer_rois_json or "[]"),
        "scores":      json.loads(r.scores_json or "{}"),
        "violations":  json.loads(r.violations_json or "[]"),
        "strengths":   json.loads(r.strengths_json or "[]"),
        "ai_commentary":      r.ai_commentary,
        "csv_filename":       r.csv_filename,
        "has_chart":          bool(r.chart_image),
        "chart_image":        r.chart_image,
        "strikes":            json.loads(r.strikes_json or "[]"),
        "charges_breakdown":  json.loads(r.charges_breakdown_json or "{}"),
        "time_pnl":           json.loads(r.time_pnl_json or "{}"),
        "journal_charts":     json.loads(r.journal_charts_json or "[]"),
    }
