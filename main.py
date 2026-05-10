"""
VJ Trading Dashboard — FastAPI v3
"""
from fastapi import FastAPI, Depends, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session as DBSession
import json, os, base64, re

# Map for parsing month names embedded in filenames (e.g. "8th may 2026").
_FILENAME_MONTHS = {
    'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
    'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12,
}


def _date_from_filename(filename: str):
    """
    Best-effort extract a YYYY-MM-DD date from a broker CSV filename.
    Tries: ISO (YYYY-MM-DD), DD-MM-YYYY, then '8th may 2026' style.
    """
    if not filename:
        return None
    # 2026-05-09 / 2026_05_09 / 2026.05.09
    m = re.search(r'(20\d{2})[-/_.](\d{1,2})[-/_.](\d{1,2})', filename)
    if m:
        yy, mm, dd = m.groups()
        try:
            return f"{yy}-{int(mm):02d}-{int(dd):02d}"
        except ValueError:
            pass
    # 09-05-2026 / 09_05_2026 / 09.05.2026
    m = re.search(r'(\d{1,2})[-/_.](\d{1,2})[-/_.](20\d{2})', filename)
    if m:
        dd, mm, yy = m.groups()
        try:
            return f"{yy}-{int(mm):02d}-{int(dd):02d}"
        except ValueError:
            pass
    # "8th may 2026" / "08 May 2026"
    m = re.search(
        r'(\d{1,2})\s*(?:st|nd|rd|th)?\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+(20\d{2})',
        filename.lower()
    )
    if m:
        try:
            return f"{m.group(3)}-{_FILENAME_MONTHS[m.group(2)[:3]]:02d}-{int(m.group(1)):02d}"
        except (KeyError, ValueError):
            pass
    return None


def _ddmmyyyy_to_iso(s: str):
    """Convert parser's DD/MM/YYYY session_date to YYYY-MM-DD; None on failure."""
    if not s:
        return None
    m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', s)
    if not m:
        return None
    dd, mm, yyyy = m.groups()
    try:
        return f"{yyyy}-{int(mm):02d}-{int(dd):02d}"
    except ValueError:
        return None

from database import engine, get_db, Base
from models import Session as SessionModel, OpenPosition
from schemas import SessionCreate, TokenRequest, TokenResponse, MessageResponse
from auth import authenticate_user, create_access_token, get_current_user, require_admin
from csv_parser import parse_orderbook_csv
from seed import seed

Base.metadata.create_all(bind=engine)

# ── COLUMN MIGRATIONS (safe ADD COLUMN for existing deployments) ──
def _migrate():
    """
    Add new columns to the sessions table without dropping data.
    Postgres-safe: inspect existing columns FIRST to avoid the
    'current transaction is aborted' cascade that follows a duplicate-column
    error when running multiple ALTER TABLEs in one connection.
    Float type differs (SQLite: REAL, Postgres: DOUBLE PRECISION) — picked per dialect.
    """
    from sqlalchemy import inspect, text
    is_pg = engine.dialect.name == "postgresql"
    float_type = "DOUBLE PRECISION" if is_pg else "REAL"
    new_cols = [
        ("journal",                "TEXT"),
        ("strikes_json",           "TEXT DEFAULT '[]'"),
        ("charges_breakdown_json", "TEXT DEFAULT '{}'"),
        ("time_pnl_json",          "TEXT DEFAULT '{}'"),
        ("margin_ts_json",         "TEXT DEFAULT '{}'"),
        ("journal_charts_json",    "TEXT DEFAULT '[]'"),
        ("peak_margin",            float_type),
    ]
    insp = inspect(engine)
    if "sessions" not in insp.get_table_names():
        return  # fresh DB; create_all already made the full schema
    existing = {c["name"] for c in insp.get_columns("sessions")}
    for col, definition in new_cols:
        if col in existing:
            continue
        # Use a fresh connection per ALTER so a failure on one doesn't poison the rest
        try:
            with engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE sessions ADD COLUMN {col} {definition}"))
        except Exception as e:
            # Best-effort: log and continue. App can still run if a column add fails.
            print(f"[migrate] skip {col}: {e}")

_migrate()
try:
    seed()
except Exception as e:
    # Don't kill the app if seed fails (e.g. Postgres uniqueness on rerun).
    print(f"[seed] skipped: {e}")

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

    # Read existing OpenPositions so the parser can match closing trades
    # against prior-session entry prices (true carry P&L).
    carry_in = [
        {"symbol": op.symbol, "side": op.side, "qty": op.qty, "avg_price": op.avg_price}
        for op in db.query(OpenPosition).all()
    ]

    result = parse_orderbook_csv(csv_text, carry_in=carry_in)
    if "error" in result:
        raise HTTPException(status_code=422, detail=result["error"])

    # ── Determine the canonical session date ──
    # Priority: CSV trade-date (most reliable) > filename date > URL session_id.
    # This prevents the "today's date used because user uploaded days late" bug.
    csv_date_iso  = _ddmmyyyy_to_iso(result.get("session_date"))
    file_date_iso = _date_from_filename(filename)
    canonical_id  = csv_date_iso or file_date_iso or session_id
    date_source = ("csv" if csv_date_iso
                   else "filename" if file_date_iso
                   else "url")

    if canonical_id != session_id:
        # The frontend creates a row at the URL id (typically today's date) BEFORE
        # uploading the CSV. If that row has no csv_data of its own, fold its
        # user-typed metadata into the canonical row, then drop the ghost.
        ghost = db.query(SessionModel).filter(SessionModel.id == session_id).first()
        if ghost and not ghost.csv_data:
            target = db.query(SessionModel).filter(SessionModel.id == canonical_id).first()
            if target is None:
                target = SessionModel(id=canonical_id, date=canonical_id, full=canonical_id,
                                      index_name=ghost.index_name or result["index_name"])
                db.add(target)
                db.flush()
            # Copy fields the user may have set (vix, dte, capital, journal, peers, etc.)
            for fld in ('index_name', 'dte', 'vix', 'capital', 'mt', 'note', 'journal',
                        'peer_rois_json', 'scores_json',
                        'violations_json', 'strengths_json',
                        'chart_image', 'journal_charts_json'):
                v = getattr(ghost, fld, None)
                if v not in (None, '', [], {}, '[]', '{}'):
                    setattr(target, fld, v)
            db.delete(ghost)
            db.commit()
        session_id = canonical_id

    row = db.query(SessionModel).filter(SessionModel.id == session_id).first()
    if not row:
        row = SessionModel(id=session_id, date=session_id, full=session_id,
                           index_name=result["index_name"])
        db.add(row)
    # Always refresh date / full so the displayed date follows the canonical id.
    row.date = session_id
    row.full = session_id
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
    row.margin_ts_json         = json.dumps(result.get("margin_ts", {}))
    row.peak_margin            = result.get("peak_margin", 0.0)
    if row.capital and row.capital > 0:
        row.net_roi   = round(result["net_pnl"]   / row.capital * 100, 4)
        row.gross_roi = round(result["gross_pnl"] / row.capital * 100, 4)

    # ── Sync OpenPosition table ──
    # Symbols traded in this session: clear their old open rows; for those that
    # remain open after this session, write fresh rows with new VWAP.
    traded_symbols = {s["symbol"] for s in result["strikes"]}
    if traded_symbols:
        db.query(OpenPosition).filter(OpenPosition.symbol.in_(traded_symbols)).delete(
            synchronize_session=False
        )
    for op in result.get("open_positions_out", []):
        db.add(OpenPosition(
            symbol=op["symbol"], side=op["side"], qty=op["qty"],
            avg_price=op["avg_price"], expiry=op["expiry"],
            strike=op["strike"], option_type=op["option_type"],
            index_name=op["index_name"], last_session_id=session_id,
        ))

    db.commit()
    msg = f"[{result.get('broker','Generic')}] Parsed {result['executed']} orders, {len(result['strikes'])} strikes"
    if date_source != "url":
        msg += f"  •  Date set from {date_source}: {session_id}"
    return {
        "session_id": session_id, "gross_pnl": result["gross_pnl"],
        "net_pnl": result["net_pnl"], "ce_pnl": result["ce_pnl"],
        "pe_pnl": result["pe_pnl"], "charges_breakdown": result["charges_breakdown"],
        "total_charges": result["total_charges"], "executed": result["executed"],
        "rejected": result["rejected"], "strikes": result["strikes"],
        "carry_positions": result["carry_positions"], "warnings": result["warnings"],
        "carry_in_count":    result.get("carry_in_count", 0),
        "carry_in_realized": result.get("carry_in_realized", 0.0),
        "peak_margin":       result.get("peak_margin", 0.0),
        "broker":  result.get("broker", "Generic"),
        "session_date":      result.get("session_date"),
        "canonical_id":      session_id,    # so frontend can refresh URL/state
        "date_source":       date_source,   # 'csv' | 'filename' | 'url'
        "message": msg,
    }


# ── OPEN POSITIONS (carry table) ──
@app.get("/open-positions", tags=["Carry"])
def list_open_positions(db: DBSession = Depends(get_db), user: dict = Depends(get_current_user)):
    rows = db.query(OpenPosition).order_by(OpenPosition.symbol).all()
    return [{
        "symbol": r.symbol, "side": r.side, "qty": r.qty, "avg_price": r.avg_price,
        "expiry": r.expiry, "strike": r.strike, "option_type": r.option_type,
        "index_name": r.index_name, "last_session_id": r.last_session_id,
        "updated_at": r.updated_at.isoformat() if r.updated_at else None,
    } for r in rows]

@app.delete("/open-positions", tags=["Carry"])
def clear_open_positions(db: DBSession = Depends(get_db), user: dict = Depends(require_admin)):
    n = db.query(OpenPosition).delete()
    db.commit()
    return {"message": f"Cleared {n} open positions. Re-upload sessions in date order to rebuild."}

@app.post("/open-positions/rebuild", tags=["Carry"])
def rebuild_open_positions(db: DBSession = Depends(get_db), user: dict = Depends(require_admin)):
    """Wipe the carry table, then re-process every session's stored CSV in date order."""
    db.query(OpenPosition).delete()
    db.commit()
    sessions = db.query(SessionModel).filter(SessionModel.csv_data != None).order_by(SessionModel.id).all()
    rebuilt = 0
    for s in sessions:
        carry_in = [
            {"symbol": op.symbol, "side": op.side, "qty": op.qty, "avg_price": op.avg_price}
            for op in db.query(OpenPosition).all()
        ]
        result = parse_orderbook_csv(s.csv_data, carry_in=carry_in)
        if "error" in result:
            continue
        traded = {st["symbol"] for st in result["strikes"]}
        if traded:
            db.query(OpenPosition).filter(OpenPosition.symbol.in_(traded)).delete(
                synchronize_session=False
            )
        for op in result.get("open_positions_out", []):
            db.add(OpenPosition(
                symbol=op["symbol"], side=op["side"], qty=op["qty"],
                avg_price=op["avg_price"], expiry=op["expiry"],
                strike=op["strike"], option_type=op["option_type"],
                index_name=op["index_name"], last_session_id=s.id,
            ))
        # also refresh session-level numbers since carry-in changed P&L
        s.gross_pnl = result["gross_pnl"]; s.net_pnl = result["net_pnl"]
        s.ce_pnl = result["ce_pnl"];       s.pe_pnl = result["pe_pnl"]
        s.charges = result["total_charges"]
        s.strikes_json = json.dumps(result["strikes"])
        s.margin_ts_json = json.dumps(result.get("margin_ts", {}))
        s.peak_margin = result.get("peak_margin", 0.0)
        if s.capital and s.capital > 0:
            s.net_roi   = round(result["net_pnl"]   / s.capital * 100, 4)
            s.gross_roi = round(result["gross_pnl"] / s.capital * 100, 4)
        db.commit()
        rebuilt += 1
    return {"message": f"Rebuilt {rebuilt} sessions in date order."}

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
        "margin_ts":          json.loads(r.margin_ts_json or "{}"),
        "peak_margin":        r.peak_margin,
        "journal_charts":     json.loads(r.journal_charts_json or "[]"),
    }
