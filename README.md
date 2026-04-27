# VJ Trading Dashboard — Backend API

FastAPI backend for the VJ Trading Dashboard.  
Deployed on Railway. Frontend on Firebase Hosting.

---

## One-Time Deploy to Railway

### Step 1 — Push to GitHub
```
git init
git add .
git commit -m "VJ Trading API initial"
git remote add origin https://github.com/YOUR_USERNAME/vj-trading-api.git
git push -u origin main
```

### Step 2 — Deploy on Railway
1. Go to railway.app → Login with GitHub
2. New Project → Deploy from GitHub repo → select vj-trading-api
3. Railway auto-detects Python + Procfile → deploys automatically
4. Go to Settings → Networking → Generate Domain
5. Your API URL: `https://vj-trading-api-xxxx.railway.app`

### Step 3 — Set Environment Variables on Railway
In Railway project → Variables tab, add:
```
SECRET_KEY          = any-long-random-string-here
VIJAY_PASSWORD      = your-chosen-password
MENTOR_PASSWORD     = mentor-view-password
PEER_PASSWORD       = peer-view-password
ANTHROPIC_API_KEY   = sk-ant-xxxx   (optional, for AI commentary)
```

### Step 4 — Update Frontend API URL
In `public/index.html`, line ~12:
```js
const API = 'https://vj-trading-api-xxxx.railway.app';  // ← your Railway URL
```

### Step 5 — Deploy Frontend to Firebase
```
firebase deploy
```

---

## System-Independent Update Workflow

### To add a session from any device:
1. Open your Firebase URL in any browser
2. Login with your credentials  
3. Click + Add Session → fill form → Save to Server
4. Data saved instantly to Railway SQLite DB

### To update frontend HTML from any device:
1. Open github.com → your repo → public/index.html
2. Edit directly in browser (pencil icon)
3. Commit → GitHub Actions auto-deploys to Firebase

### To upload CSV from any device:
1. Open Firebase URL → Upload CSV tab
2. Select session → upload STOXXO order book CSV
3. Server parses, computes charges, updates session automatically

---

## Local Development
```bash
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
# API docs at: http://localhost:8000/docs
```

---

## API Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| POST | /auth/token | — | Login, get JWT |
| GET | /auth/me | Any | Get current user |
| GET | /sessions | Any | Fetch all sessions |
| GET | /sessions/{id} | Any | Fetch one session |
| POST | /sessions | Admin | Create/update session |
| DELETE | /sessions/{id} | Admin | Delete session |
| POST | /upload-csv/{id} | Admin | Upload + parse CSV |
| POST | /ai-commentary/{id} | Admin | Generate AI commentary |

API docs (Swagger UI): `https://your-railway-url.railway.app/docs`

---

## Two Repos, Two Purposes

| Repo | Platform | What lives there |
|------|----------|-----------------|
| `vj-trading-api` | Railway | Python backend (this repo) |
| `vj-trading-frontend` | Firebase via GitHub | index.html only |

Keep them separate. Railway watches the backend repo.  
Firebase watches the frontend repo via GitHub Actions.
