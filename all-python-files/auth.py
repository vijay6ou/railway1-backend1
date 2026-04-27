import os
import bcrypt
import jwt
from datetime import datetime, timedelta
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

SECRET_KEY       = os.environ.get("SECRET_KEY", "vj-trading-secret-2026-change-me")
ALGORITHM        = "HS256"
TOKEN_EXPIRE_DAYS = 7
bearer_scheme    = HTTPBearer(auto_error=False)

def _hash(password: str) -> bytes:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt())

def _verify(password: str, hashed: bytes) -> bool:
    return bcrypt.checkpw(password.encode(), hashed)

# Passwords hashed at import time — safe, bcrypt is the direct library
USERS = {
    "vijay": {
        "username": "vijay",
        "pw_hash": _hash(os.environ.get("VIJAY_PASSWORD", "vj@2026")),
        "role": "admin"
    },
    "mentor": {
        "username": "mentor",
        "pw_hash": _hash(os.environ.get("MENTOR_PASSWORD", "mentor@2026")),
        "role": "readonly"
    },
    "peer": {
        "username": "peer",
        "pw_hash": _hash(os.environ.get("PEER_PASSWORD", "peer@2026")),
        "role": "readonly"
    }
}

def authenticate_user(username: str, password: str):
    user = USERS.get(username)
    if not user:
        return None
    if not _verify(password, user["pw_hash"]):
        return None
    return user

def create_access_token(data: dict) -> str:
    payload = data.copy()
    payload["exp"] = datetime.utcnow() + timedelta(days=TOKEN_EXPIRE_DAYS)
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)):
    if not credentials:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        user = USERS.get(username)
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        return user
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

def require_admin(user: dict = Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user
