from pydantic import BaseModel
from typing import Optional, List, Dict, Any

class ScoresModel(BaseModel):
    overall:    int = 0
    discipline: int = 0
    cap_eff:    int = 0
    trade_q:    int = 0
    mkt_align:  int = 0
    vol_handle: int = 0
    peer_cmp:   int = 0

class SessionCreate(BaseModel):
    id:          str
    date:        str
    full:        str
    index_name:  str
    dte:         int = 0
    vix:         Optional[float] = None
    capital:     Optional[float] = None
    gross_pnl:   Optional[float] = None
    net_pnl:     Optional[float] = None
    net_roi:     Optional[float] = None
    gross_roi:   Optional[float] = None
    ce_pnl:      Optional[float] = None
    pe_pnl:      Optional[float] = None
    charges:     Optional[float] = None
    executed:    Optional[int]   = None
    rejected:    Optional[int]   = None
    mt:          Optional[float] = None
    carry_out:   int = 0
    note:        Optional[str]   = None
    peer_rois:   List[float]     = []
    scores:      ScoresModel     = ScoresModel()
    violations:  List[str]       = []
    strengths:   List[str]       = []
    journal:     Optional[str]   = None

class TokenRequest(BaseModel):
    username: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type:   str = "bearer"

class MessageResponse(BaseModel):
    message: str
