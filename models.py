"""
VJ Trading Dashboard — SQLAlchemy ORM Models
"""
from sqlalchemy import Column, String, Float, Integer, Text, DateTime
from datetime import datetime
from database import Base


class OpenPosition(Base):
    """
    Persists open option positions across sessions so the next upload
    can match closing trades against the actual carry-in entry price
    (not treat the close as a fresh single-sided trade).
    """
    __tablename__ = "open_positions"
    symbol          = Column(String, primary_key=True)
    side            = Column(String)               # 'SHORT' or 'LONG'
    qty             = Column(Float)                # absolute open qty
    avg_price       = Column(Float)                # carry-in vwap
    expiry          = Column(String, nullable=True) # DD/MM/YYYY string from parse_symbol
    strike          = Column(Integer, nullable=True)
    option_type     = Column(String, nullable=True)
    index_name      = Column(String, nullable=True)
    last_session_id = Column(String, nullable=True)
    updated_at      = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Session(Base):
    __tablename__ = "sessions"

    id              = Column(String, primary_key=True, index=True)
    date            = Column(String)
    full            = Column(String)
    index_name      = Column(String)
    dte             = Column(Integer, default=0)
    vix             = Column(Float, nullable=True)
    capital         = Column(Float, nullable=True)
    gross_pnl       = Column(Float, nullable=True)
    net_pnl         = Column(Float, nullable=True)
    net_roi         = Column(Float, nullable=True)
    gross_roi       = Column(Float, nullable=True)
    ce_pnl          = Column(Float, nullable=True)
    pe_pnl          = Column(Float, nullable=True)
    charges         = Column(Float, nullable=True)
    executed        = Column(Integer, nullable=True)
    rejected        = Column(Integer, nullable=True)
    mt              = Column(Float, nullable=True)
    carry_out       = Column(Integer, default=0)
    note            = Column(Text, nullable=True)
    peer_rois_json  = Column(Text, default="[]")
    scores_json     = Column(Text, default="{}")
    violations_json = Column(Text, default="[]")
    strengths_json  = Column(Text, default="[]")
    ai_commentary   = Column(Text, nullable=True)
    csv_data        = Column(Text, nullable=True)
    csv_filename    = Column(String, nullable=True)
    chart_image     = Column(Text, nullable=True)
    journal                = Column(Text, nullable=True)
    strikes_json           = Column(Text, default="[]")
    charges_breakdown_json = Column(Text, default="{}")
    time_pnl_json          = Column(Text, default="{}")
    margin_ts_json         = Column(Text, default="{}")
    journal_charts_json    = Column(Text, default="[]")
    peak_margin            = Column(Float, nullable=True)
