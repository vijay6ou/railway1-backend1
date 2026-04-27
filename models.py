"""
VJ Trading Dashboard — SQLAlchemy ORM Models
"""
from sqlalchemy import Column, String, Float, Integer, Text
from database import Base


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
    journal         = Column(Text, nullable=True)
    strikes_json    = Column(Text, default="[]")
