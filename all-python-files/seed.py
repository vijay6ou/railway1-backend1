"""
Seed script — runs once on first deploy.
Inserts all 5 existing evaluated sessions into the DB.
"""
import json
from database import engine, SessionLocal, Base
from models import Session

def seed():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    if db.query(Session).count() > 0:
        print("DB already seeded, skipping.")
        db.close()
        return

    sessions_data = [
        {
            "id": "2026-03-30",
            "date": "Mar 30",
            "full": "30-Mar-2026",
            "index_name": "BankNifty",
            "dte": 0,
            "vix": 17.2,
            "capital": 20000000,
            "gross_pnl": None,
            "net_pnl": None,
            "net_roi": None,
            "gross_roi": None,
            "ce_pnl": None,
            "pe_pnl": None,
            "charges": None,
            "executed": None,
            "rejected": None,
            "mt": 7.0,
            "carry_out": 0,
            "note": "BankNifty monthly expiry. Full data not available — summary only.",
            "peer_rois": [],
            "scores": {"overall":62,"discipline":55,"cap_eff":65,"trade_q":60,"mkt_align":65,"vol_handle":60,"peer_cmp":70},
            "violations": ["Significant intraday peak erosion — held positions past optimal exit window","CE strike placed too close to ATM in pre-market bullish setup"],
            "strengths": ["Monthly expiry — full theta capture on most legs","Managed BankNifty range effectively for majority of session"]
        },
        {
            "id": "2026-04-06",
            "date": "Apr 6",
            "full": "06-Apr-2026",
            "index_name": "Nifty",
            "dte": 0,
            "vix": 18.2,
            "capital": 20000000,
            "gross_pnl": 100075,
            "net_pnl": None,
            "net_roi": None,
            "gross_roi": 0.50,
            "ce_pnl": None,
            "pe_pnl": None,
            "charges": None,
            "executed": None,
            "rejected": None,
            "mt": 6.5,
            "carry_out": 6,
            "note": "Nifty weekly expiry. 6 Sensex legs carried forward to Apr 9 expiry.",
            "peer_rois": [],
            "scores": {"overall":68,"discipline":60,"cap_eff":72,"trade_q":65,"mkt_align":58,"vol_handle":65,"peer_cmp":80},
            "violations": ["CE strike placed too close to ATM in pre-market bullish setup — pattern repeat","6 Sensex carry positions open post-expiry"],
            "strengths": ["₹1,00,075 gross realized P&L on Nifty closed positions","Managed strangle legs effectively despite bullish gap-up open"]
        },
        {
            "id": "2026-04-08",
            "date": "Apr 8",
            "full": "08-Apr-2026",
            "index_name": "Sensex",
            "dte": 0,
            "vix": 19.8,
            "capital": 20000000,
            "gross_pnl": -85000,
            "net_pnl": -90000,
            "net_roi": -0.45,
            "gross_roi": -0.425,
            "ce_pnl": -105000,
            "pe_pnl": 20000,
            "charges": 5000,
            "executed": 180,
            "rejected": 12,
            "mt": 8.5,
            "carry_out": 0,
            "note": "3-sigma gap-up causing loss session.",
            "peer_rois": [-0.3, -0.5],
            "scores": {"overall":45,"discipline":40,"cap_eff":50,"trade_q":40,"mkt_align":35,"vol_handle":45,"peer_cmp":65},
            "violations": ["3-sigma gap-up triggered CE short losses","CE strike placed too close to ATM — third consecutive session","No pre-defined gap-up protocol executed"],
            "strengths": ["PE leg partially offset CE losses","Closed all positions — no carry risk into next expiry"]
        },
        {
            "id": "2026-04-10",
            "date": "Apr 10",
            "full": "10-Apr-2026",
            "index_name": "Nifty",
            "dte": 0,
            "vix": 18.5,
            "capital": 20000000,
            "gross_pnl": 124423,
            "net_pnl": 119560,
            "net_roi": 0.5978,
            "gross_roi": 0.622,
            "ce_pnl": 80423,
            "pe_pnl": 44000,
            "charges": 4863,
            "executed": 151,
            "rejected": 0,
            "mt": 6.85,
            "carry_out": 0,
            "note": None,
            "peer_rois": [-0.2, -0.2],
            "scores": {"overall":71,"discipline":70,"cap_eff":80,"trade_q":65,"mkt_align":55,"vol_handle":68,"peer_cmp":92},
            "violations": ["CE strike placement too close to ATM in pre-market bullish setup — 4th consecutive session","Market Alignment below threshold"],
            "strengths": ["₹1,24,423 gross P&L on 9 contracts","0 rejections — clean margin management","Peer comparison 92/100","Capital efficiency 80/100","Charge ratio 3.9%"]
        },
        {
            "id": "2026-04-16",
            "date": "Apr 16",
            "full": "16-Apr-2026",
            "index_name": "Sensex",
            "dte": 0,
            "vix": 18.17,
            "capital": 80000000,
            "gross_pnl": 1818937,
            "net_pnl": 1732392,
            "net_roi": 2.165,
            "gross_roi": 2.274,
            "ce_pnl": 2259008,
            "pe_pnl": -1689206,
            "charges": 86545,
            "executed": 2112,
            "rejected": 570,
            "mt": 7.55,
            "carry_out": 2,
            "note": None,
            "peer_rois": [0.2, 0.9],
            "scores": {"overall":56,"discipline":30,"cap_eff":70,"trade_q":50,"mkt_align":70,"vol_handle":50,"peer_cmp":90},
            "violations": ["570 margin rejections from 09:32 (required ₹82.68Cr vs ₹80Cr limit)","77800CE shorted less than 1.5% OTM — loss -₹8.47L","78400PE + 78000PE over-sized near ATM on bearish open","No hard stop on 77800CE short — held 09:32 to 14:03"],
            "strengths": ["78200CE +₹13.39L — full theta capture","78500CE +₹7.52L — complete premium decay","77800PE +₹5.94L — aligned with morning trend","Charge ratio 4.76% efficient on ₹7.51Cr turnover"]
        }
    ]

    for s in sessions_data:
        obj = Session(
            id           = s["id"],
            date         = s["date"],
            full         = s["full"],
            index_name   = s["index_name"],
            dte          = s.get("dte", 0),
            vix          = s.get("vix"),
            capital      = s.get("capital"),
            gross_pnl    = s.get("gross_pnl"),
            net_pnl      = s.get("net_pnl"),
            net_roi      = s.get("net_roi"),
            gross_roi    = s.get("gross_roi"),
            ce_pnl       = s.get("ce_pnl"),
            pe_pnl       = s.get("pe_pnl"),
            charges      = s.get("charges"),
            executed     = s.get("executed"),
            rejected     = s.get("rejected"),
            mt           = s.get("mt"),
            carry_out    = s.get("carry_out", 0),
            note         = s.get("note"),
            peer_rois_json  = json.dumps(s.get("peer_rois", [])),
            scores_json     = json.dumps(s.get("scores", {})),
            violations_json = json.dumps(s.get("violations", [])),
            strengths_json  = json.dumps(s.get("strengths", []))
        )
        db.add(obj)

    db.commit()
    db.close()
    print(f"Seeded {len(sessions_data)} sessions successfully.")

if __name__ == "__main__":
    seed()
