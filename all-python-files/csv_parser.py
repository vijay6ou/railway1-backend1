"""
STOXXO / XTS Order Book CSV Parser — Robust multi-format version
Handles STOXXO, Jainam, Zerodha, and generic broker CSV formats.
"""
import csv
import re
import io
from collections import defaultdict
from charge_engine import compute_charges


# ── Column name alias map ──────────────────────────────────────────
SYMBOL_COLS   = ['symbol','tradingsymbol','trading_symbol','scripname','scrip_name',
                 'instrument','instrumentname','instrument_name','stock']
SIDE_COLS     = ['side','transactiontype','transaction_type','buysell','buy/sell',
                 'type','order_type','ordertype','b/s','direction','buysell']
QTY_COLS      = ['tradeqty','trade_qty','qty','filledqty','filled_qty','quantity',
                 'tradedqty','traded_qty','executedqty','executed_qty','lotsize','lots',
                 'totalqty','total_qty','tradequantity']
PRICE_COLS    = ['tradeprice','trade_price','price','averageprice','average_price',
                 'avg_price','avgprice','executedprice','executed_price','tradedprice',
                 'traded_price','lastprice','last_price','fillprice','fill_price','rate']
STATUS_COLS   = ['status','orderstatus','order_status','state','orderstate']

def _get(row, cols):
    for c in cols:
        if c in row:
            v = row[c]
            return v if v not in (None,'','N/A','NA','-') else None
    return None

def _norm_cols(row):
    """Normalize column names: lower, strip, collapse separators."""
    return {re.sub(r'[\s_\-\.]+','',k.strip().lower()): v.strip() if isinstance(v,str) else v
            for k,v in row.items()}


def parse_symbol(symbol: str) -> dict:
    """Parse NSE/BSE option symbol. Handles NIFTY, BANKNIFTY, SENSEX, FINNIFTY, MIDCPNIFTY."""
    s = symbol.upper().strip()
    # Standard compact format: INDEX + YYMMDD + STRIKE + CE/PE
    m = re.match(
        r'^(NIFTY|BANKNIFTY|SENSEX|FINNIFTY|MIDCPNIFTY|BANKEX)(\d{5,6})(\d{4,6})(CE|PE)$', s)
    if m:
        index, expiry_raw, strike, opt_type = m.groups()
        try:
            yr = 2000 + int(expiry_raw[:2])
            mo = int(expiry_raw[2:4])
            dy = int(expiry_raw[4:])
            expiry = f"{dy:02d}/{mo:02d}/{yr}"
        except Exception:
            expiry = expiry_raw
        return {"index": index, "expiry": expiry, "strike": int(strike), "option_type": opt_type}

    # Alternate: INDEX + DD + MON + YY + STRIKE + CE/PE (e.g. NIFTY24APR2526000CE)
    m2 = re.match(
        r'^(NIFTY|BANKNIFTY|SENSEX|FINNIFTY|MIDCPNIFTY|BANKEX)(\d{2})([A-Z]{3})(\d{2,4})(\d{4,6})(CE|PE)$', s)
    if m2:
        index, dd, mon, yr, strike, opt_type = m2.groups()
        return {"index": index, "expiry": f"{dd}/{mon}/{yr}", "strike": int(strike), "option_type": opt_type}

    return {"index": s.split('2')[0] if '2' in s else s, "expiry": None, "strike": None, "option_type": None}


def _detect_delimiter(text: str) -> str:
    sample = text[:3000]
    counts = {d: sample.count(d) for d in [',', '\t', ';', '|']}
    return max(counts, key=counts.get)


def _is_executed(status: str) -> bool:
    if not status:
        return True   # no status column → treat all as executed
    s = status.upper().strip()
    return any(x in s for x in ('COMPLETE','TRADED','FILLED','EXECUTED','DONE','SUCCESS','FULL'))

def _is_rejected(status: str) -> bool:
    s = (status or '').upper().strip()
    return any(x in s for x in ('REJECT','CANCEL','EXPIRED','FAILED','ERROR'))


def parse_orderbook_csv(csv_text: str) -> dict:
    warnings = []

    # Strip BOM
    csv_text = csv_text.lstrip('\ufeff')

    delim = _detect_delimiter(csv_text)
    reader = csv.DictReader(io.StringIO(csv_text), delimiter=delim)

    rows = []
    for row in reader:
        rows.append(_norm_cols(row))

    if not rows:
        return {"error": "No data rows found in CSV"}

    # Diagnose columns for debugging
    sample_keys = list(rows[0].keys()) if rows else []

    fills = defaultdict(lambda: {"buys": [], "sells": []})
    executed_count = 0
    rejected_count = 0
    total_buy_val  = 0.0
    total_sell_val = 0.0
    skipped = 0

    for row in rows:
        status_raw = _get(row, STATUS_COLS)

        if _is_rejected(status_raw):
            rejected_count += 1
            continue

        if not _is_executed(status_raw):
            skipped += 1
            continue

        symbol = _get(row, SYMBOL_COLS)
        if not symbol:
            continue
        symbol = symbol.strip().upper()

        side_raw = (_get(row, SIDE_COLS) or '').upper().strip()
        qty_raw  = _get(row, QTY_COLS)
        px_raw   = _get(row, PRICE_COLS)

        try:
            qty = float(str(qty_raw).replace(',', '').strip())
            px  = float(str(px_raw).replace(',', '').strip())
        except (ValueError, TypeError):
            warnings.append(f"Cannot parse qty/price for {symbol}: qty={qty_raw} px={px_raw}")
            continue

        if qty <= 0 or px <= 0:
            continue

        is_buy  = any(x in side_raw for x in ('BUY','B','LONG','PURCHASE')) or side_raw == 'B'
        is_sell = any(x in side_raw for x in ('SELL','S','SHORT','SALE')) or side_raw == 'S'

        if not is_buy and not is_sell:
            # Last resort: single char
            if side_raw in ('B',): is_buy = True
            elif side_raw in ('S',): is_sell = True
            else:
                warnings.append(f"Unknown side '{side_raw}' for {symbol}")
                continue

        executed_count += 1
        value = qty * px

        if is_buy:
            fills[symbol]["buys"].append({"qty": qty, "price": px})
            total_buy_val += value
        else:
            fills[symbol]["sells"].append({"qty": qty, "price": px})
            total_sell_val += value

    if not fills:
        col_hint = f"Detected columns: {', '.join(sample_keys[:12])}"
        return {"error": f"No valid executed trades found. {col_hint}. Check CSV format or column names."}

    if skipped > 0:
        warnings.append(f"{skipped} rows skipped (pending/open/unknown status)")

    total_turnover = total_buy_val + total_sell_val

    strikes = []
    gross_pnl    = 0.0
    total_ce_pnl = 0.0
    total_pe_pnl = 0.0

    for symbol, data in fills.items():
        buys  = data["buys"]
        sells = data["sells"]

        buy_qty   = sum(f["qty"] for f in buys)
        sell_qty  = sum(f["qty"] for f in sells)
        buy_vwap  = sum(f["qty"]*f["price"] for f in buys)  / buy_qty  if buy_qty  else 0
        sell_vwap = sum(f["qty"]*f["price"] for f in sells) / sell_qty if sell_qty else 0

        matched_qty = min(buy_qty, sell_qty)
        realized    = (sell_vwap - buy_vwap) * matched_qty if matched_qty > 0 else 0.0
        open_qty    = abs(buy_qty - sell_qty)
        open_side   = "BUY" if buy_qty > sell_qty else "SELL" if sell_qty > buy_qty else None

        sym_info = parse_symbol(symbol)
        strike_data = {
            "symbol":      symbol,
            "index":       sym_info["index"],
            "strike":      sym_info["strike"],
            "option_type": sym_info["option_type"],
            "expiry":      sym_info["expiry"],
            "buy_qty":     buy_qty,
            "sell_qty":    sell_qty,
            "buy_vwap":    round(buy_vwap, 2),
            "sell_vwap":   round(sell_vwap, 2),
            "matched_qty": matched_qty,
            "realized":    round(realized, 2),
            "open_qty":    open_qty,
            "open_side":   open_side,
            "is_carry":    open_qty > 0
        }
        strikes.append(strike_data)
        gross_pnl += realized
        opt = sym_info["option_type"]
        if opt == "CE": total_ce_pnl += realized
        elif opt == "PE": total_pe_pnl += realized

    charges = compute_charges(total_buy_val, total_sell_val, total_turnover)
    net_pnl = gross_pnl - charges["total"]

    indices    = [s["index"] for s in strikes if s["index"]]
    index_name = max(set(indices), key=indices.count) if indices else "Unknown"

    strikes.sort(key=lambda x: x["realized"], reverse=True)

    return {
        "index_name":    index_name,
        "gross_pnl":     round(gross_pnl, 2),
        "net_pnl":       round(net_pnl, 2),
        "ce_pnl":        round(total_ce_pnl, 2),
        "pe_pnl":        round(total_pe_pnl, 2),
        "total_buy_val":  round(total_buy_val, 2),
        "total_sell_val": round(total_sell_val, 2),
        "total_turnover": round(total_turnover, 2),
        "charges_breakdown": charges,
        "total_charges": charges["total"],
        "executed":      executed_count,
        "rejected":      rejected_count,
        "strikes":       strikes,
        "warnings":      warnings,
        "carry_positions": [s for s in strikes if s["is_carry"]]
    }
