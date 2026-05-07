"""
STOXXO / XTS Order Book CSV Parser — Robust multi-format version
Handles STOXXO, Jainam, Zerodha, and generic broker CSV formats.
"""
import csv
import re
import io
from collections import defaultdict
from charge_engine import compute_charges
from datetime import datetime


# ── Column name alias map (covers Zerodha, Upstox, Angel, Fyers, IIFL, 5paisa, STOXXO/Jainam) ──
SYMBOL_COLS   = ['symbolname','symbol','tradingsymbol','scripname','scrip',
                 'instrument','instrumentname','stock','contractname','contract']
SIDE_COLS     = ['side','transactiontype','transaction_type','buysell','buy/sell','buy_sell',
                 'type','order_type','ordertype','b/s','direction','tradetype','trade_type',
                 'orderside','txnside']
TIME_COLS     = ['time','tradetime','ordertime','timestamp','timedate','datetime',
                 'executiontime','tradeddatetime','exchangetime','tradedat','tradedtime',
                 'orderexecutiontime','exchangeordertime','exchordertime','filltime']
DATE_COLS     = ['tradedate','trade_date','date','orderdate','order_date','executiondate']
QTY_COLS      = ['tradeqty','trade_qty','qty','filledqty','filled_qty','quantity',
                 'tradedqty','traded_qty','executedqty','executed_qty','lotsize','lots',
                 'totalqty','total_qty','tradequantity','filledquantity','executedquantity']
PRICE_COLS    = ['tradeprice','trade_price','price','averageprice','average_price',
                 'avg_price','avgprice','executedprice','executed_price','tradedprice',
                 'traded_price','lastprice','last_price','fillprice','fill_price','rate',
                 'avgexecutionprice','execution_price','avgtradeprice']
STATUS_COLS   = ['status','orderstatus','order_status','state','orderstate','ordstatus','exchange_status']

# ── Broker format presets (for autodetection display) ──
BROKER_FORMATS = [
    ('Zerodha (Kite/Console)',  ['tradingsymbol','trade_price','order_execution_time']),
    ('Upstox',                  ['scrip','trade_type','traded_price','trade_date']),
    ('Angel One',               ['symbol','transactiontype','tradedquantity','averageprice']),
    ('Fyers',                   ['symbol','side','qty','tradeprice','tradetime']),
    ('IIFL Securities',         ['scripname','transactiontype','quantity','price']),
    ('5paisa',                  ['scripname','buysell','qty','price','ordstatus']),
    ('STOXXO / Jainam',         ['symbol','side','tradeqty','tradeprice']),
]

def detect_broker(headers):
    """Identify broker from CSV headers via exact column match (after normalisation)."""
    norm = set(re.sub(r'[\s_\-\./]+','',h.strip().lower()) for h in headers)
    best, best_ratio, best_score = 'Generic', 0.0, 0
    for name, signals in BROKER_FORMATS:
        sigs = [re.sub(r'[\s_\-\./]+','',s) for s in signals]
        score = sum(1 for s in sigs if s in norm)
        if score < max(2, len(sigs) * 0.6):
            continue
        ratio = score / len(sigs)
        # Prefer higher absolute score (more specific format), then higher coverage ratio
        if score > best_score or (score == best_score and ratio > best_ratio):
            best, best_score, best_ratio = name, score, ratio
    return best

def _get(row, cols):
    for c in cols:
        if c in row:
            v = row[c]
            return v if v not in (None,'','N/A','NA','-') else None
    return None

def _norm_cols(row):
    """Normalize column names: lower, strip, collapse separators."""
    return {re.sub(r'[\s_\-\./]+','',k.strip().lower()): v.strip() if isinstance(v,str) else v
            for k,v in row.items()}


def parse_symbol(symbol: str) -> dict:
    """Parse NSE/BSE option symbol. Handles NIFTY, BANKNIFTY, SENSEX, FINNIFTY, MIDCPNIFTY."""
    s = symbol.upper().strip()
    # Standard compact format: INDEX + YYMMDD + STRIKE + CE/PE
    m = re.match(
        r'^(NIFTY|BANKNIFTY|SENSEX|FINNIFTY|MIDCPNIFTY|BANKEX)(\d{5,6}?)(\d{4,6})(CE|PE)$', s)
    if m:
        index, expiry_raw, strike, opt_type = m.groups()
        try:
            yr = 2000 + int(expiry_raw[:2])
            # 5-char = YYMDD (single-digit month for Jan-Sep), 6-char = YYMMDD
            if len(expiry_raw) == 5:
                mo = int(expiry_raw[2:3])
                dy = int(expiry_raw[3:5])
            else:
                mo = int(expiry_raw[2:4])
                dy = int(expiry_raw[4:6])
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


def _parse_time(raw):
    """Return HH:MM string from various broker time formats, or None."""
    if not raw:
        return None
    raw = str(raw).strip()
    # Try common patterns (covers Zerodha, Upstox, Angel, Fyers, IIFL, 5paisa, STOXXO)
    for fmt in ('%H:%M:%S', '%H:%M', '%d/%m/%Y %H:%M:%S', '%d-%m-%Y %H:%M:%S',
                '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%d/%m/%Y %H:%M',
                '%m/%d/%Y %H:%M:%S', '%d-%b-%Y %H:%M:%S', '%Y-%m-%d %H:%M',
                '%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%d-%b-%Y',
                '%Y-%m-%dT%H:%M:%S.%f', '%d-%b-%Y %H:%M',
                '%H:%M:%S / %d-%m-%Y', '%H:%M / %d-%m-%Y'):  # STOXXO Time/Date
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.hour == 0 and dt.minute == 0 and ':' not in raw and 'T' not in raw:
                return None
            return dt.strftime('%H:%M')
        except ValueError:
            continue
    m = re.search(r'(\d{2}):(\d{2})', raw)
    if m:
        h, mn = int(m.group(1)), int(m.group(2))
        if 0 <= h <= 23 and 0 <= mn <= 59:
            return f"{h:02d}:{mn:02d}"
    return None


def _parse_trade_date(raw):
    """Extract DD/MM/YYYY date from a broker time/date string."""
    if not raw:
        return None
    raw = str(raw).strip()
    # STOXXO: '09:18:18 / 06-05-2026'
    m = re.search(r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})', raw)
    if m:
        d, mo, y = m.groups()
        return f"{int(d):02d}/{int(mo):02d}/{int(y)}"
    # ISO: '2026-05-06...'
    m = re.search(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})', raw)
    if m:
        y, mo, d = m.groups()
        return f"{int(d):02d}/{int(mo):02d}/{int(y)}"
    return None


# ── Margin model (post-Sep 2024 SEBI rules, with hedge/spread detection) ──
#
# Components per SEBI / NSE F&O margin framework:
#   SPAN      — scenario-based VaR (price + vol scan). Approx by moneyness band:
#               deep-OTM ≈ 6%, OTM ≈ 8%, ATM ≈ 11%, ITM ≈ 13%, deep-ITM ≈ 16%
#   Exposure  — flat 3% of notional for index options (5% for stock)
#   ELM       — +2% of notional for SHORT options ON EXPIRY DAY only
#               (SEBI circular CIR/MRD/DP/03/2024, effective Nov 2024)
#   Spread    — when SHORT and LONG of same opt_type held simultaneously,
#               the hedged qty consumes only spread_width × qty (max loss),
#               not the naked-short SPAN+Exposure. Applied per (index, opt_type).
#   Long premium — for excess longs (not hedging a short), premium paid is
#                  treated as capital consumed (no margin block beyond it).

EXPOSURE_PCT     = 0.03  # Exposure margin for index options
EXPIRY_ELM_PCT   = 0.02  # Extreme Loss Margin on expiry day for shorts


def _span_pct(strike, spot, opt_type):
    """SPAN margin % of notional, scaled by distance from spot.
    Approximates exchange scenario-based margin without scenario data."""
    if not spot or spot <= 0 or not strike:
        return 0.10  # conservative default when spot unknown
    if opt_type == 'CE':
        moneyness = (strike - spot) / spot   # +ve = OTM call (short safer)
    else:                                     # PE
        moneyness = (spot - strike) / spot   # +ve = OTM put (short safer)
    if moneyness >= 0.05:  return 0.06   # deep OTM
    if moneyness >= 0.02:  return 0.08   # OTM
    if moneyness >= -0.02: return 0.11   # ATM
    if moneyness >= -0.05: return 0.13   # ITM
    return 0.16                          # deep ITM


def _spot_proxy(positions):
    """Estimate spot per index from open legs.
    Uses qty-weighted midpoint of the strike range — good proxy when traders
    cluster strikes around ATM (typical for option sellers)."""
    spots = {}
    by_idx = defaultdict(list)  # idx -> list of (strike, qty)
    for p in positions:
        if p.get('index') and p.get('strike') and p.get('qty'):
            by_idx[p['index']].append((p['strike'], p['qty']))
    for idx, items in by_idx.items():
        if not items:
            continue
        # Range midpoint is the simplest decent proxy — better than weighted avg
        # because traders deliberately cluster strikes around current spot.
        ks = [k for k, _ in items]
        spots[idx] = (min(ks) + max(ks)) / 2.0
    return spots


def _compute_total_margin(positions, session_date):
    """
    positions: list of open-leg dicts:
        {symbol, index, opt_type, strike, side ('SHORT'|'LONG'), qty, vwap, expiry}
    Returns total margin required across all legs, with spread/hedge pairing
    applied per (index, opt_type) bucket.
    """
    if not positions:
        return 0.0
    spots = _spot_proxy(positions)

    # Group by (index, opt_type)
    groups = defaultdict(lambda: {'shorts': [], 'longs': []})
    for p in positions:
        key = (p.get('index'), p.get('opt_type'))
        bucket = 'shorts' if p['side'] == 'SHORT' else 'longs'
        groups[key][bucket].append(p)

    total = 0.0
    for (idx, ot), g in groups.items():
        if not idx or not ot:
            continue
        spot = spots.get(idx, 0)
        shorts = g['shorts']
        longs  = g['longs']

        ts_qty = sum(s['qty'] for s in shorts)
        tl_qty = sum(l['qty'] for l in longs)

        # ── Spread benefit (hedged qty) ──
        hedged_qty = min(ts_qty, tl_qty)
        if hedged_qty > 0 and shorts and longs:
            # qty-weighted avg strikes give a representative spread width
            avg_short_k = sum(s['qty'] * s['strike'] for s in shorts) / ts_qty
            avg_long_k  = sum(l['qty'] * l['strike'] for l in longs)  / tl_qty
            spread_width = abs(avg_short_k - avg_long_k)
            # max-loss spread margin (exact for vertical credit/debit spreads)
            total += spread_width * hedged_qty

        # ── Naked short residue ──
        naked_short_qty = ts_qty - hedged_qty
        if naked_short_qty > 0:
            avg_strike = sum(s['qty'] * s['strike'] for s in shorts) / ts_qty
            span_p = _span_pct(avg_strike, spot, ot)
            notional = naked_short_qty * avg_strike
            naked = (span_p + EXPOSURE_PCT) * notional
            # ELM only if any short leg in this group expires on session date
            if session_date and any(s.get('expiry') == session_date for s in shorts):
                naked += EXPIRY_ELM_PCT * notional
            total += naked

        # ── Excess longs (no short to hedge) — only premium paid ──
        excess_long_qty = tl_qty - hedged_qty
        if excess_long_qty > 0 and tl_qty > 0:
            avg_long_vwap = sum(l['qty'] * l['vwap'] for l in longs) / tl_qty
            total += excess_long_qty * avg_long_vwap

    return total


def _open_legs_from_book(running_book):
    """Build the per-leg open-positions list used by _compute_total_margin
    from the running fills book at a given tick."""
    legs = []
    for sym, book in running_book.items():
        bq = sum(f["qty"] for f in book["buys"])
        sq = sum(f["qty"] for f in book["sells"])
        open_qty = abs(bq - sq)
        if open_qty <= 0:
            continue
        side = 'SHORT' if sq > bq else 'LONG'
        if side == 'SHORT':
            vwap = sum(f["qty"]*f["price"] for f in book["sells"]) / sq if sq else 0
        else:
            vwap = sum(f["qty"]*f["price"] for f in book["buys"]) / bq if bq else 0
        info = parse_symbol(sym)
        legs.append({
            'symbol':   sym,
            'index':    info.get('index'),
            'opt_type': info.get('option_type'),
            'strike':   info.get('strike'),
            'expiry':   info.get('expiry'),
            'side':     side,
            'qty':      open_qty,
            'vwap':     vwap,
        })
    return legs


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


def parse_orderbook_csv(csv_text: str, carry_in: list = None) -> dict:
    """
    Parse an order book CSV.

    carry_in: optional list of dicts representing positions held coming INTO
              this session (read from the OpenPosition table). Each entry:
              {symbol, side ('SHORT'|'LONG'), qty, avg_price}
              These are prepended to today's fills so VWAP-based realised P&L
              correctly accounts for prior-session entry prices.
    """
    warnings = []
    carry_in = carry_in or []

    # Strip BOM
    csv_text = csv_text.lstrip('﻿')

    delim = _detect_delimiter(csv_text)
    reader = csv.DictReader(io.StringIO(csv_text), delimiter=delim)

    rows = []
    for row in reader:
        rows.append(_norm_cols(row))

    if not rows:
        return {"error": "No data rows found in CSV"}

    sample_keys = list(rows[0].keys()) if rows else []
    raw_first_row = next(csv.DictReader(io.StringIO(csv_text), delimiter=delim), {})
    broker = detect_broker(list(raw_first_row.keys())) if raw_first_row else 'Generic'

    fills = defaultdict(lambda: {"buys": [], "sells": []})
    executed_count = 0
    rejected_count = 0
    total_buy_val  = 0.0
    total_sell_val = 0.0
    skipped = 0
    timed_trades = []
    trade_dates  = []   # collected so we know if today == any expiry

    # Seed fills with carry-in so VWAP P&L is computed against the real entry price.
    # IMPORTANT: do NOT add carry-in qty*price into total_buy_val/total_sell_val.
    # Those totals feed compute_charges(); the prior session has already been
    # charged STT/exchange/etc. on that same value, so adding it here would
    # double-charge today's session and depress today's net P&L.
    carry_seeded = set()
    for c in carry_in:
        try:
            sym = c['symbol'].strip().upper()
            side = c['side']
            cqty = float(c['qty']); cpx = float(c['avg_price'])
        except (KeyError, TypeError, ValueError):
            continue
        if cqty <= 0 or cpx <= 0:
            continue
        if side == 'SHORT':
            fills[sym]["sells"].append({"qty": cqty, "price": cpx, "carry": True})
        elif side == 'LONG':
            fills[sym]["buys"].append({"qty": cqty, "price": cpx, "carry": True})
        carry_seeded.add(sym)

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
        time_raw = _get(row, TIME_COLS)

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
            if side_raw in ('B',): is_buy = True
            elif side_raw in ('S',): is_sell = True
            else:
                warnings.append(f"Unknown side '{side_raw}' for {symbol}")
                continue

        executed_count += 1
        value = qty * px
        t = _parse_time(time_raw)
        d = _parse_trade_date(time_raw)
        if d:
            trade_dates.append(d)

        if is_buy:
            fills[symbol]["buys"].append({"qty": qty, "price": px})
            total_buy_val += value
            if t:
                timed_trades.append((t, symbol, 'BUY', qty, px))
        else:
            fills[symbol]["sells"].append({"qty": qty, "price": px})
            total_sell_val += value
            if t:
                timed_trades.append((t, symbol, 'SELL', qty, px))

    if not fills:
        col_hint = f"Detected columns: {', '.join(sample_keys[:12])}"
        return {"error": f"No valid executed trades found. {col_hint}. Check CSV format or column names."}

    if skipped > 0:
        warnings.append(f"{skipped} rows skipped (pending/open/unknown status)")

    # Most-frequent trade date is the session date (used for ELM expiry-day check)
    session_date = max(set(trade_dates), key=trade_dates.count) if trade_dates else None

    total_turnover = total_buy_val + total_sell_val

    strikes = []
    open_positions_out = []   # to persist back into OpenPosition table
    carry_in_realized = 0.0   # P&L specifically attributable to carry positions
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
        was_carry_in = symbol in carry_seeded
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
            "is_carry":    open_qty > 0,
            "had_carry_in": was_carry_in,
        }
        strikes.append(strike_data)
        gross_pnl += realized
        if was_carry_in:
            carry_in_realized += realized
        opt = sym_info["option_type"]
        if opt == "CE": total_ce_pnl += realized
        elif opt == "PE": total_pe_pnl += realized

        # Snapshot any still-open position for OpenPosition persistence
        if open_qty > 0:
            new_side = 'LONG' if open_side == 'BUY' else 'SHORT'
            new_avg  = buy_vwap if new_side == 'LONG' else sell_vwap
            open_positions_out.append({
                "symbol":      symbol,
                "side":        new_side,
                "qty":         open_qty,
                "avg_price":   round(new_avg, 2),
                "expiry":      sym_info["expiry"],
                "strike":      sym_info["strike"],
                "option_type": sym_info["option_type"],
                "index_name":  sym_info["index"],
            })

    charges = compute_charges(total_buy_val, total_sell_val, total_turnover)
    net_pnl = gross_pnl - charges["total"]

    indices    = [s["index"] for s in strikes if s["index"]]
    index_name = max(set(indices), key=indices.count) if indices else "Unknown"

    strikes.sort(key=lambda x: x["realized"], reverse=True)

    # ── Intraday cumulative P&L curve + margin time-series ──
    # We rebuild per-symbol running book at each tick. Margin is recomputed
    # against current open qty using SHORT_MARGIN_PCT + ELM on expiry day.
    time_pnl = {}
    margin_ts = {}
    if timed_trades:
        timed_trades.sort(key=lambda x: x[0])

        # Pre-seed running book with carry-in so realised P&L delta starts from
        # the correct baseline (carry already counted in fills above).
        running = defaultdict(lambda: {"buys": [], "sells": [], "last_realized": 0.0})
        for c in carry_in:
            try:
                sym = c['symbol'].strip().upper()
                side = c['side']; cqty = float(c['qty']); cpx = float(c['avg_price'])
            except Exception:
                continue
            if cqty <= 0 or cpx <= 0:
                continue
            if side == 'SHORT':
                running[sym]["sells"].append({"qty": cqty, "price": cpx})
            elif side == 'LONG':
                running[sym]["buys"].append({"qty": cqty, "price": cpx})

        cum_total = 0.0
        for t, sym, side, qty, px in timed_trades:
            book = running[sym]
            if side == 'BUY':
                book["buys"].append({"qty": qty, "price": px})
            else:
                book["sells"].append({"qty": qty, "price": px})
            bq = sum(f["qty"] for f in book["buys"])
            sq = sum(f["qty"] for f in book["sells"])
            bvwap = (sum(f["qty"]*f["price"] for f in book["buys"]) / bq) if bq else 0
            svwap = (sum(f["qty"]*f["price"] for f in book["sells"]) / sq) if sq else 0
            matched = min(bq, sq)
            new_realized = round((svwap - bvwap) * matched, 2) if matched > 0 else 0.0
            delta = new_realized - book["last_realized"]
            book["last_realized"] = new_realized
            cum_total += delta
            time_pnl[t] = round(cum_total, 2)

            # Margin snapshot at this tick — uses spread/hedge-aware model.
            # Same timestamp ticks that occur in the same minute keep only the
            # last value (margin only changes when book changes, last is correct).
            legs = _open_legs_from_book(running)
            margin_ts[t] = round(_compute_total_margin(legs, session_date), 2)

    # Final margin snapshot (end-of-session) for the headroom card
    final_margin = max(margin_ts.values()) if margin_ts else 0.0
    peak_margin  = final_margin   # peak == max of curve (already)
    end_margin   = list(margin_ts.values())[-1] if margin_ts else 0.0

    return {
        "index_name":    index_name,
        "broker":        broker,
        "session_date":  session_date,
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
        "carry_positions":     [s for s in strikes if s["is_carry"]],
        "open_positions_out":  open_positions_out,   # → write to OpenPosition table
        "carry_in_count":      len(carry_in),
        "carry_in_realized":   round(carry_in_realized, 2),
        "time_pnl":      time_pnl,
        "margin_ts":     margin_ts,
        "peak_margin":   round(peak_margin, 2),
        "end_margin":    round(end_margin, 2),
    }
