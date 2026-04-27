"""
Indian Options Regulatory Charge Engine
Covers: STT, NSE Exchange Fee, SEBI Charges, Stamp Duty, GST
Post-Budget 2024 rates
"""

def compute_charges(
    total_buy_value: float,
    total_sell_value: float,
    total_turnover: float
) -> dict:
    """
    Compute all regulatory charges for Indian options trading.

    Args:
        total_buy_value:  Sum of (premium * qty) for all buy orders
        total_sell_value: Sum of (premium * qty) for all sell orders
        total_turnover:   total_buy_value + total_sell_value

    Returns:
        dict with individual charges and total
    """
    # STT: 0.1% on sell side only (options sell)
    stt = total_sell_value * 0.001

    # NSE Exchange Transaction Fee: 0.053% on total turnover
    exchange = total_turnover * 0.00053

    # SEBI Charges: ₹10 per crore of turnover
    sebi = (total_turnover / 1_00_00_000) * 10

    # Stamp Duty: 0.003% on buy side only
    stamp = total_buy_value * 0.00003

    # GST: 18% on (exchange fee + SEBI charges)
    gst = (exchange + sebi) * 0.18

    total = stt + exchange + sebi + stamp + gst

    return {
        "stt":      round(stt, 2),
        "exchange": round(exchange, 2),
        "sebi":     round(sebi, 2),
        "stamp":    round(stamp, 2),
        "gst":      round(gst, 2),
        "total":    round(total, 2)
    }
