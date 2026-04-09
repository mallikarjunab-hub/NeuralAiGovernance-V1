"""
Forecast Service (Enhancement 5)
=================================
Detects "forecast / predict / project / next N months" questions,
runs a simple linear regression on payment_summary data, and returns:
  - actual rows (from DB)
  - projected rows (from regression)
  - chart_type = "forecast"

The frontend renders actual in blue (solid) and projected in orange (dashed)
on the same line chart using forecastChart().
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── Forecast question detection ───────────────────────────────────────────────

_FORECAST_PATTERNS = [
    "forecast", "predict", "projection", "projected",
    "next 3 months", "next 6 months", "next year",
    "expected next", "will there be", "anticipate",
    "future trend", "growth forecast", "payment forecast",
]


def is_forecast_question(question: str) -> bool:
    """Return True if the question is asking for a forecast/projection."""
    q = question.lower()
    return any(p in q for p in _FORECAST_PATTERNS)


# ── Linear regression helper ──────────────────────────────────────────────────

def _linear_regression(x: list[float], y: list[float]):
    """
    Simple OLS linear regression: y = a*x + b.
    Returns (slope, intercept). Pure Python — no numpy needed.
    """
    n = len(x)
    if n < 2:
        return 0.0, (y[0] if y else 0.0)
    sx, sy, sxy, sxx = sum(x), sum(y), sum(xi * yi for xi, yi in zip(x, y)), sum(xi**2 for xi in x)
    denom = n * sxx - sx * sx
    if denom == 0:
        return 0.0, sy / n
    a = (n * sxy - sx * sy) / denom
    b = (sy - a * sx) / n
    return a, b


# ── Forecast computation ──────────────────────────────────────────────────────

_YEAR_LABEL_COLS = {
    "year", "payment_year", "registration_year", "fiscal_year",
    "due_year", "month", "period", "month_name", "quarter_label",
}


async def compute_forecast(actual_rows: list[dict], n_periods: int = 3) -> dict:
    """
    Given actual payment_summary rows (already sorted by year/month),
    compute the next n_periods projected values via linear regression.

    Returns:
        {
          "actuals":    [{"label": "2023", "paid_count": 12345, ...}],
          "projections":[{"label": "2027 (proj)", "paid_count": 13000}],
          "metric":     "paid_count",
          "chart_type": "forecast",
        }
    """
    if not actual_rows:
        return {}

    cols = list(actual_rows[0].keys())

    # Year/month/period columns are always labels even though their values are numeric
    def _is_label_col(c: str) -> bool:
        cl = c.lower()
        return cl in _YEAR_LABEL_COLS or cl.endswith("_year") or cl.endswith("_month")

    lbl_cols = [c for c in cols if _is_label_col(c)]
    num_cols  = [c for c in cols if not _is_label_col(c) and _is_numeric_col(actual_rows, c)]

    # Fallback: if no explicit label col, use first col as label
    if not lbl_cols and cols:
        lbl_cols = [cols[0]]
        num_cols = [c for c in cols[1:] if _is_numeric_col(actual_rows, c)]

    if not num_cols or not lbl_cols:
        return {}

    lbl_col    = lbl_cols[0]
    metric_col = _best_metric(num_cols)  # prefer paid_count or total_paid

    # Assign integer indices as x-axis for regression
    x = list(range(len(actual_rows)))
    y = [_safe_float(row.get(metric_col)) for row in actual_rows]

    slope, intercept = _linear_regression(x, y)

    # Build projected rows
    last_lbl = str(actual_rows[-1].get(lbl_col, ""))
    projected = []
    for i in range(1, n_periods + 1):
        xi   = len(actual_rows) - 1 + i
        yhat = max(0.0, slope * xi + intercept)
        try:
            base = int(last_lbl.split("-")[0])  # "2025" or "2025-04"
            proj_lbl = f"{base + i} (proj)"
        except (ValueError, IndexError):
            proj_lbl = f"Period +{i} (proj)"
        projected.append({"label": proj_lbl, metric_col: round(yhat, 2)})

    actuals_out = [{"label": str(r.get(lbl_col, i)), metric_col: _safe_float(r.get(metric_col))}
                   for i, r in enumerate(actual_rows)]

    return {
        "actuals":     actuals_out,
        "projections": projected,
        "metric":      metric_col,
        "chart_type":  "forecast",
        "slope":       round(slope, 4),
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_numeric_col(rows: list[dict], col: str) -> bool:
    vals = [rows[i].get(col) for i in range(min(5, len(rows)))]
    return all(_safe_float(v) is not None for v in vals if v is not None)


def _safe_float(v) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _best_metric(num_cols: list[str]) -> str:
    """Prefer meaningful payment metrics over raw IDs."""
    preferred = ["paid_count", "total_paid", "total_net_amount",
                 "success_rate_pct", "registrations", "count"]
    for p in preferred:
        if p in num_cols:
            return p
    return num_cols[0]
