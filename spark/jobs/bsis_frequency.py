"""BSIS dataset reporting-cadence map, for date-range (backfill) extraction.

bsis_extractor itself only knows whether a dataset *requires* a date
(get_registry_info()'s requires_date) — not whether that date walks daily or
monthly. That cadence lives in BOT's existing production pipeline
(bank_profiling_risk_analysis/data_pipeline/config/source_config.yaml, the
`reporting_frequency` field per source) and is mirrored here so a date range
walks the right dates instead of e.g. querying a monthly dataset once per
calendar day.

deleted_returns is deliberately excluded: it's an audit/high-water-mark feed
(BSIS returns rows with deleted_date >= the date passed in), not a per-date
report — a date-range backfill doesn't apply to it the same way.
"""

from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta

DAILY_DATASETS = {
    "smr_daily_compliance",
    "liquidity_compliance",
    "net_open_position",
}

MONTHLY_DATASETS = {
    "lcr",
    "nsfr",
    "capital_adequacy",
    "sensitivity_market_risk",
    "asset_quality",
    "credit_exposure",
    "balance_sheet",
    "earnings",
    "income_statement",
}


def frequency_of(dataset: str) -> str | None:
    """Return 'daily', 'monthly', or None if the dataset isn't a dated fact report."""
    if dataset in DAILY_DATASETS:
        return "daily"
    if dataset in MONTHLY_DATASETS:
        return "monthly"
    return None


def _last_day_of_month(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def generate_date_range(frequency: str, start_date: str, end_date: str) -> list[date]:
    """Every report date between start and end (inclusive) for the given frequency.

    daily   -> every calendar day
    monthly -> the last day of every month touched by the range
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    freq = frequency.strip().lower()

    if freq == "daily":
        result = []
        cur = start
        while cur <= end:
            result.append(cur)
            cur += timedelta(days=1)
        return result

    if freq == "monthly":
        result = []
        year, month = start.year, start.month
        while True:
            candidate = _last_day_of_month(year, month)
            if candidate > end:
                break
            if candidate >= start:
                result.append(candidate)
            month += 1
            if month > 12:
                year, month = year + 1, 1
        return result

    raise ValueError(f"Unsupported frequency {frequency!r}. Expected 'daily' or 'monthly'.")
