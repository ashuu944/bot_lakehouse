"""Curated table descriptions and suggested-analysis text for every
Bronze/Silver/Gold dataset -- the single source both catalog_builder.py (runs
in the Spark container) and openmetadata-ingestion/set_catalog_metadata.py
(runs in the lightweight openmetadata-ingestion container, which has no
pyspark) read from. Deliberately has zero pyspark/delta imports so either
side can import it.
"""

from __future__ import annotations

BSIS_DESCRIPTIONS: dict[str, tuple[str, str]] = {
    "smr_daily_compliance": ("Daily statutory minimum reserve (SMR) compliance per institution.",
                              "Day-to-day SMR breach detection, compliance trend."),
    "liquidity_compliance": ("Daily liquid-assets-to-demand-liabilities compliance ratio.",
                              "Daily liquidity compliance trend, breach alerts."),
    "net_open_position": ("Daily net open foreign-exchange position.",
                           "FX risk exposure monitoring."),
    "lcr": ("Monthly Liquidity Coverage Ratio (Basel III).",
             "Liquidity risk trend analysis, regulatory threshold breaches."),
    "nsfr": ("Monthly Net Stable Funding Ratio (Basel III).",
              "Funding stability trend analysis."),
    "capital_adequacy": ("Monthly capital adequacy ratios (Tier 1, Total Capital, Leverage).",
                          "Solvency/capital-strength trend analysis -- CAMELS 'C'."),
    "sensitivity_market_risk": ("Monthly market risk sensitivity measures.",
                                 "Interest-rate / market-risk exposure analysis."),
    "asset_quality": ("Monthly asset quality rating and related metrics.",
                       "Asset quality trend analysis -- CAMELS 'A'."),
    "credit_exposure": ("Monthly credit exposure by economic sector, with performing/non-performing breakdown.",
                         "Sector concentration risk, NPL-by-sector, institution-sector exposure graphs."),
    "balance_sheet": ("Monthly full balance sheet, one column per line item (pivoted from Oracle's long format).",
                       "Balance sheet structure analysis, peer comparison."),
    "earnings": ("Monthly earnings/profitability metrics.",
                 "Profitability trend analysis -- CAMELS 'E'."),
    "income_statement": ("Monthly income statement, monthly + year-to-date amount per line item (pivoted).",
                          "P&L trend analysis, revenue/expense breakdown."),
}

MASTER_DESCRIPTIONS: dict[str, str] = {
    "institutions": "Registered financial institutions (code, name, status) -- the join key for every BSIS fact table.",
    "institution_categories": "Institution category reference (e.g. commercial bank, community bank).",
    "institution_types": "Institution type reference.",
    "sectors": "Economic sector reference -- joined against credit_exposure.",
    "risk_types": "Risk type reference used across risk datasets.",
    "regulatory_thresholds": "Regulatory minimum/maximum thresholds per ratio (e.g. minimum LCR).",
    "reporting_frequencies": "Reference for how often each BSIS report is submitted.",
    "reporting_obligations": "Which institutions must submit which reports.",
    "dataset_types": "BSIS dataset/report type reference.",
    "camels_rating_criteria": "CAMELS rating scale/criteria reference.",
    "holidays": "Bank holiday calendar -- relevant for daily-report date logic.",
}

GOLD_DESCRIPTIONS: dict[str, tuple[str, str]] = {
    "sector_exposure": (
        "Institution x sector x month credit exposure, with total_exposure and npl_ratio derived.",
        "Sector concentration risk, NPL-by-sector; edge-shaped for a future Neo4j institution-sector graph.",
    ),
    "institution_risk_trends": (
        "Institution x month view of LCR, NSFR, and capital-adequacy ratios side by side.",
        "Power BI trend dashboards: one ratio's trend per institution, or compare institutions.",
    ),
}
