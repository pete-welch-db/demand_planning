from __future__ import annotations

import random
from datetime import date, timedelta

import pandas as pd
from faker import Faker


fake = Faker()


REGIONS = ["Northeast", "Southeast", "Midwest", "SouthCentral", "West"]
FAMILIES = ["pipe", "chambers", "structures"]


def _weeks(n: int = 52, end: date | None = None) -> list[date]:
    end = end or date.today()
    # Align to Monday-ish by subtracting weekday
    end = end - timedelta(days=end.weekday())
    return [end - timedelta(days=7 * i) for i in range(n)][::-1]


def demand_vs_forecast_mock(n_weeks: int = 78) -> pd.DataFrame:
    random.seed(7)
    weeks = _weeks(n_weeks)
    rows = []
    for fam in FAMILIES:
        for reg in REGIONS:
            base = {"pipe": 18000, "chambers": 5200, "structures": 2400}[fam]
            reg_adj = {"Northeast": 0.9, "Southeast": 1.1, "Midwest": 1.0, "SouthCentral": 1.05, "West": 0.95}[reg]
            level = base * reg_adj
            for i, w in enumerate(weeks):
                seasonal = 0.18 * (1 + __import__("math").sin(2 * __import__("math").pi * (i % 52) / 52))
                actual = max(0, level * (0.85 + seasonal) * (0.9 + random.random() * 0.25))
                forecast = max(0, actual * (0.92 + random.random() * 0.16))
                rows.append(
                    {
                        "week": w,
                        "sku_family": fam,
                        "region": reg,
                        "actual_units": round(actual, 0),
                        "forecast_units": round(forecast, 0),
                    }
                )
    return pd.DataFrame(rows)


def control_tower_weekly_mock(n_weeks: int = 52) -> pd.DataFrame:
    random.seed(9)
    weeks = _weeks(n_weeks)
    rows = []
    for w in weeks:
        for reg in REGIONS:
            for fam in FAMILIES:
                otif = max(0.75, min(0.98, random.gauss(0.9, 0.04)))
                freight_cost_per_ton = max(80.0, random.gauss(135.0, 18.0))
                premium_pct = max(0.02, min(0.35, random.gauss(0.12, 0.06)))
                co2_per_ton = max(15.0, random.gauss(32.0, 6.0))
                energy_per_unit = max(0.5, random.gauss({"pipe": 1.9, "chambers": 3.3, "structures": 5.1}[fam], 0.25))
                rows.append(
                    {
                        "week": w,
                        "region": reg,
                        "plant_id": f"PL{random.randint(1,10):02d}",
                        "dc_id": f"DC{random.randint(1,10):02d}",
                        "otif_rate": otif,
                        "freight_cost_per_ton": freight_cost_per_ton,
                        "premium_freight_pct": premium_pct,
                        "co2_kg_per_ton": co2_per_ton,
                        "sku_family": fam,
                        "energy_kwh_per_unit": energy_per_unit,
                    }
                )
    return pd.DataFrame(rows)


def mape_by_family_region_mock() -> pd.DataFrame:
    random.seed(11)
    rows = []
    for fam in FAMILIES:
        for reg in REGIONS:
            m = max(0.06, min(0.28, random.gauss(0.14, 0.05)))
            rows.append({"sku_family": fam, "region": reg, "avg_mape": m})
    return pd.DataFrame(rows).sort_values("avg_mape", ascending=False)


def order_late_risk_mock(n_rows: int = 2000) -> pd.DataFrame:
    random.seed(13)
    rows = []
    today = date.today()
    for _ in range(n_rows):
        od = today - timedelta(days=random.randint(0, 120))
        fam = random.choice(FAMILIES)
        reg = random.choice(REGIONS)
        channel = random.choice(["distributor", "contractor", "DOT", "ag"])
        dc = f"DC{random.randint(1,10):02d}"
        pl = f"PL{random.randint(1,10):02d}"
        units = max(1, int(random.gauss(30 if fam == "pipe" else 10, 12)))
        dtr = max(1, int(random.gauss(7 if channel in ["contractor", "DOT"] else 9, 3)))
        # Risk: higher for short lead + large orders + contractor/DOT
        prob = 0.10 + (0.18 if dtr <= 5 else 0.0) + (0.10 if units >= 50 else 0.0) + (0.08 if channel in ["contractor", "DOT"] else 0.0)
        prob = max(0.01, min(0.95, prob + random.gauss(0.0, 0.05)))
        flag = 1 if prob >= 0.5 else 0
        actual = 1 if random.random() < prob * 0.9 else 0
        rows.append(
            {
                "order_date": od,
                "customer_region": reg,
                "channel": channel,
                "dc_id": dc,
                "plant_id": pl,
                "sku_family": fam,
                "units_ordered": units,
                "days_to_request": dtr,
                "late_risk_prob": prob,
                "late_risk_flag": flag,
                "actual_late": actual,
            }
        )
    return pd.DataFrame(rows)

