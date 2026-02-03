from __future__ import annotations

import random
from datetime import date, timedelta

import pandas as pd
from faker import Faker


fake = Faker()


REGIONS = ["Northeast", "Southeast", "Midwest", "SouthCentral", "West"]
FAMILIES = ["pipe", "chambers", "structures"]

# Real ADS (Advanced Drainage Systems) plant locations with coordinates
ADS_PLANTS = [
    {"plant_id": "PL01", "plant_name": "Hilliard OH Plant", "plant_city": "Hilliard", "plant_state": "OH", "plant_region": "Midwest", "plant_lat": 40.0334, "plant_lon": -83.1585},
    {"plant_id": "PL02", "plant_name": "Greenville TX Plant", "plant_city": "Greenville", "plant_state": "TX", "plant_region": "SouthCentral", "plant_lat": 33.1385, "plant_lon": -96.1108},
    {"plant_id": "PL03", "plant_name": "Mesa AZ Plant", "plant_city": "Mesa", "plant_state": "AZ", "plant_region": "West", "plant_lat": 33.4152, "plant_lon": -111.8315},
    {"plant_id": "PL04", "plant_name": "Hanceville AL Plant", "plant_city": "Hanceville", "plant_state": "AL", "plant_region": "Southeast", "plant_lat": 34.0606, "plant_lon": -86.7672},
    {"plant_id": "PL05", "plant_name": "Denver CO Plant", "plant_city": "Denver", "plant_state": "CO", "plant_region": "West", "plant_lat": 39.7392, "plant_lon": -104.9903},
    {"plant_id": "PL06", "plant_name": "Lebanon IN Plant", "plant_city": "Lebanon", "plant_state": "IN", "plant_region": "Midwest", "plant_lat": 40.0484, "plant_lon": -86.4692},
    {"plant_id": "PL07", "plant_name": "Jerome ID Plant", "plant_city": "Jerome", "plant_state": "ID", "plant_region": "West", "plant_lat": 42.7249, "plant_lon": -114.5185},
    {"plant_id": "PL08", "plant_name": "Hamilton OH Plant", "plant_city": "Hamilton", "plant_state": "OH", "plant_region": "Midwest", "plant_lat": 39.3995, "plant_lon": -84.5613},
    {"plant_id": "PL09", "plant_name": "Apple Creek OH Plant", "plant_city": "Apple Creek", "plant_state": "OH", "plant_region": "Midwest", "plant_lat": 40.7509, "plant_lon": -81.8410},
    {"plant_id": "PL10", "plant_name": "Leland NC Plant", "plant_city": "Leland", "plant_state": "NC", "plant_region": "Southeast", "plant_lat": 34.2257, "plant_lon": -78.0453},
    {"plant_id": "PL11", "plant_name": "Salt Lake City UT Plant", "plant_city": "Salt Lake City", "plant_state": "UT", "plant_region": "West", "plant_lat": 40.7608, "plant_lon": -111.8910},
    {"plant_id": "PL12", "plant_name": "Princeton MN Plant", "plant_city": "Princeton", "plant_state": "MN", "plant_region": "Midwest", "plant_lat": 45.5688, "plant_lon": -93.5816},
    {"plant_id": "PL13", "plant_name": "Harrisburg PA Plant", "plant_city": "Harrisburg", "plant_state": "PA", "plant_region": "Northeast", "plant_lat": 40.2732, "plant_lon": -76.8867},
    {"plant_id": "PL14", "plant_name": "Waterloo IA Plant", "plant_city": "Waterloo", "plant_state": "IA", "plant_region": "Midwest", "plant_lat": 42.4928, "plant_lon": -92.3426},
    {"plant_id": "PL15", "plant_name": "Auburndale FL Plant", "plant_city": "Auburndale", "plant_state": "FL", "plant_region": "Southeast", "plant_lat": 28.0653, "plant_lon": -81.7887},
    {"plant_id": "PL16", "plant_name": "Martinsburg WV Plant", "plant_city": "Martinsburg", "plant_state": "WV", "plant_region": "Northeast", "plant_lat": 39.4562, "plant_lon": -77.9639},
]

# Real ADS distribution center locations with coordinates
ADS_DCS = [
    {"dc_id": "DC01", "dc_name": "Hilliard OH DC", "dc_city": "Hilliard", "dc_state": "OH", "dc_region": "Midwest", "dc_lat": 40.0334, "dc_lon": -83.1585},
    {"dc_id": "DC02", "dc_name": "Dallas TX DC", "dc_city": "Dallas", "dc_state": "TX", "dc_region": "SouthCentral", "dc_lat": 32.7767, "dc_lon": -96.7970},
    {"dc_id": "DC03", "dc_name": "Phoenix AZ DC", "dc_city": "Phoenix", "dc_state": "AZ", "dc_region": "West", "dc_lat": 33.4484, "dc_lon": -112.0740},
    {"dc_id": "DC04", "dc_name": "Atlanta GA DC", "dc_city": "Atlanta", "dc_state": "GA", "dc_region": "Southeast", "dc_lat": 33.7490, "dc_lon": -84.3880},
    {"dc_id": "DC05", "dc_name": "Charlotte NC DC", "dc_city": "Charlotte", "dc_state": "NC", "dc_region": "Southeast", "dc_lat": 35.2271, "dc_lon": -80.8431},
    {"dc_id": "DC06", "dc_name": "Indianapolis IN DC", "dc_city": "Indianapolis", "dc_state": "IN", "dc_region": "Midwest", "dc_lat": 39.7684, "dc_lon": -86.1581},
    {"dc_id": "DC07", "dc_name": "Denver CO DC", "dc_city": "Denver", "dc_state": "CO", "dc_region": "West", "dc_lat": 39.7392, "dc_lon": -104.9903},
    {"dc_id": "DC08", "dc_name": "Minneapolis MN DC", "dc_city": "Minneapolis", "dc_state": "MN", "dc_region": "Midwest", "dc_lat": 44.9778, "dc_lon": -93.2650},
    {"dc_id": "DC09", "dc_name": "Philadelphia PA DC", "dc_city": "Philadelphia", "dc_state": "PA", "dc_region": "Northeast", "dc_lat": 39.9526, "dc_lon": -75.1652},
    {"dc_id": "DC10", "dc_name": "Tampa FL DC", "dc_city": "Tampa", "dc_state": "FL", "dc_region": "Southeast", "dc_lat": 27.9506, "dc_lon": -82.4572},
    {"dc_id": "DC11", "dc_name": "Seattle WA DC", "dc_city": "Seattle", "dc_state": "WA", "dc_region": "West", "dc_lat": 47.6062, "dc_lon": -122.3321},
]


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


# SKU part numbers by family (realistic ADS-style naming)
SKU_PARTS = {
    "pipe": [
        {"sku_id": "PP-4-100", "sku_name": "4\" Corrugated Pipe 100ft"},
        {"sku_id": "PP-6-100", "sku_name": "6\" Corrugated Pipe 100ft"},
        {"sku_id": "PP-8-100", "sku_name": "8\" Corrugated Pipe 100ft"},
        {"sku_id": "PP-10-20", "sku_name": "10\" Corrugated Pipe 20ft"},
        {"sku_id": "PP-12-20", "sku_name": "12\" Corrugated Pipe 20ft"},
        {"sku_id": "PP-15-20", "sku_name": "15\" Corrugated Pipe 20ft"},
        {"sku_id": "PP-18-20", "sku_name": "18\" Corrugated Pipe 20ft"},
        {"sku_id": "PP-24-20", "sku_name": "24\" Corrugated Pipe 20ft"},
        {"sku_id": "SP-4-10", "sku_name": "4\" Solid Pipe 10ft"},
        {"sku_id": "SP-6-10", "sku_name": "6\" Solid Pipe 10ft"},
    ],
    "chambers": [
        {"sku_id": "SC3-STD", "sku_name": "StormTech SC-310 Chamber"},
        {"sku_id": "SC4-STD", "sku_name": "StormTech SC-450 Chamber"},
        {"sku_id": "SC7-STD", "sku_name": "StormTech SC-740 Chamber"},
        {"sku_id": "MC35-STD", "sku_name": "StormTech MC-3500 Chamber"},
        {"sku_id": "MC45-STD", "sku_name": "StormTech MC-4500 Chamber"},
        {"sku_id": "DC78-STD", "sku_name": "StormTech DC-780 Chamber"},
        {"sku_id": "EC-END", "sku_name": "End Cap Assembly"},
        {"sku_id": "EC-ISOL", "sku_name": "Isolator Row Assembly"},
    ],
    "structures": [
        {"sku_id": "NDS-12", "sku_name": "12\" Nyloplast Drain"},
        {"sku_id": "NDS-18", "sku_name": "18\" Nyloplast Drain"},
        {"sku_id": "NDS-24", "sku_name": "24\" Nyloplast Drain"},
        {"sku_id": "CB-18", "sku_name": "18\" Catch Basin"},
        {"sku_id": "CB-24", "sku_name": "24\" Catch Basin"},
        {"sku_id": "CB-30", "sku_name": "30\" Catch Basin"},
        {"sku_id": "MH-48", "sku_name": "48\" Manhole"},
        {"sku_id": "MH-60", "sku_name": "60\" Manhole"},
    ],
}


def mape_by_sku_mock(sku_family: str = None) -> pd.DataFrame:
    """Generate mock SKU-level MAPE data."""
    random.seed(42)
    rows = []
    
    families_to_use = [sku_family] if sku_family else FAMILIES
    
    for fam in families_to_use:
        if fam not in SKU_PARTS:
            continue
        # Get base MAPE for family (from family-level mock)
        base_mape = {"pipe": 0.12, "chambers": 0.18, "structures": 0.22}[fam]
        
        for sku in SKU_PARTS[fam]:
            for reg in REGIONS:
                # Vary MAPE by SKU and region
                sku_adj = random.gauss(0, 0.04)
                reg_adj = {"Northeast": -0.02, "Southeast": 0.01, "Midwest": -0.01, "SouthCentral": 0.02, "West": 0.0}[reg]
                mape = max(0.03, min(0.45, base_mape + sku_adj + reg_adj + random.gauss(0, 0.03)))
                weeks = random.randint(8, 52)
                
                rows.append({
                    "sku_id": sku["sku_id"],
                    "sku_name": sku["sku_name"],
                    "sku_family": fam,
                    "region": reg,
                    "avg_mape": round(mape, 4),
                    "weeks_measured": weeks,
                })
    
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


def plant_locations_mock() -> pd.DataFrame:
    """Return all ADS plant locations with coordinates."""
    return pd.DataFrame(ADS_PLANTS)


def dc_locations_mock() -> pd.DataFrame:
    """Return all ADS DC locations with coordinates."""
    return pd.DataFrame(ADS_DCS)


def freight_lanes_mock() -> pd.DataFrame:
    """Generate mock freight lanes between plants and DCs with costs."""
    random.seed(42)
    rows = []
    
    # Create realistic lanes based on regional proximity
    for plant in ADS_PLANTS:
        # Each plant ships to 3-5 DCs
        num_dcs = random.randint(3, 5)
        # Prefer DCs in same or adjacent regions
        same_region_dcs = [dc for dc in ADS_DCS if dc["dc_region"] == plant["plant_region"]]
        other_dcs = [dc for dc in ADS_DCS if dc["dc_region"] != plant["plant_region"]]
        
        # Pick DCs: prioritize same region
        selected_dcs = same_region_dcs[:2] if same_region_dcs else []
        remaining = num_dcs - len(selected_dcs)
        if remaining > 0 and other_dcs:
            selected_dcs.extend(random.sample(other_dcs, min(remaining, len(other_dcs))))
        
        for dc in selected_dcs:
            # Calculate approximate distance for cost scaling
            lat_diff = abs(plant["plant_lat"] - dc["dc_lat"])
            lon_diff = abs(plant["plant_lon"] - dc["dc_lon"])
            approx_distance = ((lat_diff ** 2 + lon_diff ** 2) ** 0.5) * 69  # rough miles
            
            # Freight cost: base + distance factor + regional adjustment
            base_cost = 85.0
            distance_cost = approx_distance * 0.08
            regional_adj = random.gauss(0, 15)
            freight_cost = max(75.0, base_cost + distance_cost + regional_adj)
            
            # CO2 scales with distance and cost
            co2_kg_per_ton = max(15.0, 18.0 + (approx_distance * 0.02) + random.gauss(0, 5))
            
            # Volume (shipments per week)
            volume = max(10, int(random.gauss(80, 30)))
            
            rows.append({
                "plant_id": plant["plant_id"],
                "plant_name": plant["plant_name"],
                "plant_lat": plant["plant_lat"],
                "plant_lon": plant["plant_lon"],
                "dc_id": dc["dc_id"],
                "dc_name": dc["dc_name"],
                "dc_lat": dc["dc_lat"],
                "dc_lon": dc["dc_lon"],
                "freight_cost_per_ton": round(freight_cost, 2),
                "co2_kg_per_ton": round(co2_kg_per_ton, 1),
                "weekly_volume": volume,
            })
    
    return pd.DataFrame(rows).sort_values("freight_cost_per_ton", ascending=False)


def order_volume_kpis_mock() -> pd.DataFrame:
    """Mock order volume and customer metrics (13 weeks)."""
    return pd.DataFrame([{
        "total_orders": 82_275,  # ~329,100 / 4 quarters
        "unique_customers": 1_245,
        "sales_channels": 4,
        "total_units_ordered": 2_600_000,
    }])


def service_performance_kpis_mock() -> pd.DataFrame:
    """Mock service performance metrics."""
    return pd.DataFrame([{
        "perfect_order_rate": 0.912,
        "cancellation_rate": 0.020,
        "backorder_rate": 0.078,
    }])


def transport_mode_comparison_mock() -> pd.DataFrame:
    """Mock transport mode cost and CO2 comparison."""
    return pd.DataFrame([
        {"transport_mode": "own_fleet", "freight_cost_per_ton": 408.0, "co2_kg_per_ton": 59.73, "tons_shipped": 145000},
        {"transport_mode": "carrier", "freight_cost_per_ton": 508.0, "co2_kg_per_ton": 70.13, "tons_shipped": 87000},
    ])


def product_family_mix_mock() -> pd.DataFrame:
    """Mock product family volume distribution."""
    return pd.DataFrame([
        {"sku_family": "pipe", "total_units": 2_210_000, "pct_of_total": 85.0},
        {"sku_family": "chambers", "total_units": 260_000, "pct_of_total": 10.0},
        {"sku_family": "structures", "total_units": 130_000, "pct_of_total": 5.0},
    ])


def orders_by_channel_mock() -> pd.DataFrame:
    """Mock orders and OTIF by sales channel."""
    return pd.DataFrame([
        {"sales_channel": "distributor", "order_count": 35_000, "units_ordered": 1_100_000, "otif_rate": 0.94},
        {"sales_channel": "contractor", "order_count": 28_000, "units_ordered": 880_000, "otif_rate": 0.91},
        {"sales_channel": "DOT", "order_count": 12_000, "units_ordered": 420_000, "otif_rate": 0.89},
        {"sales_channel": "ag", "order_count": 7_275, "units_ordered": 200_000, "otif_rate": 0.93},
    ])

