import json
import math
import pandas as pd
from pathlib import Path
from sqlalchemy import text
from typing import TypedDict

from source.utils.db import get_engine

import logging
logger = logging.getLogger(__name__)

class ZoneShape(TypedDict):
    id: int
    polygons: list[list[list[float]]]
    borough: str


class ZoneCenter(TypedDict):
    id: int
    x: float
    y: float


_ZONE_CACHE = None

def load_zone_shapes():
    global _ZONE_CACHE
    if _ZONE_CACHE is None:
        path = Path(__file__).resolve().parent / "zone_shapes.json"
        logger.info("Loading zone shapes from %s", path)
        with open(path, "r", encoding="utf-8") as f:
            _ZONE_CACHE = json.load(f)["zones"]
    return _ZONE_CACHE


def mock_polygon_center(points: list[list[float]]) -> tuple[float, float]:
    if not points:
        return 0.0, 0.0

    sx = 0.0
    sy = 0.0
    for x, y in points:
        sx += x
        sy += y

    return sx / len(points), sy / len(points)


def mock_zone_centers(zones: list[ZoneShape]) -> list[ZoneCenter]:
    centers: list[ZoneCenter] = []

    for zone in zones:
        ring = zone["polygons"][0] if zone["polygons"] else []
        x, y = mock_polygon_center(ring)
        centers.append({
            "id": zone["id"],
            "x": x,
            "y": y,
        })

    return centers


def get_nearby_zone_ids(
    selected_ids: list[int],
    n_nearest: int = 4,
) -> list[int]:
    logger.info("Computing nearby zones | selected_ids=%s | n_nearest=%s", selected_ids, n_nearest)

    zones = load_zone_shapes()
    centers = mock_zone_centers(zones)
    center_by_id = {c["id"]: c for c in centers}
    selected_set = set(selected_ids)
    nearby: set[int] = set()

    for selected_id in selected_ids:
        c = center_by_id.get(selected_id)
        if c is None:
            continue

        nearest = (
            sorted(
                (
                    {
                        "id": other["id"],
                        "d": ((other["x"] - c["x"]) ** 2 + (other["y"] - c["y"]) ** 2) ** 0.5,
                    }
                    for other in centers
                    if other["id"] != selected_id and other["id"] not in selected_set
                ),
                key=lambda item: item["d"],
            )[:n_nearest]
        )

        for item in nearest:
            nearby.add(item["id"])

    logger.info("Nearby zones computed | result=%s", list(nearby))
    return list(nearby)


def get_zone_metadata(zone_ids):
    engine = get_engine()

    query = text("""
        SELECT location_id, borough, service_zone
        FROM dim_zone
        WHERE location_id = ANY(:zone_ids)
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"zone_ids": zone_ids})

    if df.empty:
        return {}

    zone_meta = {}
    for _, row in df.iterrows():
        zone_id = int(row["location_id"])

        borough = None
        if pd.notna(row["borough"]):
            borough = str(row["borough"]).strip()

        service_zone = None
        if pd.notna(row["service_zone"]):
            service_zone = str(row["service_zone"]).strip()

        zone_meta[zone_id] = {
            "borough": borough,
            "service_zone": service_zone,
        }

    logger.info("Zone metadata query returned %s rows", len(df))
    return zone_meta


def build_zone_feature_rows(included_zone_ids, selected_zone_ids, timestamp, zone_meta):
    hour = timestamp.hour
    day_of_week = timestamp.weekday()
    month = timestamp.month
    is_weekend = 1 if day_of_week >= 5 else 0

    hour_sin = math.sin(2 * math.pi * hour / 24)
    hour_cos = math.cos(2 * math.pi * hour / 24)
    dow_sin = math.sin(2 * math.pi * day_of_week / 7)
    dow_cos = math.cos(2 * math.pi * day_of_week / 7)

    selected_set = set(selected_zone_ids)
    rows = []

    for zone_id in included_zone_ids:
        meta = zone_meta.get(zone_id, {})
        borough = meta.get("borough")
        service_zone = meta.get("service_zone")

        borough_lower = borough.lower() if borough else None

        row = {
            "pulocationid": zone_id,
            "hour_ts": timestamp,
            "hour": hour,
            "day_of_week": day_of_week,
            "month": month,
            "is_weekend": is_weekend,
            "hour_sin": hour_sin,
            "hour_cos": hour_cos,
            "dow_sin": dow_sin,
            "dow_cos": dow_cos,
            "borough": borough,
            "service_zone": service_zone,
            "source": "selected" if zone_id in selected_set else "nearby",

            "borough_manhattan": 1 if borough_lower == "manhattan" else 0,
            "borough_brooklyn": 1 if borough_lower == "brooklyn" else 0,
            "borough_queens": 1 if borough_lower == "queens" else 0,
            "borough_bronx": 1 if borough_lower == "bronx" else 0,
            "borough_staten island": 1 if borough_lower == "staten island" else 0,
            "borough_ewr": 1 if borough_lower == "ewr" else 0,
            "borough_nan": 1 if not borough else 0,

            "service_zone_Yellow_Zone": 1 if service_zone == "Yellow Zone" else 0,
            "service_zone_Boro_Zone": 1 if service_zone == "Boro Zone" else 0,
            "service_zone_Airports": 1 if service_zone == "Airports" else 0,
            "service_zone_EWR": 1 if service_zone == "EWR" else 0,
            "service_zone_nan": 1 if not service_zone else 0,
        }

        rows.append(row)

    return rows