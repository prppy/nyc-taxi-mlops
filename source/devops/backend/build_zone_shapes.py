#!/usr/bin/env python3
"""Build frontend-friendly zone_shapes.json from NYC taxi zone shapefile."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import shapefile  # pyshp


def simplify_points(points: list[list[float]], step: int) -> list[list[float]]:
    if step <= 1 or len(points) <= 30:
        return points
    reduced = points[::step]
    if reduced[-1] != points[-1]:
        reduced.append(points[-1])
    return reduced


def normalize_point(x: float, y: float, bbox: list[float], precision: int) -> list[float]:
    min_x, min_y, max_x, max_y = bbox
    dx = max(max_x - min_x, 1e-12)
    dy = max(max_y - min_y, 1e-12)
    nx = (x - min_x) / dx
    ny = 1.0 - ((y - min_y) / dy)
    return [round(nx, precision), round(ny, precision)]


def shape_to_polygons(
    shape: shapefile.Shape, bbox: list[float], precision: int, simplify_step: int
) -> list[list[list[float]]]:
    points = shape.points
    if not points:
        return []

    parts = list(shape.parts) + [len(points)]
    polygons: list[list[list[float]]] = []

    for idx in range(len(parts) - 1):
        start = parts[idx]
        end = parts[idx + 1]
        ring = [
            normalize_point(x, y, bbox, precision)
            for x, y in points[start:end]
        ]
        if ring:
            polygons.append(simplify_points(ring, simplify_step))

    return polygons


def pick_field(record: dict[str, Any], *candidates: str, default: str = "") -> Any:
    for key in candidates:
        if key in record and record[key] is not None:
            return record[key]
    return default


def build_zone_shapes(
    input_path: Path, output_path: Path, precision: int, simplify_step: int
) -> dict[str, Any]:
    reader = shapefile.Reader(str(input_path))
    bbox = [float(v) for v in reader.bbox]
    zones = []

    for shape_rec in reader.iterShapeRecords():
        rec = shape_rec.record.as_dict()
        zone_id = int(pick_field(rec, "LocationID", "locationid", "OBJECTID", default=0))
        zone_name = str(pick_field(rec, "zone", "Zone", "name", "Name", default=f"Zone {zone_id}"))
        borough = str(pick_field(rec, "borough", "Borough", default="Unknown"))
        polygons = shape_to_polygons(shape_rec.shape, bbox, precision, simplify_step)
        zones.append(
            {
                "id": zone_id,
                "name": zone_name,
                "borough": borough,
                "polygons": polygons,
            }
        )

    payload = {"zones": zones}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    return payload


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def parse_args() -> argparse.Namespace:
    repo_root = default_repo_root()
    default_input = repo_root / "source" / "devops" / "frontend" / "public" / "taxi_zones" / "taxi_zones.shp"
    default_output = repo_root / "source" / "devops" / "frontend" / "src" / "data" / "zone_shapes.json"

    parser = argparse.ArgumentParser(
        description="Convert taxi_zones shapefile into frontend zone_shapes.json"
    )
    parser.add_argument("--input", type=Path, default=default_input, help="Path to .shp file")
    parser.add_argument(
        "--output",
        type=Path,
        default=default_output,
        help="Path to output zone_shapes.json",
    )
    parser.add_argument(
        "--precision",
        type=int,
        default=6,
        help="Decimal precision for normalized coordinates",
    )
    parser.add_argument(
        "--simplify-step",
        type=int,
        default=4,
        help="Keep every Nth point (>=1) to reduce file size",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        raise FileNotFoundError(f"Input shapefile not found: {args.input}")

    payload = build_zone_shapes(
        input_path=args.input,
        output_path=args.output,
        precision=args.precision,
        simplify_step=max(1, args.simplify_step),
    )
    print(
        f"Wrote {len(payload['zones'])} zones to {args.output} "
        f"(precision={args.precision}, simplify_step={max(1, args.simplify_step)})"
    )


if __name__ == "__main__":
    main()
