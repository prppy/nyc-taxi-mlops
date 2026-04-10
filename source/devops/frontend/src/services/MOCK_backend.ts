import type {
  DataDriftResponse,
  DemandModelPredictionResponse,
  PredictionRow,
  WeatherSnapshot,
  ZoneShape,
} from "../types";
import { FEATURE_COLS } from "../constants/appConstants";

type ZoneCenter = { id: number; x: number; y: number };

function MOCK_polygonCenter(points: [number, number][]): [number, number] {
  if (points.length === 0) return [0, 0];
  let sx = 0;
  let sy = 0;
  for (const [x, y] of points) {
    sx += x;
    sy += y;
  }
  return [sx / points.length, sy / points.length];
}

function MOCK_zoneCenters(zones: ZoneShape[]): ZoneCenter[] {
  return zones.map((zone) => {
    const ring = zone.polygons[0] ?? [];
    const [x, y] = MOCK_polygonCenter(ring);
    return { id: zone.id, x, y };
  });
}

function MOCK_getNearbyZoneIds(
  selectedIds: number[],
  zones: ZoneShape[],
): number[] {
  const centers = MOCK_zoneCenters(zones);
  const centerById = new Map(centers.map((c) => [c.id, c]));
  const selectedSet = new Set(selectedIds);
  const nearby = new Set<number>();

  for (const selectedId of selectedIds) {
    const c = centerById.get(selectedId);
    if (!c) continue;
    const nearest = centers
      .filter((other) => other.id !== selectedId && !selectedSet.has(other.id))
      .map((other) => ({
        id: other.id,
        d: Math.hypot(other.x - c.x, other.y - c.y),
      }))
      .sort((a, b) => a.d - b.d)
      .slice(0, 4);
    nearest.forEach((n) => nearby.add(n.id));
  }

  return [...nearby];
}

/** mock-only: synthetic demand → [0, 1] score as a real service might return. */
function MOCK_scoreFromSyntheticDemand(demand: number): number {
  const raw = demand / 130;
  return Math.min(1, Math.max(0, Number(raw.toFixed(4))));
}

export async function MOCK_fetchDemandModelPrediction(
  selectedZoneIds: number[],
  timestamp: string,
  zones: ZoneShape[],
): Promise<DemandModelPredictionResponse> {
  await new Promise((resolve) => setTimeout(resolve, 700));

  const hour = new Date(timestamp).getHours();
  const peak = (hour >= 7 && hour <= 9) || (hour >= 17 && hour <= 20);
  const night = hour >= 22 || hour <= 3;

  const nearbyZoneIds = MOCK_getNearbyZoneIds(selectedZoneIds, zones);
  const includedZoneIds = [...new Set([...selectedZoneIds, ...nearbyZoneIds])];
  const selectedSet = new Set(selectedZoneIds);

  const predictions: PredictionRow[] = includedZoneIds.map((zoneId) => {
    const selectedBoost = selectedSet.has(zoneId) ? 22 : 10;
    const cyc = ((zoneId * 17 + hour * 11) % 60) - 10;
    const base = 24 + Math.max(0, cyc);
    const demand = Math.round(
      base + selectedBoost + (peak ? 32 : 0) + (night ? 9 : 0),
    );
    const score = MOCK_scoreFromSyntheticDemand(demand);
    return {
      zoneId,
      score,
      predictedDemand: demand,
      source: selectedSet.has(zoneId) ? "selected" : "nearby",
    };
  });

  const weatherByZoneId = Object.fromEntries(
    includedZoneIds.map((zoneId) => {
      const zone = zones.find((z) => z.id === zoneId);
      return [zoneId, MOCK_weatherSnapshot(hour, zone?.borough ?? null, zoneId)];
    }),
  );

  return {
    includedZoneIds,
    predictions,
    weatherByZoneId,
  };
}

export async function MOCK_fetchDataDrift(
  timestamp: string,
): Promise<DataDriftResponse> {
  await new Promise((resolve) => setTimeout(resolve, 500));
  const hour = new Date(timestamp).getHours();

  const featureStats = FEATURE_COLS.map((feature, idx) => {
    const trainingMean = Number((0.4 + ((idx * 11) % 10) * 0.2).toFixed(3));
    const driftBias = Math.sin(hour * 0.25 + idx * 0.7) * 0.35;
    const currentMean = Number((trainingMean + driftBias).toFixed(3));
    const driftScore = Math.min(
      1,
      Number(
        (
          Math.abs(currentMean - trainingMean) /
          Math.max(0.15, trainingMean)
        ).toFixed(3),
      ),
    );
    return { feature, currentMean, trainingMean, driftScore };
  });

  return { featureStats };
}

function MOCK_weatherSnapshot(
  hour: number,
  borough: string | null,
  zoneId: number,
): WeatherSnapshot {
  const weatherSeed = Math.abs(Math.sin(hour * 0.7 + zoneId * 0.13));
  const maybeNull = (value: number): number | null =>
    weatherSeed < 0.06 ? null : Number(value.toFixed(1));

  return {
    temperatureMean: maybeNull(8 + weatherSeed * 20),
    precipitationSum: maybeNull(weatherSeed > 0.45 ? weatherSeed * 9 : 0),
    windSpeedMax: maybeNull(6 + weatherSeed * 26),
    borough,
  };
}
