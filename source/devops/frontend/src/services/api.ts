import type {
  DataDriftResponse,
  DemandModelPredictionResponse,
} from "../types";

const API_BASE_URL = "https://api.nyc-taxi.app";

export async function fetchDemandPrediction(
  selectedZoneIds: number[],
  timestamp: string,
): Promise<DemandModelPredictionResponse> {
  const params = new URLSearchParams({
    zone_ids: selectedZoneIds.join(","),
    timestamp,
  });

  const response = await fetch(
    `${API_BASE_URL}/api/predict?${params.toString()}`,
  );

  if (!response.ok) {
    throw new Error(`Prediction request failed: ${response.status}`);
  }

  return response.json();
}

export async function fetchDataDrift(
  month?: string,
): Promise<DataDriftResponse> {
  const url = month
    ? `${API_BASE_URL}/api/drift?month=${encodeURIComponent(month)}`
    : `${API_BASE_URL}/api/drift`;

  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`Drift request failed: ${response.status}`);
  }

  return response.json();
}
