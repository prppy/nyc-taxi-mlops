import type {
  DataDriftResponse,
  DemandModelPredictionResponse,
} from "../types";

const API_BASE_URL = "https://api.nyc-taxi.app";

export async function fetchDemandPrediction(
  selectedZoneIds: number[],
  timestamp: string,
): Promise<DemandModelPredictionResponse> {
  const response = await fetch(`${API_BASE_URL}/predict`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ selectedZoneIds, timestamp }),
  });

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