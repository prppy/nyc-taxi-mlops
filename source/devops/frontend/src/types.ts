export type ZoneShape = {
  id: number;
  name: string;
  borough: string;
  polygons: [number, number][][];
};

// driver view
export type PredictionRow = {
  zoneId: number;
  score: number;   /** backend demand score in [0, 1]; 1 = highest. */
  predictedDemand: number;
  source: "selected" | "nearby";
};

export type DemandModelPredictionResponse = {
  includedZoneIds: number[];
  predictions: PredictionRow[];
  weatherByZoneId: Record<number, WeatherSnapshot>;
};


export type WeatherSnapshot = {
  temperatureMean: number | null;
  precipitationSum: number | null;
  windSpeedMax: number | null;
  borough?: string | null;
};

// manager view
export type PredictionLog = {
  id: string;
  timestamp: string;
  selectedCount: number;
  includedCount: number;
  topZoneName: string;
  topScore: number; // same scale as PredictionRow.score: [0, 1]
};

export type FeatureStat = {
  feature: string;
  currentMean: number;
  trainingMean: number;
  driftScore: number;
};
export type DataDriftResponse = {
  featureStats: FeatureStat[];
};
