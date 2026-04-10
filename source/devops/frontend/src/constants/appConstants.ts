import type { CSSProperties } from "react";
import { THEME } from "./theme";

export const APP_LOGO = "/nyc-tlc-logo.svg";
export const MAP_SIZE = 980;
export const MAX_ZOOM = 6;
export const MIN_ZOOM = 0.75;   // allow zooming out beyond 100%
export const ZOOM_STEP = 0.25;

export const HOURS = Array.from({ length: 24 }, (_, hour) =>
  String(hour).padStart(2, "0"),
);

export const FEATURE_COLS = [
  "demand_lag_1h",
  "demand_lag_24h",
  "demand_lag_168h",
  "rolling_mean_7d",
  "hour",
  "day_of_week",
  "month",
  "hour_sin",
  "hour_cos",
  "dow_sin",
  "dow_cos",
  "is_weekend",
  "is_peak_hour",
  "is_raining",
  "extreme_weather_flag",
  "rain_peak_interaction",
  "temperature",
  "rainfall",
  "temperature_mean",
  "precipitation_sum",
  "wind_speed_max",
  "avg_trip_distance",
  "avg_fare",
] as const;

export const FEATURE_GROUPS: Record<string, string[]> = {
  "Demand Lags": ["demand_lag_1h", "demand_lag_24h", "demand_lag_168h", "rolling_mean_7d"],
  "Time Buckets": ["hour", "day_of_week", "month"],
  "Time Encoding": ["hour_sin", "hour_cos", "dow_sin", "dow_cos"],
  "Time Flags": ["is_weekend", "is_peak_hour"],
  "Weather": [
    "is_raining",
    "extreme_weather_flag",
    "rain_peak_interaction",
    "temperature",
    "rainfall",
    "temperature_mean",
    "precipitation_sum",
    "wind_speed_max",
  ],
  "Trip Metrics": ["avg_trip_distance", "avg_fare"],
};

export const FEATURE_LABELS: Record<string, string> = {
  demand_lag_1h: "Demand Lag 1h",
  demand_lag_24h: "Demand Lag 24h",
  demand_lag_168h: "Demand Lag 168h",
  rolling_mean_7d: "Rolling Mean 7d",
  hour: "Hour",
  day_of_week: "Day of Week",
  month: "Month",
  hour_sin: "Hour Sin",
  hour_cos: "Hour Cos",
  dow_sin: "DOW Sin",
  dow_cos: "DOW Cos",
  is_weekend: "Is Weekend",
  is_peak_hour: "Is Peak Hour",
  is_raining: "Is Raining",
  extreme_weather_flag: "Extreme Weather",
  rain_peak_interaction: "Rain × Peak",
  temperature: "Temperature",
  rainfall: "Rainfall",
  temperature_mean: "Temperature Mean",
  precipitation_sum: "Precipitation Sum",
  wind_speed_max: "Wind Speed Max",
  avg_trip_distance: "Avg Trip Distance",
  avg_fare: "Avg Fare",
};

/** shared panel / layout styles used across screens. */
export const GLOBAL_STYLES = {
  panel: {
    background: THEME.panel,
    border: `1px solid ${THEME.accentDark}`,
    borderRadius: 14,
    padding: "18px 20px",
  } as CSSProperties,

  panelSm: {
    background: THEME.panel,
    border: `1px solid ${THEME.accentDark}`,
    borderRadius: 12,
    padding: "14px 16px",
  } as CSSProperties,

  sectionLabel: {
    fontSize: 15,
    fontWeight: 700,
    color: THEME.text,
    marginBottom: 10,
  } as CSSProperties,

  panelTitle: {
    fontSize: "0.95rem",
    fontWeight: 700,
    color: THEME.text,
    margin: 0,
    marginBottom: 14,
  } as CSSProperties,

  mutedText: {
    color: THEME.mutedText,
    fontSize: "0.88rem",
  } as CSSProperties,

  statTile: {
    background: THEME.surface,
    border: `1px solid ${THEME.accentDark}`,
    borderRadius: 10,
    padding: "12px 14px",
  } as CSSProperties,

  statLabel: {
    fontSize: "0.72rem",
    fontWeight: 600,
    letterSpacing: "0.05em",
    textTransform: "uppercase" as const,
    color: THEME.mutedText,
    marginBottom: 4,
  } as CSSProperties,

  statValue: {
    fontSize: "1.6rem",
    fontWeight: 800,
    color: THEME.text,
    lineHeight: 1.1,
  } as CSSProperties,

  pill: {
    display: "inline-flex",
    alignItems: "center",
    padding: "3px 10px",
    borderRadius: 999,
    fontSize: "0.75rem",
    fontWeight: 700,
  } as CSSProperties,

  divider: {
    height: 1,
    background: THEME.accentDark,
    margin: "12px 0",
  } as CSSProperties,

  buttonPressable: {
    transition: "box-shadow 0.12s ease",
  } as CSSProperties,
} as const;