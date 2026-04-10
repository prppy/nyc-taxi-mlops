import { THEME } from "../constants/theme";

type DriftLevel = {
  fg: string;
  bg: string;
  label: string;
};

const DRIFT: Record<string, DriftLevel> = {
  critical: { fg: THEME.danger,   bg: THEME.dangerLight,  label: "Critical" },
  high:     { fg: THEME.orange,   bg: THEME.orangeLight,  label: "High"     },
  medium:   { fg: THEME.warning,  bg: THEME.warningLight, label: "Medium"   },
  healthy:  { fg: THEME.success,  bg: THEME.successLight, label: "Healthy"  },
};

function driftLevel(driftScore: number): DriftLevel {
  if (driftScore >= 0.8)  return DRIFT.critical;
  if (driftScore >= 0.55) return DRIFT.high;
  if (driftScore >= 0.3)  return DRIFT.medium;
  return DRIFT.healthy;
}

export function driftSeverityColor(driftScore: number): string {
  return driftLevel(driftScore).fg;
}

export function driftSeverityBg(driftScore: number): string {
  return driftLevel(driftScore).bg;
}

export function driftSeverityLabel(driftScore: number): string {
  return driftLevel(driftScore).label;
}

// full object for callers that need all three at once (e.g. overallStatus)
export function driftSeverity(driftScore: number): DriftLevel {
  return driftLevel(driftScore);
}