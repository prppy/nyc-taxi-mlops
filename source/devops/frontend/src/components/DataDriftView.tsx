import type { CSSProperties } from "react";
import { useState } from "react";
import {
  FEATURE_COLS,
  FEATURE_GROUPS,
  FEATURE_LABELS,
  GLOBAL_STYLES,
} from "../constants/appConstants";
import { THEME } from "../constants/theme";
import { driftSeverity, driftSeverityColor } from "../services/driftDisplay";
import type { FeatureStat, PredictionLog } from "../types";
import { PredictionLogTable } from "./PredictionLogTable";
// TYPES

type Props = {
  featureStats: FeatureStat[];
  predictionLogs: PredictionLog[];
};

// CONSTANTS — single source of truth via driftSeverity()

const LEGEND = [
  { label: "Critical ≥0.8", fg: driftSeverity(0.8).fg },
  { label: "High ≥0.55", fg: driftSeverity(0.55).fg },
  { label: "Medium ≥0.3", fg: driftSeverity(0.3).fg },
  { label: "Healthy", fg: driftSeverity(0).fg },
];

const DRIFT_PREVIEW_GROUPS = 2;

// COMPONENTS

function StatCard({
  label,
  value,
  sub,
  valueColor,
  bgColor,
}: {
  label: string;
  value: string | number;
  sub?: string;
  valueColor?: string;
  bgColor?: string;
}) {
  return (
    <div
      style={{
        ...STYLES.statCard,
        ...(bgColor ? { background: bgColor } : {}),
      }}
    >
      <div style={STYLES.statLabel}>{label}</div>
      <div style={{ ...STYLES.statVal, color: valueColor ?? THEME.text }}>
        {value}
      </div>
      {sub && <div style={STYLES.statSub}>{sub}</div>}
    </div>
  );
}

function DriftBar({ row }: { row: FeatureStat }) {
  const { fg, bg } = driftSeverity(row.driftScore);
  const pct = Math.round(row.driftScore * 100);
  const displayName = FEATURE_LABELS[row.feature] ?? row.feature;

  return (
    <div style={STYLES.barRow}>
      <div style={STYLES.barLabel} title={displayName}>
        {displayName}
      </div>
      <div style={STYLES.barTrack}>
        <div
          style={{
            ...STYLES.barFill,
            width: `${pct}%`,
            background: fg,
            opacity: 0.8,
          }}
        />
      </div>
      <div style={{ ...STYLES.driftBadge, background: bg, color: fg }}>
        {pct}%
      </div>
    </div>
  );
}

function HealthItem({ row }: { row: FeatureStat }) {
  const { fg, bg } = driftSeverity(row.driftScore);
  const pct = Math.round(row.driftScore * 100);
  const displayName = FEATURE_LABELS[row.feature] ?? row.feature;

  return (
    <div style={STYLES.healthRow}>
      <div style={{ ...STYLES.healthDot, background: fg }} />
      <div style={STYLES.healthName} title={displayName}>
        {displayName}
      </div>
      <div style={{ ...STYLES.driftBadge, background: bg, color: fg }}>
        {pct}%
      </div>
    </div>
  );
}

// MAIN COMPONENT

export function DataDriftView({ featureStats, predictionLogs }: Props) {
  const [driftExpanded, setDriftExpanded] = useState(false);
  const [healthExpanded, setHealthExpanded] = useState(false);

  const statsMap = new Map(featureStats.map((r) => [r.feature, r]));

  const avgDrift =
    featureStats.length === 0
      ? 0
      : featureStats.reduce((s, r) => s + r.driftScore, 0) /
        featureStats.length;

  const criticalCount = featureStats.filter((r) => r.driftScore >= 0.8).length;
  const highCount = featureStats.filter((r) => r.driftScore >= 0.55).length;
  const healthyCount = featureStats.filter((r) => r.driftScore < 0.3).length;

  // use same thresholds as driftSeverity() //TODO: abstract out into constant
  const worstScore = criticalCount > 0 ? 0.8 : highCount > 0 ? 0.55 : avgDrift;
  const {
    fg: statusColor,
    bg: statusBg,
    label: statusLabel,
  } = driftSeverity(worstScore);

  const sortedByDrift = [...featureStats].sort(
    (a, b) => b.driftScore - a.driftScore,
  );
  const visibleHealth = healthExpanded
    ? sortedByDrift
    : sortedByDrift.filter((r) => r.driftScore >= 0.3);
  const hiddenHealthCount = sortedByDrift.length - visibleHealth.length;

  const groupEntries = Object.entries(FEATURE_GROUPS);
  const visibleGroups = driftExpanded
    ? groupEntries
    : groupEntries.slice(0, DRIFT_PREVIEW_GROUPS);
  const hiddenGroupCount = groupEntries.length - DRIFT_PREVIEW_GROUPS;

  return (
    <div style={STYLES.root}>
      {/* summary stat strip */}
      <div style={STYLES.statStrip}>
        <StatCard
          label="Monitored Features"
          value={FEATURE_COLS.length}
          sub={`${featureStats.length} with data`}
        />
        <StatCard
          label="Avg Drift Score"
          value={avgDrift.toFixed(2)}
          sub="across all features"
          valueColor={driftSeverityColor(avgDrift)}
        />
        <StatCard
          label="High-Drift Features"
          value={highCount}
          sub={`${criticalCount} critical`}
          valueColor={highCount > 0 ? driftSeverityColor(0.55) : THEME.text}
        />
        <StatCard
          label="Overall Status"
          value={statusLabel}
          sub={`${healthyCount} healthy features`}
          valueColor={statusColor}
          bgColor={statusBg}
        />
      </div>

      {/* main two-column layout */}
      <div style={STYLES.twoCol}>
        {/* left: drift bars by group */}
        <section style={GLOBAL_STYLES.panel}>
          {/* TODO: ADD WHAT TYPE OF FEATURE DRIFT IT IS TO THE LABEL */}
          <div style={GLOBAL_STYLES.sectionLabel}>Feature Drift</div>
          <div style={STYLES.legendRow}>
            {LEGEND.map(({ label, fg }) => (
              <div key={label} style={STYLES.legendItem}>
                <span style={{ ...STYLES.legendDot, background: fg }} />
                {label}
              </div>
            ))}
          </div>

          {featureStats.length === 0 ? (
            <p style={{ color: THEME.mutedText, fontSize: "0.9rem" }}>
              No feature statistics available yet.
            </p>
          ) : (
            <>
              {visibleGroups.map(([groupName, features]) => {
                const groupRows = features
                  .map((f) => statsMap.get(f))
                  .filter((r): r is FeatureStat => r !== undefined)
                  .sort((a, b) => b.driftScore - a.driftScore);
                if (groupRows.length === 0) return null;
                return (
                  <div key={groupName} style={STYLES.groupWrap}>
                    <div style={STYLES.groupHeader}>{groupName}</div>
                    {groupRows.map((row) => (
                      <DriftBar key={row.feature} row={row} />
                    ))}
                  </div>
                );
              })}
              {hiddenGroupCount > 0 && (
                <button
                  style={STYLES.showMoreBtn}
                  onClick={() => setDriftExpanded((e) => !e)}
                >
                  {driftExpanded
                    ? "Show less ▲"
                    : `Show ${hiddenGroupCount} more groups ▼`}
                </button>
              )}
            </>
          )}
        </section>

        {/* right: health ranking */}
        <section style={GLOBAL_STYLES.panel}>
          <div style={GLOBAL_STYLES.sectionLabel}>Feature Health Ranking</div>
          {featureStats.length === 0 ? (
            <p style={{ color: THEME.mutedText, fontSize: "0.88rem" }}>
              No data yet.
            </p>
          ) : (
            <>
              <div style={STYLES.healthList}>
                {visibleHealth.map((row) => (
                  <HealthItem key={row.feature} row={row} />
                ))}
              </div>
              {(hiddenHealthCount > 0 || healthExpanded) && (
                <button
                  style={STYLES.showMoreBtn}
                  onClick={() => setHealthExpanded((e) => !e)}
                >
                  {healthExpanded
                    ? "Show less ▲"
                    : `+${hiddenHealthCount} healthy features ▼`}
                </button>
              )}
            </>
          )}
        </section>
      </div>

      <PredictionLogTable predictionLogs={predictionLogs} />
    </div>
  );
}

// STYLES

const STYLES: Record<string, CSSProperties> = {
  root: { display: "flex", flexDirection: "column", gap: 24 },
  twoCol: {
    display: "grid",
    gridTemplateColumns: "3fr 1fr",
    gap: 16,
    alignItems: "start",
  },
  statStrip: {
    display: "grid",
    gridTemplateColumns: "repeat(4, minmax(0,1fr))",
    gap: 16,
  },
  statCard: {
    background: THEME.panel,
    borderRadius: 11,
    padding: "13px 16px",
  },
  statLabel: {
    fontSize: "0.68rem",
    fontWeight: 700,
    letterSpacing: "0.07em",
    textTransform: "uppercase",
    color: THEME.mutedText,
    marginBottom: 5,
  },
  statVal: { fontSize: "1.7rem", fontWeight: 800, lineHeight: 1.1 },
  statSub: { fontSize: "0.72rem", color: THEME.mutedText, marginTop: 2 },
  legendRow: {
    display: "flex",
    alignItems: "center",
    gap: 16,
    marginBottom: 14,
    flexWrap: "wrap",
  },
  legendItem: {
    display: "flex",
    alignItems: "center",
    gap: 5,
    fontSize: "0.75rem",
    color: THEME.mutedText,
  },
  legendDot: { width: 9, height: 9, borderRadius: 3 },
  groupHeader: {
    fontSize: "0.68rem",
    fontWeight: 800,
    letterSpacing: "0.08em",
    textTransform: "uppercase",
    color: THEME.mutedText,
    padding: "10px 0 5px",
    borderBottom: `1px solid ${THEME.accentDark}`,
    marginBottom: 6,
  },
  groupWrap: { marginBottom: 14 },
  barRow: {
    display: "grid",
    gridTemplateColumns: "170px 1fr 68px",
    alignItems: "center",
    gap: 10,
    padding: "5px 0",
  },
  barLabel: {
    fontSize: "0.8rem",
    color: THEME.text,
    whiteSpace: "nowrap",
    overflow: "hidden",
    textOverflow: "ellipsis",
    fontWeight: 500,
  },
  barTrack: {
    background: THEME.surface,
    borderRadius: 999,
    height: 8,
    overflow: "hidden",
    border: `1px solid ${THEME.grayZone}`,
  },
  barFill: { height: "100%", borderRadius: 999, transition: "width 0.4s ease" },
  barValue: {
    fontSize: "0.78rem",
    fontWeight: 700,
    color: THEME.text,
    textAlign: "right",
  },
  driftBadge: {
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    padding: "2px 8px",
    borderRadius: 999,
    fontSize: "0.68rem",
    fontWeight: 700,
    textAlign: "center",
    whiteSpace: "nowrap",
  },
  healthList: { display: "flex", flexDirection: "column", gap: 0 },
  healthRow: {
    display: "flex",
    alignItems: "center",
    gap: 10,
    padding: "9px 0",
    borderBottom: `1px solid ${THEME.grayZone}`,
  },
  healthDot: { width: 8, height: 8, borderRadius: "50%", flexShrink: 0 },
  healthName: {
    fontSize: "0.8rem",
    color: THEME.text,
    flex: 1,
    minWidth: 0,
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap",
    fontWeight: 500,
  },
  healthScore: { fontSize: "0.78rem", fontWeight: 700, flexShrink: 0 },
  healthLabel: {
    fontSize: "0.68rem",
    color: THEME.mutedText,
    flexShrink: 0,
    minWidth: 44,
    textAlign: "right",
  },
  showMoreBtn: {
    marginTop: 10,
    width: "100%",
    padding: "7px 0",
    background: "transparent",
    border: "none",
    outline: "none", // optional default reset
    boxShadow: "none",
    borderRadius: 8,
    fontSize: "0.75rem",
    fontWeight: 700,
    color: THEME.mutedText,
    cursor: "pointer",
    letterSpacing: "0.04em",
    fontFamily: "inherit",
    appearance: "none", // important for safari
    WebkitAppearance: "none", // safari again
  },
  tableWrap: { overflowX: "auto", marginTop: 0 },
  table: { width: "100%", borderCollapse: "collapse", fontSize: "0.82rem" },
  th: {
    padding: "8px 12px",
    textAlign: "left",
    fontSize: "0.68rem",
    fontWeight: 700,
    letterSpacing: "0.07em",
    textTransform: "uppercase",
    color: THEME.mutedText,
    borderBottom: `2px solid ${THEME.accentDark}`,
    whiteSpace: "nowrap",
    background: THEME.surface,
  },
  td: {
    padding: "10px 12px",
    borderBottom: `1px solid ${THEME.accentDark}`,
    color: THEME.text,
    verticalAlign: "middle",
  },
  tdMuted: { color: THEME.mutedText, fontSize: "0.78rem" },
  emptyRow: {
    textAlign: "center",
    padding: "32px 0",
    color: THEME.mutedText,
    fontSize: "0.88rem",
  },
};
