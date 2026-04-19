import { useState } from "react";
import type { CSSProperties } from "react";
import Collapse from "@mui/material/Collapse";
import { FaCheck, FaLocationDot, FaLock } from "react-icons/fa6";
import {
  IoThermometerOutline,
  IoRainyOutline,
  IoSpeedometerOutline,
  IoBusinessOutline,
  IoTelescopeOutline,
  IoMapOutline,
} from "react-icons/io5";

import { GLOBAL_STYLES } from "../constants/appConstants";
import { THEME } from "../constants/theme";
import { formatDemandScore, scoreFillColor } from "../services/scoreDisplay";
import type { PredictionRow, WeatherSnapshot, ZoneShape } from "../types";

// TYPES

type Props = {
  zones: ZoneShape[];
  selectedZoneIds: number[];
  rankedPredictions: PredictionRow[];
  locked: boolean;
  weatherByZoneId: Record<number, WeatherSnapshot>;
  hoveredZoneId: number | null;
};

// COMPONENTS

function WeatherTile({
  label,
  value,
  icon,
}: {
  label: string;
  value: string;
  icon: React.ReactNode;
}) {
  return (
    <div style={STYLES.weatherTile}>
      <div style={STYLES.weatherTileIcon}>{icon}</div>
      <div style={STYLES.weatherTileLabel}>{label}</div>
      <div style={STYLES.weatherTileVal}>{value}</div>
    </div>
  );
}

function WeatherSection({
  hasPredictions,
  hoveredZoneId,
  weatherByZoneId,
}: {
  hasPredictions: boolean;
  hoveredZoneId: number | null;
  weatherByZoneId: Record<number, WeatherSnapshot>;
}) {
  const hoveredWeather =
    hoveredZoneId != null ? weatherByZoneId[hoveredZoneId] : undefined;
  const isHoveredPredictedZone =
    hoveredZoneId != null && hoveredWeather != null;

  const fmt = (v: number | null | undefined, unit: string) =>
    v == null ? "—" : `${v.toFixed(1)}${unit}`;

  return (
    <section style={GLOBAL_STYLES.panel}>
      <div style={GLOBAL_STYLES.sectionLabel}>Weather Data</div>

      {/* fixed-height body so the panel never jumps */}
      <div style={STYLES.weatherBody}>
        {/* overlay hint when there's nothing to show */}
        {(!hasPredictions || !isHoveredPredictedZone) && (
          <div style={STYLES.weatherOverlay}>
            <span style={STYLES.weatherOverlayIcon}>
              {!hasPredictions ? <IoTelescopeOutline /> : <IoMapOutline />}
            </span>
            <span style={STYLES.weatherOverlayText}>
              {!hasPredictions
                ? "Run a forecast to unlock weather data"
                : "Hover a predicted zone to see conditions"}
            </span>
          </div>
        )}

        {/* grid is always rendered so height is constant; opacity fades it in */}
        <div
          style={{
            ...STYLES.weatherGrid,
            opacity: isHoveredPredictedZone ? 1 : 0,
            pointerEvents: isHoveredPredictedZone ? "auto" : "none",
            transition: "opacity 0.2s",
          }}
        >
          <WeatherTile
            icon={<IoThermometerOutline />}
            label="Temp Mean"
            value={fmt(hoveredWeather?.temperatureMean, "°C")}
          />
          <WeatherTile
            icon={<IoRainyOutline />}
            label="Precip Sum"
            value={fmt(hoveredWeather?.precipitationSum, " mm")}
          />
          <WeatherTile
            icon={<IoSpeedometerOutline />}
            label="Wind Max"
            value={fmt(hoveredWeather?.windSpeedMax, " kph")}
          />
          <WeatherTile
            icon={<IoBusinessOutline />}
            label="Borough"
            value={hoveredWeather?.borough ?? "—"}
          />
        </div>
      </div>
    </section>
  );
}

function SelectedZonesSection({
  selectedZones,
  locked,
}: {
  selectedZones: ZoneShape[];
  locked: boolean;
}) {
  return (
    <section style={GLOBAL_STYLES.panel}>
      <div style={GLOBAL_STYLES.sectionLabel}>
        Selected Zones
        {selectedZones.length > 0 && ` · ${selectedZones.length}`}
      </div>

      <div style={STYLES.chipWrap}>
        {selectedZones.length === 0 ? (
          <span style={STYLES.chipEmpty}>Tap zones on the map</span>
        ) : (
          selectedZones.map((zone) => (
            <span key={zone.id} style={STYLES.chip}>
              #{zone.id} {zone.name}
            </span>
          ))
        )}
      </div>

      {locked && (
        <div style={STYLES.lockedNote}>
          <FaLock style={STYLES.lockIcon} />
          Selection locked
        </div>
      )}
    </section>
  );
}

function RankItem({
  row,
  index,
  zones,
}: {
  row: PredictionRow;
  index: number;
  zones: ZoneShape[];
}) {
  const zone = zones.find((z) => z.id === row.zoneId);
  if (!zone) return null;
  const isTop3 = index < 3;

  return (
    <li style={STYLES.listItem}>
      <div style={{ ...STYLES.rank, ...(isTop3 ? STYLES.rankTop : {}) }}>
        {index + 1}
      </div>

      <div style={STYLES.zoneInfo}>
        <span style={STYLES.zoneName}>{zone.name}</span>
        <span style={STYLES.zoneSource}>
          {row.source === "nearby" ? (
            <>
              <FaLocationDot style={STYLES.sourceIcon} /> nearby
            </>
          ) : (
            <>
              <FaCheck style={STYLES.sourceIcon} /> selected
            </>
          )}
        </span>
      </div>

      <div style={STYLES.scoreBlock}>
        {/* TODO: do we show exact predicted demand? */}
        <span style={STYLES.rides}>{row.predictedDemand} rides</span>
      </div>
    </li>
  );
}

function RankingsSection({
  zones,
  rankedPredictions,
}: {
  zones: ZoneShape[];
  rankedPredictions: PredictionRow[];
}) {
  const [showAll, setShowAll] = useState(false);
  const top = rankedPredictions.slice(0, 5);
  const rest = rankedPredictions.slice(5);

  return (
    <section style={GLOBAL_STYLES.panel}>
      <div style={GLOBAL_STYLES.sectionLabel}>Top Demand Zones</div>

      {rankedPredictions.length === 0 ? (
        <p style={STYLES.emptyHint}>
          Run forecast to view ranked demand zones.
        </p>
      ) : (
        <>
          <ul style={STYLES.list}>
            {top.map((row, i) => (
              <RankItem key={row.zoneId} row={row} index={i} zones={zones} />
            ))}
            <Collapse in={showAll}>
              {rest.map((row, i) => (
                <RankItem
                  key={row.zoneId}
                  row={row}
                  index={i + 5}
                  zones={zones}
                />
              ))}
            </Collapse>
          </ul>

          {rest.length > 0 && (
            <button
              onClick={() => setShowAll((p) => !p)}
              style={STYLES.showMoreButton}
            >
              {showAll ? "Show less" : `Show ${rest.length} more`}
            </button>
          )}
        </>
      )}
    </section>
  );
}

// MAIN COMPONENT

export function SidePanel({
  zones,
  selectedZoneIds,
  rankedPredictions,
  locked,
  weatherByZoneId,
  hoveredZoneId,
}: Props) {
  const selectedZones = zones.filter((z) => selectedZoneIds.includes(z.id));
  const hasPredictions = rankedPredictions.length > 0;

  return (
    <aside style={STYLES.sideStack}>
      <WeatherSection
        hasPredictions={hasPredictions}
        hoveredZoneId={hoveredZoneId}
        weatherByZoneId={weatherByZoneId}
      />
      <SelectedZonesSection selectedZones={selectedZones} locked={locked} />
      <RankingsSection zones={zones} rankedPredictions={rankedPredictions} />
    </aside>
  );
}

// STYLES

const STYLES: Record<string, CSSProperties> = {
  sideStack: {
    display: "flex",
    flexDirection: "column",
    gap: 12,
  },

  // WEATHER

  weatherBody: {
    position: "relative",
    marginTop: 10,
    height: 164, // enough for the 2×2 tile grid (tile ~72px + gap)
  },

  // centered hint that sits on top when grid is invisible
  weatherOverlay: {
    position: "absolute",
    inset: 0,
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    justifyContent: "center",
    gap: 8,
    background: THEME.surface,
    border: `1px dashed ${THEME.accentDark}`,
    borderRadius: 10,
    zIndex: 1,
    pointerEvents: "none",
  },
  weatherOverlayIcon: {
    fontSize: "1.6rem",
    color: THEME.mutedText,
  },
  weatherOverlayText: {
    fontSize: "0.78rem",
    color: THEME.mutedText,
    textAlign: "center",
    maxWidth: 180,
    lineHeight: 1.4,
  },

  weatherGrid: {
    display: "grid",
    gridTemplateColumns: "1fr 1fr",
    gap: 8,
    height: "100%",
  },

  weatherTile: {
    background: THEME.surface,
    border: `1px solid ${THEME.grayZone}`,
    borderRadius: 10,
    padding: "10px 12px",
    display: "flex",
    flexDirection: "column",
    justifyContent: "space-between",
  },
  weatherTileIcon: {
    fontSize: "1.2rem",
    color: THEME.accentDark,
    marginBottom: 2,
    display: "flex",
  },
  weatherTileLabel: {
    fontSize: "0.66rem",
    fontWeight: 700,
    letterSpacing: "0.05em",
    textTransform: "uppercase",
    color: THEME.mutedText,
  },
  weatherTileVal: {
    fontSize: "1.05rem",
    fontWeight: 800,
    color: THEME.text,
    marginTop: 4,
  },

  // SELECTED ZONES

  chipWrap: {
    display: "flex",
    gap: 6,
    flexWrap: "wrap",
    marginTop: 8,
  },
  chip: {
    display: "inline-flex",
    alignItems: "center",
    gap: 4,
    background: THEME.accentDark,
    color: "#e0eeff",
    border: "1px solid #2d4a7a",
    borderRadius: 999,
    fontSize: "0.76rem",
    fontWeight: 600,
    padding: "5px 10px",
  },
  chipEmpty: {
    background: THEME.surface,
    color: THEME.mutedText,
    border: `1px solid ${THEME.grayZone}`,
    borderRadius: 999,
    fontSize: "0.76rem",
    padding: "5px 10px",
  },
  lockedNote: {
    display: "flex",
    alignItems: "center",
    gap: 6,
    marginTop: 10,
    padding: "7px 10px",
    background: THEME.surface,
    border: `1px solid ${THEME.disabledZone}`,
    borderRadius: 8,
    fontSize: "0.76rem",
    color: THEME.mutedText,
    fontWeight: 500,
  },
  lockIcon: {
    width: 11,
    height: 11,
    flexShrink: 0,
  },

  // RANKINGS

  list: {
    listStyle: "none",
    margin: 0,
    padding: 0,
    display: "flex",
    flexDirection: "column",
    gap: 0,
  },
  listItem: {
    display: "grid",
    gridTemplateColumns: "28px 1fr auto",
    alignItems: "center",
    gap: 10,
    padding: "10px 0",
    borderBottom: `1px solid ${THEME.grayZone}`,
  },
  rank: {
    width: 24,
    height: 24,
    borderRadius: 7,
    background: THEME.surface,
    border: `1px solid ${THEME.accentDark}`,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    fontSize: "0.72rem",
    fontWeight: 800,
    color: THEME.mutedText,
    flexShrink: 0,
  },
  rankTop: {
    background: THEME.accentLight,
    border: `1px solid ${THEME.accentDark}`,
    color: "black",
  },
  zoneInfo: {
    display: "flex",
    flexDirection: "column",
    gap: 2,
    minWidth: 0,
    overflow: "hidden",
  },
  zoneName: {
    fontSize: "0.83rem",
    fontWeight: 700,
    color: THEME.text,
    whiteSpace: "nowrap",
    overflow: "hidden",
    textOverflow: "ellipsis",
  },
  zoneSource: {
    display: "inline-flex",
    alignItems: "center",
    gap: 4,
    fontSize: "0.7rem",
    color: THEME.mutedText,
  },
  sourceIcon: {
    width: 11,
    height: 11,
    flexShrink: 0,
  },
  scoreBlock: {
    display: "flex",
    flexDirection: "column",
    alignItems: "flex-end",
    gap: 2,
    flexShrink: 0,
  },
  score: {
    fontSize: "0.9rem",
    fontWeight: 800,
  },
  rides: {
    fontSize: "0.72rem",
    color: THEME.mutedText,
    whiteSpace: "nowrap",
  },
  emptyHint: {
    color: THEME.mutedText,
    fontSize: "0.85rem",
    margin: 0,
    padding: "8px 0",
  },
  showMoreButton: {
    marginTop: 6,
    background: "transparent",
    border: "none",
    color: THEME.mutedText,
    fontSize: "0.8rem",
    fontWeight: 600,
    cursor: "pointer",
    padding: "6px 0",
  },
};
