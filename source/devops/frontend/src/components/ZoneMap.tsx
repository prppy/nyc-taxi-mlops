import { useMemo, useRef, useState, useEffect } from "react";
import type { CSSProperties } from "react";
import { FiPlus, FiMinus, FiRotateCcw } from "react-icons/fi";
import {
  GLOBAL_STYLES,
  MAP_SIZE,
  MAX_ZOOM,
  MIN_ZOOM,
  ZOOM_STEP,
} from "../constants/appConstants";
import { THEME } from "../constants/theme";
import { formatDemandScore, scoreFillColor } from "../services/scoreDisplay";
import {
  zonePolygonFillColor,
  zonePolygonFillOpacity,
  zonePolygonFillOpacityWithHover,
} from "../services/zoneMapFill";
import type { PredictionRow, ZoneShape } from "../types";

// TYPES
type Props = {
  zones: ZoneShape[];
  selectedZoneIds: number[];
  locked: boolean;
  predictionsById: Map<number, PredictionRow>;
  includedZoneIds: Set<number>;
  onToggleZone: (zoneId: number) => void;
  onHoverZoneChange?: (zoneId: number | null) => void;
};

// CONSTANTS
const LEGEND_ITEMS = [
  { label: "High demand", color: THEME.scoreHigh },
  { label: "Medium demand", color: THEME.scoreMid },
  { label: "Low demand", color: THEME.scoreLow },
  { label: "Unselected", color: THEME.grayZone },
];

const IS_HOVERED_COLOR = "#3b82f6";

const ZOOM_ICON_SIZE = 16;

// HELPERS
function pointToSvg(point: [number, number]) {
  return `${(point[0] * MAP_SIZE).toFixed(2)},${(point[1] * MAP_SIZE).toFixed(2)}`;
}

// COMPONENTS

// header
function MapHeader({
  selectedZoneIds,
  locked,
}: {
  selectedZoneIds: number[];
  locked: boolean;
}) {
  return (
    <div style={STYLES.headerRow}>
      <div style={STYLES.titleBlock}>
        <h2 style={STYLES.title}>Zone Map</h2>
        <span style={STYLES.subtitle}>
          {selectedZoneIds.length > 0
            ? `${selectedZoneIds.length} zone${selectedZoneIds.length > 1 ? "s" : ""} selected`
            : locked
              ? "Prediction complete — click zones to explore"
              : "Click zones to select, then run forecast"}
        </span>
      </div>
    </div>
  );
}

// zoom controls (zoom in and out)
function ZoomControls({ zoomBy }: { zoomBy: (delta: number) => void }) {
  return (
    <div style={STYLES.zoomCluster}>
      <button
        style={{ ...STYLES.zoomButton, ...GLOBAL_STYLES.buttonPressable }}
        onClick={() => zoomBy(ZOOM_STEP)}
        title="Zoom in"
        aria-label="Zoom in"
      >
        <FiPlus size={ZOOM_ICON_SIZE} strokeWidth={2.5} />
      </button>
      <div style={STYLES.zoomDivider} />
      <button
        style={{ ...STYLES.zoomButton, ...GLOBAL_STYLES.buttonPressable }}
        onClick={() => zoomBy(-ZOOM_STEP)}
        title="Zoom out"
        aria-label="Zoom out"
      >
        <FiMinus size={ZOOM_ICON_SIZE} strokeWidth={2.5} />
      </button>
    </div>
  );
}

// reset button (right below zoom in and out!)
function ResetButton({ onReset }: { onReset: () => void }) {
  return (
    <button
      style={{ ...STYLES.resetButton, ...GLOBAL_STYLES.buttonPressable }}
      onClick={onReset}
      title="Reset view"
      aria-label="Reset view"
    >
      <FiRotateCcw size={ZOOM_ICON_SIZE} strokeWidth={2.5} />
    </button>
  );
}

// zoom pill (current state of zoom)
function ZoomPill({ scale }: { scale: number }) {
  return <div style={STYLES.zoomPill}>{Math.round(scale * 100)}%</div>;
}

// legend
function Legend() {
  return (
    <div style={STYLES.legend}>
      <span style={STYLES.legendLabel}>Demand</span>
      {LEGEND_ITEMS.map(({ label, color }) => (
        <div key={label} style={STYLES.legendItem}>
          <span style={{ ...STYLES.legendDot, background: color }} />
          {label}
        </div>
      ))}
    </div>
  );
}

// tooltip
type TooltipProps = {
  hoveredZone: ZoneShape;
  tooltip: { x: number; y: number };
  predictionsById: Map<number, PredictionRow>;
  selectedZoneIds: number[];
  locked: boolean;
};

function Tooltip({
  hoveredZone,
  tooltip,
  predictionsById,
  selectedZoneIds,
  locked,
}: TooltipProps) {
  return (
    <div
      style={{
        ...STYLES.tooltip,
        left: tooltip.x + 14,
        top: tooltip.y + 14,
      }}
    >
      <div style={STYLES.tooltipZoneId}>Zone #{hoveredZone.id}</div>
      <div style={STYLES.tooltipTitle}>{hoveredZone.name}</div>
      <div style={STYLES.tooltipBorough}>{hoveredZone.borough}</div>

      {predictionsById.get(hoveredZone.id) && (
        <>
          <div style={STYLES.tooltipDivider} />
          {/* <div style={STYLES.tooltipScore}>
            <span>Demand Score</span>
            <strong
              style={{
                color: scoreFillColor(
                  predictionsById.get(hoveredZone.id)!.score,
                ),
              }}
            >
              {formatDemandScore(predictionsById.get(hoveredZone.id)!.score)}
            </strong>
          </div> */}
          <div style={{ ...STYLES.tooltipScore, marginTop: 4 }}>
            <span>Predicted Rides</span>
            <strong>
              {predictionsById.get(hoveredZone.id)!.predictedDemand}/hr
            </strong>
          </div>
        </>
      )}

      {!locked && (
        <div style={STYLES.tooltipHint}>
          {selectedZoneIds.includes(hoveredZone.id)
            ? "Click to deselect"
            : "Click to select"}
        </div>
      )}
    </div>
  );
}

// MAIN COMPONENT
export function ZoneMap({
  zones,
  selectedZoneIds,
  locked,
  predictionsById,
  includedZoneIds,
  onToggleZone,
  onHoverZoneChange,
}: Props) {
  const [scale, setScale] = useState(1.25);
  const [panX, setPanX] = useState(0);
  const [panY, setPanY] = useState(0);
  const [isDragging, setIsDragging] = useState(false);
  const [hoveredZoneId, setHoveredZoneId] = useState<number | null>(null);
  const [tooltip, setTooltip] = useState<{ x: number; y: number } | null>(null);
  const dragStartRef = useRef<{ x: number; y: number } | null>(null);
  const mapWrapRef = useRef<HTMLDivElement>(null);

  const hoveredZone = useMemo(
    () => zones.find((z) => z.id === hoveredZoneId) ?? null,
    [zones, hoveredZoneId],
  );

  const zoomBy = (delta: number) =>
    setScale((prev) =>
      Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, Number((prev + delta).toFixed(2)))),
    );

  const resetView = () => {
    setScale(1);
    setPanX(0);
    setPanY(0);
  };

  // NO scroll/wheel zoom -> ZOOM WITH BUTTONS only
  const handlePointerDown: React.PointerEventHandler<SVGSVGElement> = (e) => {
    dragStartRef.current = { x: e.clientX, y: e.clientY };
    setIsDragging(false);
  };

  const handlePointerMove: React.PointerEventHandler<SVGSVGElement> = (e) => {
    if (dragStartRef.current) {
      const dx = e.clientX - dragStartRef.current.x;
      const dy = e.clientY - dragStartRef.current.y;
      if (Math.abs(dx) > 3 || Math.abs(dy) > 3) setIsDragging(true);
      setPanX((prev) => prev + dx / scale);
      setPanY((prev) => prev + dy / scale);
      dragStartRef.current = { x: e.clientX, y: e.clientY };
      return;
    }
    setTooltip({ x: e.clientX, y: e.clientY });
  };

  const handlePointerUp: React.PointerEventHandler<SVGSVGElement> = () => {
    dragStartRef.current = null;
    setIsDragging(false);
  };

  const hasPredictions = predictionsById.size > 0;

  useEffect(() => {
    onHoverZoneChange?.(hoveredZoneId);
  }, [hoveredZoneId, onHoverZoneChange]);

  useEffect(() => {
    const entries = [...predictionsById.entries()].map(([id, p]) => ({
      id,
      score: p.score,
    }));
    console.table(entries);
  }, [predictionsById]);

  return (
    <>
      <section style={{ ...GLOBAL_STYLES.panel, ...STYLES.mapPanel }}>
        <MapHeader selectedZoneIds={selectedZoneIds} locked={locked} />

        {/* map area */}
        <div style={STYLES.mapWrap} ref={mapWrapRef}>
          <svg
            viewBox={`0 0 ${MAP_SIZE} ${MAP_SIZE}`}
            style={{ ...STYLES.map, ...(isDragging ? STYLES.mapGrabbing : {}) }}
            onPointerDown={handlePointerDown}
            onPointerMove={handlePointerMove}
            onPointerUp={handlePointerUp}
            onPointerLeave={() => {
              dragStartRef.current = null;
              setIsDragging(false);
              setTooltip(null);
              setHoveredZoneId(null);
            }}
          >
            <g transform={`translate(${panX} ${panY}) scale(${scale})`}>
              {zones.map((zone) => {
                const selected = selectedZoneIds.includes(zone.id);
                const prediction = predictionsById.get(zone.id);
                const isIncluded = includedZoneIds.has(zone.id);
                const isHovered = hoveredZoneId === zone.id;

                const score = prediction?.score;

                const fill =
                  score != null
                    ? scoreFillColor(score)
                    : zonePolygonFillColor({
                        prediction,
                        locked,
                        isIncluded,
                        selected,
                      }); // pass selected

                const effectiveFill =
                  !hasPredictions && isHovered && !selected
                    ? THEME.grayZoneHover // hover = exactly same color as selected
                    : fill;

                const fillOpacity = zonePolygonFillOpacityWithHover(
                  zonePolygonFillOpacity({
                    prediction,
                    locked,
                    isIncluded,
                    selected,
                  }),
                  isHovered && !hasPredictions, // no hover effect after forecast
                );

                const strokeColor = selected
                  ? THEME.selectedStroke
                  : isHovered
                    ? IS_HOVERED_COLOR
                    : THEME.zoneStroke;

                const strokeWidth = selected ? 2.5 : isHovered ? 1.8 : 0.7;

                return zone.polygons.map((ring, idx) => (
                  <polygon
                    key={`${zone.id}-${idx}`}
                    points={ring.map(pointToSvg).join(" ")}
                    fill={effectiveFill}
                    fillOpacity={fillOpacity}
                    stroke={strokeColor}
                    strokeWidth={strokeWidth}
                    style={{
                      transition: "fill-opacity 0.15s, stroke-width 0.1s",
                    }}
                    onMouseEnter={() => setHoveredZoneId(zone.id)}
                    onMouseMove={(e) =>
                      setTooltip({ x: e.clientX, y: e.clientY })
                    }
                    onMouseLeave={() => setHoveredZoneId(null)}
                    onClick={() => {
                      if (!locked && !isDragging) onToggleZone(zone.id);
                    }}
                  />
                ));
              })}
            </g>
          </svg>

          {/* zooms */}
          <ZoomControls zoomBy={zoomBy} />
          <ResetButton onReset={resetView} />
          <ZoomPill scale={scale} />

          {/* legends */}
          {hasPredictions && <Legend />}
        </div>
      </section>

      {/* tooltip */}
      {hoveredZone && tooltip && (
        <Tooltip
          hoveredZone={hoveredZone}
          tooltip={tooltip}
          predictionsById={predictionsById}
          selectedZoneIds={selectedZoneIds}
          locked={locked}
        />
      )}
    </>
  );
}

const STYLES: Record<string, CSSProperties> = {
  mapPanel: {
    minHeight: 560,
    display: "flex",
    flexDirection: "column",
  },
  headerRow: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: 12,
  },
  titleBlock: {
    display: "flex",
    flexDirection: "column",
    gap: 2,
  },
  title: {
    margin: 0,
    color: THEME.text,
    fontSize: "0.95rem",
    fontWeight: 700,
  },
  subtitle: {
    color: THEME.mutedText,
    fontSize: "0.78rem",
  },
  mapWrap: {
    flex: 1,
    position: "relative",
    borderRadius: 10,
    overflow: "hidden",
    border: `1px solid ${THEME.grayZone}`,
    background: "linear-gradient(145deg, #f0f5fc 0%, #e4edf8 100%)",
  },
  map: {
    width: "100%",
    maxHeight: "72vh",
    aspectRatio: "1 / 1",
    cursor: "grab",
    display: "block",
  },
  mapGrabbing: {
    cursor: "grabbing",
  },

  // ZOOM CLUSTER
  zoomCluster: {
    position: "absolute",
    bottom: 56,
    right: 12,
    display: "flex",
    flexDirection: "column",
    borderRadius: 8,
    overflow: "hidden",
    boxShadow: "0 2px 8px rgba(4, 9, 16, 0.18)",
    border: `1px solid ${THEME.grayZone}`,
    zIndex: 10,
  },
  zoomButton: {
    width: 36,
    height: 36,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    background: "rgba(255,255,255,0.97)",
    color: THEME.text,
    border: "none",
    cursor: "pointer",
    transition: "background 0.12s",
    padding: 0,
    lineHeight: 1,
  },
  zoomDivider: {
    height: 1,
    background: THEME.accentDark,
    opacity: 0.35,
  },

  // RESET BUTTON
  resetButton: {
    position: "absolute",
    bottom: 12,
    right: 12,
    width: 36,
    height: 36,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    background: "rgba(255,255,255,0.97)",
    color: THEME.text,
    border: `1px solid ${THEME.grayZone}`,
    borderRadius: 8,
    cursor: "pointer",
    boxShadow: "0 2px 8px rgba(4, 9, 16, 0.18)",
    zIndex: 10,
    transition: "background 0.12s",
    padding: 0,
    lineHeight: 1,
  },

  // ZOOM PILL
  zoomPill: {
    position: "absolute",
    top: 12,
    left: 12,
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    fontSize: "0.72rem",
    fontWeight: 700,
    color: THEME.text,
    background: "rgba(255,255,255,0.90)",
    border: `1px solid ${THEME.grayZone}`,
    borderRadius: 6,
    padding: "4px 8px",
    boxShadow: "0 1px 4px rgba(13,24,41,0.10)",
    zIndex: 10,
    pointerEvents: "none",
    userSelect: "none",
  },

  // LEGEND
  legend: {
    display: "flex",
    alignItems: "center",
    gap: 14,
    flexWrap: "wrap",
    padding: "8px 12px",
    background: "rgba(255,255,255,0.85)",
    borderTop: `1px solid ${THEME.accentDark}`,
  },
  legendLabel: {
    fontSize: "0.72rem",
    fontWeight: 600,
    letterSpacing: "0.04em",
    textTransform: "uppercase",
    color: THEME.mutedText,
    marginRight: 4,
  },
  legendItem: {
    display: "flex",
    alignItems: "center",
    gap: 5,
    fontSize: "0.73rem",
    color: THEME.mutedText,
  },
  legendDot: {
    width: 10,
    height: 10,
    borderRadius: 3,
  },

  // TOOLTIP
  tooltip: {
    position: "fixed",
    zIndex: 50,
    pointerEvents: "none",
    background: "rgba(255,255,255,0.97)",
    borderRadius: 10,
    padding: "10px 12px",
    minWidth: 190,
    boxShadow: "0 8px 24px rgba(13,24,41,0.12)",
    backdropFilter: "blur(6px)",
  },
  tooltipZoneId: {
    fontSize: "0.7rem",
    fontWeight: 700,
    letterSpacing: "0.06em",
    textTransform: "uppercase",
    color: THEME.mutedText,
    marginBottom: 2,
  },
  tooltipTitle: {
    color: THEME.text,
    fontSize: "0.9rem",
    fontWeight: 700,
    marginBottom: 1,
  },
  tooltipBorough: {
    color: THEME.mutedText,
    fontSize: "0.78rem",
    marginBottom: 6,
  },
  tooltipDivider: {
    height: 1,
    background: THEME.accentDark,
    margin: "6px 0",
  },
  tooltipScore: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    fontSize: "0.82rem",
    color: THEME.text,
  },
  tooltipHint: {
    fontSize: "0.73rem",
    color: THEME.mutedText,
    marginTop: 4,
    fontStyle: "italic",
  },
};
