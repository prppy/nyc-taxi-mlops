import type { CSSProperties } from "react";
import { useState } from "react";
import { ControlPanel } from "./components/ControlPanel";
import { DataDriftView } from "./components/DataDriftView";
import { SidePanel } from "./components/SidePanel";
import { Tabs, type TabKey } from "./components/Tabs";
import { ZoneMap } from "./components/ZoneMap";
import { THEME } from "./constants/theme";
import zoneShapesData from "./data/zone_shapes.json";
import {
  MOCK_fetchDataDrift,
  MOCK_fetchDemandModelPrediction,
} from "./services/MOCK_backend";
import type {
  FeatureStat,
  PredictionLog,
  PredictionRow,
  WeatherSnapshot,
  ZoneShape,
} from "./types";
import { APP_LOGO } from "./constants/appConstants";

// HEADER

interface AppHeaderProps {
  activeTab: TabKey;
  onTabChange: (tab: TabKey) => void;
}

function AppHeader({ activeTab, onTabChange }: AppHeaderProps) {
  return (
    <header style={STYLES.header}>
      <div style={STYLES.headerInner}>
        <div style={STYLES.logoMark}>
          <img src={APP_LOGO} alt="TLC Logo" style={STYLES.logoImage} />
        </div>
        <span style={STYLES.logoSlash}>/</span>
        <Tabs activeTab={activeTab} onChange={onTabChange} />
        <div style={STYLES.headerRight}>
          <div style={STYLES.headerBadge}>
            <div style={STYLES.liveDot} />
            Model Live
          </div>
          <span style={STYLES.headerMeta}>NYC Taxi · 2026</span>
        </div>
      </div>
    </header>
  );
}

// DRIVER VIEW

interface DriverViewProps {
  zones: ZoneShape[];
  selectedZoneIds: number[];
  locked: boolean;
  predictionsById: Map<number, PredictionRow>;
  includedZoneIds: Set<number>;
  rankedPredictions: PredictionRow[];
  weatherByZoneId: Record<number, WeatherSnapshot>;
  hoveredZoneId: number | null;
  dateValue: string;
  hourValue: string;
  loading: boolean;
  disabled: boolean;
  onToggleZone: (zoneId: number) => void;
  onDateChange: (val: string) => void;
  onHourChange: (val: string) => void;
  onPredict: () => void;
  onResetSelection: () => void;
  onHoverZoneChange: (zoneId: number | null) => void;
}

function DriverView({
  zones,
  selectedZoneIds,
  locked,
  predictionsById,
  includedZoneIds,
  rankedPredictions,
  weatherByZoneId,
  hoveredZoneId,
  dateValue,
  hourValue,
  loading,
  disabled,
  onToggleZone,
  onDateChange,
  onHourChange,
  onPredict,
  onResetSelection,
  onHoverZoneChange,
}: DriverViewProps) {
  return (
    <>
      <ControlPanel
        dateValue={dateValue}
        hourValue={hourValue}
        loading={loading}
        disabled={disabled}
        locked={locked}
        onDateChange={onDateChange}
        onHourChange={onHourChange}
        onPredict={onPredict}
        onResetSelection={onResetSelection}
      />
      <div style={STYLES.driverLayout}>
        <div style={STYLES.driverLeft}>
          <ZoneMap
            zones={zones}
            selectedZoneIds={selectedZoneIds}
            locked={locked}
            predictionsById={predictionsById}
            includedZoneIds={includedZoneIds}
            onToggleZone={onToggleZone}
            onHoverZoneChange={onHoverZoneChange}
          />
        </div>
        <SidePanel
          zones={zones}
          selectedZoneIds={selectedZoneIds}
          rankedPredictions={rankedPredictions}
          locked={locked}
          weatherByZoneId={weatherByZoneId}
          hoveredZoneId={hoveredZoneId}
        />
      </div>
    </>
  );
}

// MAIN APP

export default function App() {
  // STATES
  const [activeTab, setActiveTab] = useState<TabKey>("driver");
  const zones = (zoneShapesData as { zones: ZoneShape[] }).zones;
  const [selectedZoneIds, setSelectedZoneIds] = useState<number[]>([]);
  const [locked, setLocked] = useState(false);
  const [predictionsById, setPredictionsById] = useState<
    Map<number, PredictionRow>
  >(new Map());
  const [includedZoneIds, setIncludedZoneIds] = useState<Set<number>>(
    new Set(),
  );
  const [rankedPredictions, setRankedPredictions] = useState<PredictionRow[]>(
    [],
  );
  const [weatherByZoneId, setWeatherByZoneId] = useState<
    Record<number, WeatherSnapshot>
  >({});
  const [hoveredZoneId, setHoveredZoneId] = useState<number | null>(null);
  const [featureStats, setFeatureStats] = useState<FeatureStat[]>([]);
  const [predictionLogs, setPredictionLogs] = useState<PredictionLog[]>([]);
  const [dateValue, setDateValue] = useState(
    () => new Date().toISOString().split("T")[0],
  );
  const [hourValue, setHourValue] = useState(() =>
    String(new Date().getHours()).padStart(2, "0"),
  );
  const [loading, setLoading] = useState(false);

  // FUNCTIONS
  const onToggleZone = (zoneId: number) => {
    if (locked) return;
    setSelectedZoneIds((prev) =>
      prev.includes(zoneId)
        ? prev.filter((id) => id !== zoneId)
        : [...prev, zoneId],
    );
  };

  // TODO: REPLACE W ACTUAL
  const onPredict = async () => {
    if (selectedZoneIds.length === 0 || loading) return;
    setLoading(true);
    try {
      const timestamp = `${dateValue}T${hourValue}:00:00`;
      const [predictionResponse, driftResponse] = await Promise.all([
        MOCK_fetchDemandModelPrediction(selectedZoneIds, timestamp, zones),
        MOCK_fetchDataDrift(timestamp),
      ]);
      const sortedPredictions = [...predictionResponse.predictions].sort(
        (a, b) => b.score - a.score,
      );
      const topPrediction = sortedPredictions[0];
      const topZone = zones.find((z) => z.id === topPrediction?.zoneId);

      setRankedPredictions(sortedPredictions);
      setPredictionsById(
        new Map(predictionResponse.predictions.map((row) => [row.zoneId, row])),
      );
      setIncludedZoneIds(new Set(predictionResponse.includedZoneIds));
      setWeatherByZoneId(predictionResponse.weatherByZoneId);
      setFeatureStats(driftResponse.featureStats);
      setLocked(true);

      if (topPrediction && topZone) {
        const nextLog: PredictionLog = {
          id: crypto.randomUUID(),
          timestamp: new Date().toISOString(),
          selectedCount: selectedZoneIds.length,
          includedCount: predictionResponse.includedZoneIds.length,
          topZoneName: topZone.name,
          topScore: topPrediction.score,
        };
        setPredictionLogs((prev) => [nextLog, ...prev].slice(0, 50));
      }
    } catch (error) {
      console.error("Failed to run forecast", error);
    } finally {
      setLoading(false);
    }
  };

  const onResetSelection = () => {
    setLocked(false);
    setSelectedZoneIds([]);
    setRankedPredictions([]);
    setPredictionsById(new Map());
    setIncludedZoneIds(new Set());
    setWeatherByZoneId({});
    setHoveredZoneId(null);
  };

  return (
    <div style={STYLES.page}>
      <AppHeader activeTab={activeTab} onTabChange={setActiveTab} />
      <main style={STYLES.content}>
        {activeTab === "driver" && (
          <DriverView
            zones={zones}
            selectedZoneIds={selectedZoneIds}
            locked={locked}
            predictionsById={predictionsById}
            includedZoneIds={includedZoneIds}
            rankedPredictions={rankedPredictions}
            weatherByZoneId={weatherByZoneId}
            hoveredZoneId={hoveredZoneId}
            dateValue={dateValue}
            hourValue={hourValue}
            loading={loading}
            disabled={selectedZoneIds.length === 0 || loading || locked}
            onToggleZone={onToggleZone}
            onDateChange={setDateValue}
            onHourChange={setHourValue}
            onPredict={onPredict}
            onResetSelection={onResetSelection}
            onHoverZoneChange={setHoveredZoneId}
          />
        )}
        {activeTab === "drift" && (
          <DataDriftView
            featureStats={featureStats}
            predictionLogs={predictionLogs}
          />
        )}
      </main>
    </div>
  );
}

const STYLES: Record<string, CSSProperties> = {
  page: {
    minHeight: "100vh",
    padding: "0 0 40px",
    boxSizing: "border-box",
  },
  header: {
    position: "sticky",
    top: 0,
    zIndex: 30,
    background: THEME.accentDark,
    backdropFilter: "blur(10px)",
    padding: "0 28px",
  },
  headerInner: {
    display: "flex",
    alignItems: "center",
    gap: 20,
    height: 56,
  },
  logoMark: {
    display: "flex",
    alignItems: "center",
    gap: 8,
  },
  logoImage: {
    width: 140,
  },
  logoText: {
    fontSize: "0.9rem",
    fontWeight: 800,
    color: THEME.text,
    letterSpacing: "-0.01em",
  },
  logoSlash: {
    color: THEME.accentDark,
    fontSize: "1.2rem",
    margin: "0 2px",
  },
  headerRight: {
    marginLeft: "auto",
    display: "flex",
    alignItems: "center",
    gap: 12,
  },
  headerBadge: {
    display: "flex",
    alignItems: "center",
    gap: 5,
    padding: "4px 10px",
    background: THEME.surface,
    border: `1px solid ${THEME.accentMid}`,
    borderRadius: 999,
    fontSize: "0.72rem",
    fontWeight: 700,
    color: THEME.accentDark,
  },
  liveDot: {
    width: 6,
    height: 6,
    borderRadius: "50%",
    background: THEME.success,
    animation: "pulse 2s infinite",
  },
  headerMeta: {
    fontSize: "0.78rem",
    color: "white",
  },
  content: {
    maxWidth: 1440,
    margin: "0 auto",
    padding: "20px 28px 0",
  },
  driverLayout: {
    display: "grid",
    gridTemplateColumns: "minmax(0, 1fr) 300px",
    gap: 14,
    alignItems: "start",
  },
  driverLeft: {
    display: "flex",
    flexDirection: "column",
    gap: 0,
  },
};
