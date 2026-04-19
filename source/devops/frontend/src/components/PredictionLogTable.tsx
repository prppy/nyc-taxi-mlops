import type { CSSProperties } from "react";
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from "@mui/material";

import { GLOBAL_STYLES } from "../constants/appConstants";
import { THEME } from "../constants/theme";
import { formatDemandScore, scoreFillColor } from "../services/scoreDisplay";
import type { PredictionLog } from "../types";

type Props = {
  predictionLogs: PredictionLog[];
  driftLoading: boolean;
};

type PredictionLogRow = {
  id: string;
  time: string;
  date: string;
  topZoneName: string;
  topScoreLabel: string;
  topScoreColor: string;
  selectedCount: number;
  includedCount: number;
};

function mapLogsToRows(logs: PredictionLog[]): PredictionLogRow[] {
  return logs.map((log) => {
    const d = new Date(log.timestamp);

    return {
      id: log.id,
      time: d.toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit",
      }),
      date: d.toLocaleDateString([], {
        month: "short",
        day: "numeric",
        year: "numeric",
      }),
      topZoneName: log.topZoneName,
      topScoreLabel: formatDemandScore(log.topScore),
      topScoreColor: scoreFillColor(log.topScore),
      selectedCount: log.selectedCount,
      includedCount: log.includedCount,
    };
  });
}

export function PredictionLogTable({ predictionLogs, driftLoading }: Props) {
  const rows = mapLogsToRows(predictionLogs);

  return (
    <section style={GLOBAL_STYLES.panel}>
      <div style={GLOBAL_STYLES.sectionLabel}>
        Prediction Log
        {!driftLoading && rows.length > 0 && ` · ${rows.length} entries`}
      </div>

      <TableContainer sx={STYLES.tableWrap}>
        <Table size="small" sx={STYLES.table}>
          <TableHead>
            <TableRow>
              <TableCell sx={STYLES.th}>Timestamp</TableCell>
              <TableCell sx={STYLES.th}>Top Zone</TableCell>
              <TableCell sx={{ ...STYLES.th, textAlign: "right" }}>
                Top Score
              </TableCell>
              <TableCell sx={{ ...STYLES.th, textAlign: "right" }}>
                Selected Zones
              </TableCell>
              <TableCell sx={{ ...STYLES.th, textAlign: "right" }}>
                Predicted Zones
              </TableCell>
            </TableRow>
          </TableHead>

          <TableBody>
            {driftLoading ? (
              <TableRow>
                <TableCell
                  colSpan={5}
                  sx={{ ...STYLES.td, ...STYLES.emptyRow }}
                >
                  Loading recent prediction activity...
                </TableCell>
              </TableRow>
            ) : rows.length === 0 ? (
              <TableRow>
                <TableCell
                  colSpan={5}
                  sx={{ ...STYLES.td, ...STYLES.emptyRow }}
                >
                  No predictions logged yet — run a driver forecast to see
                  entries here.
                </TableCell>
              </TableRow>
            ) : (
              rows.map((row) => (
                <TableRow key={row.id}>
                  <TableCell sx={STYLES.td}>
                    <div style={{ fontWeight: 600 }}>{row.time}</div>
                    <div style={STYLES.tdMuted}>{row.date}</div>
                  </TableCell>

                  <TableCell sx={STYLES.td}>
                    <div style={{ fontWeight: 600 }}>{row.topZoneName}</div>
                  </TableCell>

                  <TableCell sx={{ ...STYLES.td, textAlign: "right" }}>
                    <span
                      style={{
                        fontWeight: 800,
                        fontSize: "0.95rem",
                        color: row.topScoreColor,
                      }}
                    >
                      {row.topScoreLabel}
                    </span>
                  </TableCell>

                  <TableCell
                    sx={{
                      ...STYLES.td,
                      textAlign: "right",
                      color: THEME.mutedText,
                    }}
                  >
                    {row.selectedCount}
                  </TableCell>

                  <TableCell
                    sx={{
                      ...STYLES.td,
                      textAlign: "right",
                      color: THEME.mutedText,
                    }}
                  >
                    {row.includedCount}
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </TableContainer>
    </section>
  );
}

const STYLES: Record<string, CSSProperties> = {
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
    borderBottom: `1px solid ${THEME.grayZone}`,
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