const TAXI_BLUE  = "rgb(32, 61, 100)"
const LIGHTER_TAXI_BLUE = "rgb(50, 92, 146)"
const TAXI_YELLOW = "#f7b731"


const SCORE_LOW = "#ffd18b";   // warm yellow-orange
const SCORE_MID = "#fb923c";   // strong orange
const SCORE_HIGH = "#ea580c";  // deep taxi orange
export const THEME = {
  bg: TAXI_BLUE,
  panel: "#ffffff",
  text: "#111827",
  mutedText: "#64748b",
  subtleText: "#94a3b8",
  accent: TAXI_YELLOW,
  accentLight: TAXI_YELLOW,
  accentMid: "#ffd976",
  accentDark: LIGHTER_TAXI_BLUE,
  surface: "#f8fafc",
  inputBg: "#ffffff",
  grayZone: "#c8d4e6",
  grayZoneHover: "#8fa8c8",
  disabledZone: "#e3eaf4",
  selectedStroke:LIGHTER_TAXI_BLUE,
  zoneStroke: "#9eb2cf",
  // status colors
  success: "#16a34a",
  successLight: "#f0fdf4",
  warning: "#d97706",
  warningLight: "#fffbeb",
  danger: "#dc2626",
  dangerLight: "#fef2f2",
  orange: "#ea580c",
  orangeLight: "#fff7ed",

  scoreLow: SCORE_LOW,
  scoreMid: SCORE_MID,
  scoreHigh: SCORE_HIGH

} as const;
