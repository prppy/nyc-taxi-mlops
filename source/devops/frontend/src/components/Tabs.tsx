import type { CSSProperties } from "react";
import { GLOBAL_STYLES } from "../constants/appConstants";
import { THEME } from "../constants/theme";
import {
  IoCar,
  IoCarOutline,
  IoTrendingUp,
  IoTrendingUpOutline,
} from "react-icons/io5";

// TYPE
type Props = {
  activeTab: TabKey;
  onChange: (tab: TabKey) => void;
};

// CONSTANTS
const TABS = [
  {
    key: "driver",
    label: "Driver View",
    icon: {
      active: IoCar,
      inactive: IoCarOutline,
    },
  },
  {
    key: "drift",
    label: "Manager View",
    icon: {
      active: IoTrendingUp,
      inactive: IoTrendingUpOutline,
    },
  },
] as const;

export type TabKey = (typeof TABS)[number]["key"];

export function Tabs({ activeTab, onChange }: Props) {
  return (
    <div style={STYLES.wrapper}>
      {TABS.map(({ key, label, icon }) => {
        const isActive = activeTab === key;
        const Icon = isActive ? icon.active : icon.inactive;

        return (
          <button
            key={key}
            style={{
              ...STYLES.tab,
              ...GLOBAL_STYLES.buttonPressable,
              ...(isActive ? STYLES.tabActive : {}),
            }}
            onClick={() => onChange(key)}
          >
            <Icon size={16} color={isActive ? THEME.bg : THEME.grayZone} />
            {label}
          </button>
        );
      })}
    </div>
  );
}

const STYLES: Record<string, CSSProperties> = {
  wrapper: {
    display: "flex",
    alignItems: "center",
    gap: 3,
    background: THEME.bg,
    border: `1px solid ${THEME.accentDark}`,
    borderRadius: 13,
    padding: 5,
  },
  tab: {
    display: "flex",
    alignItems: "center",
    gap: 8,
    border: "none",
    background: "transparent",
    color: THEME.grayZone,
    padding: "10px 18px",
    borderRadius: 10,
    cursor: "pointer",
    fontSize: "0.88rem",
    fontWeight: 600,
    letterSpacing: "0.01em",
    transition: "all 0.15s ease",
    whiteSpace: "nowrap",
  },
  tabActive: {
    background: THEME.accent,
    color: THEME.text,
    boxShadow: "0 1px 4px rgba(13, 24, 41, 0.1), 0 0 0 1px rgba(13,24,41,0.06)",
  },
};
