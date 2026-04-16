import { useEffect, useRef, useState } from "react";
import type { CSSProperties } from "react";
import { DayPicker } from "react-day-picker";
import "react-day-picker/dist/style.css";
import "../styles/calendar.css";
import { GLOBAL_STYLES, HOURS } from "../constants/appConstants";
import { THEME } from "../constants/theme";

// TYPES

type Props = {
  dateValue: string;
  hourValue: string;
  loading: boolean;
  disabled: boolean;
  locked: boolean;
  onDateChange: (value: string) => void;
  onHourChange: (value: string) => void;
  onPredict: () => void;
  onResetSelection: () => void;
};

// HELPERS

function parseDate(value: string): Date | undefined {
  if (!value) return undefined;
  const [y, m, d] = value.split("-").map(Number);
  if (!y || !m || !d) return undefined;
  return new Date(y, m - 1, d);
}

function formatDate(date: Date): string {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function formatDisplayDate(value: string): string {
  const date = parseDate(value);
  if (!date) return "Select date";
  return date.toLocaleDateString("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  });
}

// COMPONENTS

// datepicker
function DatePicker({
  value,
  disabled,
  onChange,
}: {
  value: string;
  disabled?: boolean;
  onChange: (value: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const selected = parseDate(value);
  const today = new Date();
  today.setHours(0, 0, 0, 0);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node))
        setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  return (
    <div ref={ref} style={STYLES.pickerWrap}>
      <button
        type="button"
        style={{
          ...STYLES.trigger,
          ...(disabled ? STYLES.triggerDisabled : {}),
        }}
        onClick={() => !disabled && setOpen((o) => !o)}
      >
        <span style={STYLES.triggerText}>{formatDisplayDate(value)}</span>
        <span
          style={{
            ...STYLES.chevron,
            transform: open ? "rotate(180deg)" : "rotate(0deg)",
          }}
        >
          ▾
        </span>
      </button>

      {open && (
        <div style={{ ...STYLES.dropdown, ...STYLES.calDropdown }}>
          <DayPicker
            mode="single"
            selected={selected}
            defaultMonth={selected ?? today}
            disabled={{ before: today }}
            onSelect={(date: Date | undefined) => {
              if (date) {
                onChange(formatDate(date));
                setOpen(false);
              }
            }}
            showOutsideDays
            captionLayout="dropdown"
            startMonth={today}
            endMonth={new Date(2035, 11)}
            classNames={{
              selected: "rdp-day-selected",
              today: "rdp-day-today",
            }}
          />
        </div>
      )}
    </div>
  );
}

// hour picker
function HourPicker({
  value,
  dateValue,
  disabled,
  onChange,
}: {
  value: string;
  dateValue: string;
  disabled?: boolean;
  onChange: (value: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const [hovered, setHovered] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node))
        setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  const now = new Date();
  const todayStr = formatDate(now);
  const isToday = dateValue === todayStr;
  const currentHour = now.getHours();

  const displayLabel = value
    ? `${String(value).padStart(2, "0")}:00`
    : "Select hour";

  return (
    <div ref={ref} style={STYLES.pickerWrap}>
      <button
        type="button"
        style={{
          ...STYLES.trigger,
          ...(disabled ? STYLES.triggerDisabled : {}),
        }}
        onClick={() => !disabled && setOpen((o) => !o)}
      >
        <span style={STYLES.triggerText}>{displayLabel}</span>
        <span
          style={{
            ...STYLES.chevron,
            transform: open ? "rotate(180deg)" : "rotate(0deg)",
          }}
        >
          ▾
        </span>
      </button>

      {open && (
        <div style={{ ...STYLES.dropdown, ...STYLES.hourDropdown }}>
          {HOURS.map((hour) => {
            const isPast = isToday && Number(hour) < currentHour;
            return (
              <button
                key={hour}
                type="button"
                disabled={isPast}
                style={{
                  ...STYLES.hourOption,
                  ...(String(hour) === String(value)
                    ? STYLES.hourOptionSelected
                    : {}),
                  ...(hovered === hour && String(hour) !== String(value)
                    ? STYLES.hourOptionHovered
                    : {}),
                  ...(isPast ? STYLES.hourOptionDisabled : {}),
                }}
                onMouseEnter={() => !isPast && setHovered(hour)}
                onMouseLeave={() => setHovered(null)}
                onClick={() => {
                  if (!isPast) {
                    onChange(String(hour));
                    setOpen(false);
                  }
                }}
              >
                {String(hour).padStart(2, "0")}:00
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

// control panel (MAIN COMPONENT)
export function ControlPanel({
  dateValue,
  hourValue,
  loading,
  disabled,
  locked,
  onDateChange,
  onHourChange,
  onPredict,
  onResetSelection,
}: Props) {
  return (
    <section style={{ ...GLOBAL_STYLES.panel, marginBottom: 16 }}>
      <div style={STYLES.row}>
        <div style={STYLES.field}>
          <label style={STYLES.label}>Date</label>
          <DatePicker
            value={dateValue}
            disabled={locked}
            onChange={onDateChange}
          />
        </div>

        <div style={STYLES.field}>
          <label style={STYLES.label}>Hour</label>
          <HourPicker
            value={hourValue}
            dateValue={dateValue}
            disabled={locked}
            onChange={onHourChange}
          />
        </div>

        {!locked ? (
          <button
            onClick={onPredict}
            style={{
              ...STYLES.buttonPrimary,
              ...(disabled ? STYLES.buttonDisabled : {}),
              ...GLOBAL_STYLES.buttonPressable,
            }}
            disabled={disabled}
          >
            {loading ? "Forecasting…" : "Get Demand Heatmap"}
          </button>
        ) : (
          <button
            onClick={onResetSelection}
            style={{
              ...STYLES.buttonPrimary,
              ...GLOBAL_STYLES.buttonPressable,
            }}
          >
            New Selection
          </button>
        )}
      </div>
    </section>
  );
}

// STYLES

const STYLES: Record<string, CSSProperties> = {
  row: {
    display: "flex",
    alignItems: "flex-end",
    gap: 14,
    flexWrap: "wrap",
  },
  field: {
    display: "flex",
    flexDirection: "column",
    gap: 5,
  },
  label: {
    fontSize: "0.68rem",
    fontWeight: 700,
    letterSpacing: "0.09em",
    textTransform: "uppercase",
    color: THEME.mutedText,
  },

  // picker
  pickerWrap: { position: "relative" },

  trigger: {
    display: "flex",
    alignItems: "center",
    gap: 8,
    padding: "9px 12px",
    border: `1.5px solid ${THEME.accentDark}`,
    borderRadius: 9,
    fontSize: "0.875rem",
    fontWeight: 500,
    fontFamily: "inherit",
    background: THEME.inputBg,
    color: THEME.text,
    cursor: "pointer",
    minWidth: 158,
    whiteSpace: "nowrap",
    transition: "border-color 0.15s, box-shadow 0.15s",
    outline: "none",
  },
  triggerDisabled: {
    opacity: 0.45,
    cursor: "not-allowed",
  },
  triggerText: {
    flex: 1,
  },
  chevron: {
    fontSize: "1.3rem",
    width: 18,
    height: 18,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    color: THEME.mutedText,
    transition: "transform 0.15s",
    flex: "0 0 auto",
  },

  // dropdown
  dropdown: {
    position: "absolute",
    top: "calc(100% + 6px)",
    left: 0,
    zIndex: 200,
    background: "#fff",
    border: `1.5px solid ${THEME.accentDark}`,
    borderRadius: 12,
    boxShadow: "0 8px 32px rgba(13,24,41,0.15)",
  },

  // calendar dropdown
  calDropdown: { padding: "6px" },

  // button
  buttonPrimary: {
    padding: "9px 22px",
    background: THEME.accent,
    color: "#111",
    border: `1.5px solid ${THEME.accentMid}`,
    borderRadius: 9,
    fontSize: "0.875rem",
    fontWeight: 700,
    letterSpacing: "0.01em",
    cursor: "pointer",
    whiteSpace: "nowrap",
    alignSelf: "flex-end",
    fontFamily: "inherit",
    transition: "opacity 0.15s, box-shadow 0.15s",
  },
  buttonDisabled: {
    opacity: 0.45,
    cursor: "not-allowed",
  },

  // hour picker
  hourDropdown: {
    padding: "4px",
    maxHeight: 220,
    overflowY: "auto" as const,
    minWidth: 120,
  },
  hourOption: {
    display: "block",
    width: "100%",
    padding: "7px 12px",
    border: "none",
    borderRadius: 7,
    background: "transparent",
    color: THEME.text,
    fontFamily: "inherit",
    fontSize: "0.875rem",
    fontWeight: 500,
    cursor: "pointer",
    textAlign: "left" as const,
  },
  hourOptionSelected: {
    background: THEME.accent,
    color: "#111",
    fontWeight: 700,
  },
  hourOptionHovered: {
    background: `color-mix(in srgb, ${THEME.accent} 20%, transparent)`,
  },
  hourOptionDisabled: {
    opacity: 0.35,
    cursor: "not-allowed",
  },
};
