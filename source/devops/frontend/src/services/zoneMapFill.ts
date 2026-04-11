import { THEME } from "../constants/theme";
import type { PredictionRow } from "../types";
import { scoreFillColor } from "./scoreDisplay";

/** opacity when the zone is outside the forecast set after run (locked). */
export const ZONE_MAP_FILL_OPACITY_EXCLUDED_WHEN_LOCKED = 0.3;

/** opacity for zones selected before forecast. */
export const ZONE_MAP_FILL_OPACITY_SELECTED = 1; 

/** default zone fill opacity (no prediction, not excluded). */
export const ZONE_MAP_FILL_OPACITY_DEFAULT = 0.35;  


export const ZONE_MAP_HOVER_FILL_OPACITY_DELTA = 0.55;
export const ZONE_MAP_HOVER_FILL_OPACITY_CAP = 0.95;

export function zonePolygonFillColor(args: {
  prediction: PredictionRow | undefined;
  locked: boolean;
  isIncluded: boolean;
  selected?: boolean;   // add this
}): string {
  if (args.prediction) return scoreFillColor(args.prediction.score);
  if (args.locked && !args.isIncluded) return THEME.disabledZone;
  if (args.selected) return THEME.grayZoneHover;  // selected = same as hover color
  return THEME.grayZone;
}

export function zonePolygonFillOpacity(args: {
  prediction: PredictionRow | undefined;
  locked: boolean;
  isIncluded: boolean;
  selected: boolean;
}): number {
  if (args.locked && !args.isIncluded) return ZONE_MAP_FILL_OPACITY_EXCLUDED_WHEN_LOCKED;
  if (args.prediction) return 1.0;        //  post-forecast: all solid, no transparency
  if (args.selected) return ZONE_MAP_FILL_OPACITY_SELECTED;  // selected: solid
  return ZONE_MAP_FILL_OPACITY_DEFAULT;   // unselected pre-forecast: faded
}

export function zonePolygonFillOpacityWithHover(
  baseOpacity: number,
  isHovered: boolean,
): number {
  if (!isHovered) return baseOpacity;
  return Math.min(
    baseOpacity + ZONE_MAP_HOVER_FILL_OPACITY_DELTA,
    ZONE_MAP_HOVER_FILL_OPACITY_CAP,
  );
}
