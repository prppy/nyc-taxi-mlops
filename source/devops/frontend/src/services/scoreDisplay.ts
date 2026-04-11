import {THEME} from "../constants/theme"

// utils that DETERMINE heatmap color (& intensity) according to score

// make sure score always fall between [0, 1]
export function clampDemandScore(score: number): number {
  if (Number.isNaN(score)) return 0;
  return Math.min(1, Math.max(0, score));
}


/**
 * linear interpolation between two hex colors -> gives us our gradients
 */
function lerp(a: string, b: string, t: number): string {
  const ah = parseInt(a.slice(1), 16);
  const bh = parseInt(b.slice(1), 16);

  const ar = (ah >> 16) & 0xff, ag = (ah >> 8) & 0xff, ab = ah & 0xff;
  const br = (bh >> 16) & 0xff, bg = (bh >> 8) & 0xff, bb = bh & 0xff;

  return `rgb(${
    Math.round(ar + (br - ar) * t)
  }, ${
    Math.round(ag + (bg - ag) * t)
  }, ${
    Math.round(ab + (bb - ab) * t)
  })`;
}

export function scoreFillColor(score: number): string {
  const s = clampDemandScore(score);

  if (s < 0.5) {
    return lerp(THEME.scoreLow, THEME.scoreMid, s / 0.5);
  }

  return lerp(THEME.scoreMid, THEME.scoreHigh, (s - 0.5) / 0.5);
}


export function formatDemandScore(score: number): string {
  return `${Math.round(clampDemandScore(score) * 100)}%`;
}