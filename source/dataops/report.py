import io
import logging
import pandas as pd
from airflow.utils.email import send_email
from typing import List

from utils.config import get_month_year
from utils.db import get_engine
from utils.monitoring import monitor
from utils.alerting import ALERT_EMAILS


logger = logging.getLogger(__name__)


# constants

# ── columns to profile ────────────────────────────────────────────────────────
# version 1: pickup zone + hour aggregated fact table
FACT_NUMERIC = [
    "demand",
    "avg_trip_distance",
    "avg_total_amount",
]

FACT_CATEGORICAL = [
    "pulocationid",
]

WEATHER_NUMERIC  = ["temperature_mean", "precipitation_sum", "wind_speed_max"]

# ── sampling ──────────────────────────────────────────────────────────────────
# % passed to TABLESAMPLE SYSTEM(...): 5 means ~5% of pages scanned.
TABLESAMPLE_PCT  = 5
# hard row cap applied on top of TABLESAMPLE (guards against huge samples).
SAMPLE_ROW_LIMIT = 100_000
# fallback LIMIT used when the DB doesn't support TABLESAMPLE.
FALLBACK_ROW_LIMIT = 50_000

# ── anomaly thresholds ────────────────────────────────────────────────────────
# flag if this share of trips has zero trip_distance (e.g. 0.05 = 5%).
ZERO_DISTANCE_RATE_THRESHOLD = 0.05
# flag if total_amount exceeds this dollar value (likely outlier / data error).
HIGH_FARE_THRESHOLD          = 500
# flag any column whose null rate exceeds this share (e.g. 0.30 = 30%).
HIGH_NULL_RATE_THRESHOLD     = 0.30
# flag if proportion of zero-demand rows exceeds this threshold.
ZERO_DEMAND_RATE_THRESHOLD = 0.5
# flag any row with demand above this value (possible outlier or surge event).
HIGH_DEMAND_THRESHOLD = 3000

# ── chart appearance ──────────────────────────────────────────────────────────
CHART_DPI             = 110      # png resolution; raise for crisper images
HIST_BINS             = 40       # number of bins in each histogram
HIST_COLOR            = "#4A8FD4"
NULL_BAR_COLOR        = "#E07B54"
# outlier cap: values above this percentile are clipped before plotting.
OUTLIER_CAP_QUANTILE  = 0.995
# max categories shown in a bar chart (long-tail is trimmed).
CAT_BAR_MAX_CATEGORIES = 15
# max columns per row in the histogram grid.
HIST_GRID_MAX_COLS    = 3


# private helpers

def _fig_to_bytes(fig) -> bytes:
    """Render a matplotlib figure to PNG bytes."""
    import matplotlib.pyplot as plt
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=CHART_DPI, bbox_inches="tight")
    buf.seek(0)
    data = buf.read()
    plt.close(fig)
    return data


def _style_axes(ax, title):
    """Apply a minimal, clean style to an axes."""
    import matplotlib.ticker as mticker
    ax.set_title(title, fontsize=9, fontweight="bold", pad=6)
    ax.tick_params(labelsize=7)
    ax.spines[["top", "right"]].set_visible(False)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))


def _numeric_dist_chart(df: pd.DataFrame, columns: List[str], title: str) -> bytes:
    """
    Grid of histograms — one subplot per numeric column.
    Returns PNG bytes.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
 
    cols_present = [c for c in columns if c in df.columns]
    if not cols_present:
        return b""

    n = len(cols_present)
    ncols = min(HIST_GRID_MAX_COLS, n)
    nrows = (n + ncols - 1) // ncols

    fig, axes = plt.subplots(nrows, ncols, figsize=(4.5 * ncols, 3 * nrows))
    axes = [axes] if n == 1 else list(axes.flat)

    for ax, col in zip(axes, cols_present):
        series = df[col].dropna()
        cap = series.quantile(OUTLIER_CAP_QUANTILE)
        series = series[series <= cap]
        ax.hist(series, bins=HIST_BINS, color=HIST_COLOR, edgecolor="white", linewidth=0.4)
        _style_axes(ax, col)
        ax.set_xlabel("")

    # hide any unused subplots
    for ax in axes[len(cols_present):]:
        ax.set_visible(False)

    fig.suptitle(title, fontsize=11, fontweight="bold", y=1.01)
    fig.tight_layout()
    return _fig_to_bytes(fig)


def _categorical_bar_charts(df: pd.DataFrame, columns: List[str], title: str) -> bytes:
    """
    Grid of horizontal bar charts — one subplot per categorical column.
    Returns PNG bytes.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    cols_present = [c for c in columns if c in df.columns]
    if not cols_present:
        return b""

    n = len(cols_present)
    ncols = min(HIST_GRID_MAX_COLS, n)
    nrows = (n + ncols - 1) // ncols

    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 3 * nrows))
    axes = [axes] if n == 1 else list(axes.flat)

    for ax, col in zip(axes, cols_present):
        vc = df[col].value_counts().head(CAT_BAR_MAX_CATEGORIES)

        ax.barh(
            vc.index.astype(str)[::-1],
            vc.values[::-1],
            color=HIST_COLOR,
            height=0.6
        )

        _style_axes(ax, col)
        ax.set_xlabel("trips", fontsize=7)

    # hide unused axes
    for ax in axes[len(cols_present):]:
        ax.set_visible(False)

    fig.suptitle(title, fontsize=11, fontweight="bold", y=1.01)
    fig.tight_layout()

    return _fig_to_bytes(fig)

def _null_rate_charts(df: pd.DataFrame, title: str) -> bytes:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker

    null_rates = df.isnull().mean()
    null_rates = null_rates[null_rates > 0]

    if null_rates.empty:
        return b""

    cols = null_rates.index.tolist()
    values = null_rates.values

    n = len(cols)
    ncols = min(HIST_GRID_MAX_COLS, n)
    nrows = (n + ncols - 1) // ncols

    fig, axes = plt.subplots(nrows, ncols, figsize=(4.5 * ncols, 2.5 * nrows))
    axes = [axes] if n == 1 else list(axes.flat)

    for ax, col, val in zip(axes, cols, values):
        ax.barh([col], [val], color=NULL_BAR_COLOR)
        _style_axes(ax, col)
        ax.xaxis.set_major_formatter(mticker.PercentFormatter(xmax=1))
        ax.set_xlim(0, 1)

    # hide unused axes
    for ax in axes[len(cols):]:
        ax.set_visible(False)

    fig.suptitle(title, fontsize=11, fontweight="bold", y=1.02)
    fig.tight_layout()

    return _fig_to_bytes(fig)


def _stats_html(df: pd.DataFrame, columns: List[str]) -> str:
    """
    Compact HTML table of describe() stats for the given numeric columns.
    Only count / mean / std / min / 25% / 50% / 75% / max are kept.
    """
    cols_present = [c for c in columns if c in df.columns]
    if not cols_present:
        return "<p>No numeric columns found.</p>"

    if df.empty:
        return "<p>No data found for this period.</p>"

    described = df[cols_present].describe()

    wanted_rows = ["count", "mean", "std", "min", "25%", "50%", "75%", "max"]
    available_rows = [r for r in wanted_rows if r in described.index]

    if not available_rows:
        return "<p>No descriptive statistics available.</p>"

    stats = described.loc[available_rows].T.round(2)

    rows = "".join(
        f"<tr><td><b>{idx}</b></td>"
        + "".join(
            f"<td>{v:,.2f}</td>" if pd.notnull(v) else "<td>-</td>"
            for v in row
        )
        + "</tr>"
        for idx, row in stats.iterrows()
    )

    headers = "".join(f"<th>{h}</th>" for h in ["column"] + list(stats.columns))
    return f"""
    <table style="border-collapse:collapse;font-size:12px;width:100%">
      <thead>
        <tr style="background:#f0f4f8">{headers}</tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
    """


def _collect_charts(fact_df: pd.DataFrame, weather_df: pd.DataFrame) -> dict:
    """Returns {filename: png_bytes} for all report charts."""
    charts = {}

    fig_bytes = _numeric_dist_chart(fact_df, FACT_NUMERIC, "Fact trips pickup — numeric distributions")
    if fig_bytes:
        charts["fact_numeric_distributions.png"] = fig_bytes

    fig_bytes = _categorical_bar_charts(
        fact_df,
        FACT_CATEGORICAL,
        "Fact trips pickup — categorical distributions"
    )
    if fig_bytes:
        charts["categorical_distributions.png"] = fig_bytes

    fig_bytes = _null_rate_charts(
        fact_df,
        "Null rate by column (fact_trips_pickup sample)"
        )
    if fig_bytes:
        charts["null_rates.png"] = fig_bytes


    fig_bytes = _numeric_dist_chart(weather_df, WEATHER_NUMERIC, "Weather — numeric distributions")
    if fig_bytes:
        charts["weather_distributions.png"] = fig_bytes

    return charts


# task helpers

def _anomaly_flags(df: pd.DataFrame) -> List[str]:
    """
    Simple rule-based anomaly checks for aggregated pickup zone-hour fact table.
    Returns a list of warning strings.
    """
    flags = []

    if df.empty:
        flags.append("⚠ No fact_trips_pickup rows found for this period")
        return flags

    if "demand" in df.columns:
        zero_demand = (df["demand"] == 0).mean()
        if zero_demand > ZERO_DEMAND_RATE_THRESHOLD:
            flags.append(f"⚠ {zero_demand:.1%} of rows have zero demand")

        high_demand = (df["demand"] > HIGH_DEMAND_THRESHOLD).sum()
        if high_demand > 0:
            flags.append(f"⚠ {high_demand:,} rows have demand > {HIGH_DEMAND_THRESHOLD} (possible outlier)")

    if "avg_total_amount" in df.columns:
        neg_total = (df["avg_total_amount"] < 0).sum()
        if neg_total > 0:
            flags.append(f"⚠ {neg_total:,} rows have negative avg_total_amount")
        
        high_total = (df["avg_total_amount"] > HIGH_FARE_THRESHOLD).sum()
        if high_total > 0:
            flags.append(
                f"⚠ {high_total:,} rows have avg_total_amount > {HIGH_FARE_THRESHOLD} (possible outlier / data error)"
            )

    if "avg_trip_distance" in df.columns:
        neg_distance = (df["avg_trip_distance"] < 0).sum()
        if neg_distance > 0:
            flags.append(f"⚠ {neg_distance:,} rows have negative avg_trip_distance")

        zero_distance_rate = (df["avg_trip_distance"] == 0).mean()
        if zero_distance_rate > ZERO_DISTANCE_RATE_THRESHOLD:
            flags.append(
                f"⚠ {zero_distance_rate:.1%} of rows have avg_trip_distance = 0 "
                f"(threshold {ZERO_DISTANCE_RATE_THRESHOLD:.0%})"
            )

    null_rates = df.isnull().mean()
    high_null = null_rates[null_rates > HIGH_NULL_RATE_THRESHOLD]
    for col, rate in high_null.items():
        flags.append(f"⚠ Column '{col}' has {rate:.1%} null values")

    return flags


# data loaders

# joining columns for a targeted SELECT instead of SELECT * (more efficient!)
# version 1: pickup zone + hour fact table
FACT_COLUMNS = list(dict.fromkeys(
    FACT_NUMERIC + FACT_CATEGORICAL + ["hour_ts"]
))

# version 2: pickup-dropoff pair table
# FACT_COLUMNS = list(dict.fromkeys(
#     FACT_NUMERIC + ["pulocationid", "dolocationid", "hour_ts"]
# ))

WEATHER_COLUMNS = WEATHER_NUMERIC + ["date"]


def load_fact_sample(period: str) -> pd.DataFrame:
    cols = ", ".join(FACT_COLUMNS)
    engine = get_engine()

    sample_query = f"""
        SELECT {cols}
        FROM (
            SELECT {cols}
            FROM fact_trips_pickup
            WHERE DATE_TRUNC('month', hour_ts) = :period_date
        ) monthly_data
        TABLESAMPLE SYSTEM({TABLESAMPLE_PCT})
        LIMIT {SAMPLE_ROW_LIMIT}
    """

    fallback_query = f"""
        SELECT {cols}
        FROM fact_trips_pickup
        WHERE DATE_TRUNC('month', hour_ts) = :period_date
        LIMIT {FALLBACK_ROW_LIMIT}
    """

    with engine.connect() as conn:
        try:
            return pd.read_sql(
                sample_query,
                conn,
                params={"period_date": f"{period}-01"},
                parse_dates=["hour_ts"],
            )
        except Exception:
            return pd.read_sql(
                fallback_query,
                conn,
                params={"period_date": f"{period}-01"},
                parse_dates=["hour_ts"],
            )

def load_weather(period: str) -> pd.DataFrame:
    cols = ", ".join(WEATHER_COLUMNS)
    engine = get_engine()

    with engine.connect() as conn:
        try:
            return pd.read_sql(
                f"""
                SELECT {cols}
                FROM dim_weather
                WHERE DATE_TRUNC('month', date) = '{period}-01'
                """,
                conn,
                parse_dates=["date"],
            )
        except Exception:
            return pd.DataFrame()


# email section builders

def _build_fact_section(fact_df: pd.DataFrame) -> str:
    """Stats table for fact_trips_pickup — charts are sent as attachments."""
    stats_html = _stats_html(fact_df, FACT_NUMERIC)

    return f"""
      <h3>Fact trips pickup — descriptive statistics</h3>
      {stats_html}
      <h3>Categorical distributions</h3>
      <p style="color:#666;font-size:12px">
        See attached: categorical_distributions.png
        </p>
    """


def _build_null_section(fact_df: pd.DataFrame) -> str:
    """Null rate summary — chart is sent as attachment."""
    nonzero_null = fact_df.isnull().mean().sort_values(ascending=False)
    nonzero_null = nonzero_null[nonzero_null > 0]

    if nonzero_null.empty:
        return "<h3>Null rates</h3><p>No nulls found.</p>"

    return "<h3>Null rates</h3><p style=\"color:#666;font-size:12px\">See attached: null_rates.png</p>"


def _build_weather_section(weather_df: pd.DataFrame) -> str:
    """Stats table for dim_weather — chart is sent as attachment."""
    stats_html = _stats_html(weather_df, WEATHER_NUMERIC)

    return f"""
      <h3>Weather — descriptive statistics</h3>
      {stats_html}
      <p style="color:#666;font-size:12px">See attached: weather_distributions.png</p>
    """


def _build_flag_block(flags: List[str]) -> str:
    """Green all-clear banner or amber warning list."""
    if not flags:
        return '<div style="background:#e8f5e9;border-left:4px solid #43a047;padding:10px 14px;margin:12px 0;border-radius:4px">✅ No anomalies detected</div>'
    items = "".join(f"<li>{f}</li>" for f in flags)
    return f"""
    <div style="background:#fff8e1;border-left:4px solid #f9a825;padding:10px 14px;margin:12px 0;border-radius:4px">
      <b>Anomaly flags</b>
      <ul style="margin:6px 0 0;padding-left:18px">{items}</ul>
    </div>
    """


def _build_email_body(period: str, n_fact: int, n_weather: int,
                      flags: List[str], fact_df: pd.DataFrame,
                      weather_df: pd.DataFrame) -> str:
    """Assemble the full HTML email body from section builders."""
    return f"""
    <div style="font-family:Arial,sans-serif;font-size:13px;max-width:900px;color:#222">

      <h2 style="margin-bottom:4px">Data Quality Report — {period}</h2>
      <p style="color:#666;margin-top:0">
        Sampled <b>{n_fact:,}</b> fact_trips_pickup rows &nbsp;|&nbsp;
        <b>{n_weather:,}</b> dim_weather rows
      </p>

      {_build_flag_block(flags)}

      <hr style="border:none;border-top:1px solid #e0e0e0;margin:16px 0"/>

      {_build_fact_section(fact_df)}
      {_build_null_section(fact_df)}
      {_build_weather_section(weather_df)}

      <p style="color:#999;font-size:11px;margin-top:24px">
        Generated by Airflow DAG · {period} · charts based on a {TABLESAMPLE_PCT}% sample of fact_trips_pickup
      </p>
    </div>
    """


# email sending

def _send_email_with_charts(subject: str, html_body: str, images: dict, period: str):
    import tempfile, os
    import uuid
    # avoids parallel runs even for the same period
    run_id = str(uuid.uuid4())
    base_dir = os.path.join(
        tempfile.gettempdir(),
        "reports",
        period,
        run_id
    )
    os.makedirs(base_dir, exist_ok=True)

    tmp_files = []
    try:
        for filename, png_bytes in images.items():
            tmp_path = os.path.join(
                base_dir,
                filename
            )
            with open(tmp_path, "wb") as f:
                f.write(png_bytes)

            tmp_files.append(tmp_path)

        send_email(
            to=ALERT_EMAILS,
            subject=subject,
            html_content=html_body,
            files=tmp_files
        )

    finally:
        # comment out if we want to keep the files (won't be temp anymore)
        for path in tmp_files:
            os.unlink(path)

# main task

@monitor
def report_data(**context):
    execution_date = context["execution_date"]
    year, month = get_month_year(execution_date)
    period = f"{year}-{month:02d}"

    logger.info(f"Generating data quality report for {period}")

    fact_df    = load_fact_sample(period)
    weather_df = load_weather(period)
    logger.info(f"Sampled {len(fact_df):,} fact_trips_pickup rows, {len(weather_df):,} weather rows")

    flags   = _anomaly_flags(fact_df)
    charts  = _collect_charts(fact_df, weather_df)
    body    = _build_email_body(period, len(fact_df), len(weather_df), flags, fact_df, weather_df)
    subject = f"[DataOps] Quality Report — {period}" + (" ⚠ ANOMALIES DETECTED" if flags else " ✔ Clean")

    _send_email_with_charts(subject, body, charts, period)
    logger.info(f"Report email sent for {period} ({len(flags)} flags)")