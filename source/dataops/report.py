import io
import base64
import logging
import smtplib
import pandas as pd

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from typing import List

from utils.db import engine
from utils.monitoring import monitor
from utils.alerting import ALERT_EMAILS

logger = logging.getLogger(__name__)


# constants

# ── columns to profile ────────────────────────────────────────────────────────
FACT_NUMERIC = [
    "trip_distance", "fare_amount", "total_amount",
    "tip_amount", "tolls_amount", "congestion_surcharge",
    "passenger_count", "trip_time",
]
FACT_CATEGORICAL = ["taxi_type", "payment_type", "ratecode_id"]
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
# flag if this share of trips has zero passenger_count (e.g. 0.10 = 10%).
ZERO_PAX_RATE_THRESHOLD      = 0.10
# flag any column whose null rate exceeds this share (e.g. 0.30 = 30%).
HIGH_NULL_RATE_THRESHOLD     = 0.30

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


def _categorical_bar_chart(df: pd.DataFrame, column: str) -> bytes:
    """
    Horizontal bar chart for a categorical column's value counts.
    Returns PNG bytes.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
 
    if column not in df.columns:
        return b""

    vc = df[column].value_counts().head(CAT_BAR_MAX_CATEGORIES)
    fig, ax = plt.subplots(figsize=(5, max(2.5, 0.4 * len(vc))))
    ax.barh(vc.index.astype(str)[::-1], vc.values[::-1], color=HIST_COLOR, height=0.6)
    _style_axes(ax, column)
    ax.set_xlabel("trips", fontsize=7)
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

    stats = df[cols_present].describe().loc[
        ["count", "mean", "std", "min", "25%", "50%", "75%", "max"]
    ].T.round(2)

    rows = "".join(
        f"<tr><td><b>{idx}</b></td>"
        + "".join(f"<td>{v:,.2f}</td>" for v in row)
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

    fig_bytes = _numeric_dist_chart(fact_df, FACT_NUMERIC, "Fact trips — numeric distributions")
    if fig_bytes:
        charts["fact_numeric_distributions.png"] = fig_bytes

    for col in FACT_CATEGORICAL:
        fig_bytes = _categorical_bar_chart(fact_df, col)
        if fig_bytes:
            charts[f"categorical_{col}.png"] = fig_bytes

    nonzero_null = fact_df.isnull().mean().sort_values(ascending=False)
    nonzero_null = nonzero_null[nonzero_null > 0]
    if not nonzero_null.empty:
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
        fig, ax = plt.subplots(figsize=(7, max(2.5, 0.35 * len(nonzero_null))))
        ax.barh(nonzero_null.index[::-1], nonzero_null.values[::-1], color=NULL_BAR_COLOR, height=0.6)
        _style_axes(ax, "Null rate by column (fact_trips sample)")
        ax.xaxis.set_major_formatter(mticker.PercentFormatter(xmax=1))
        ax.set_xlabel("null %", fontsize=7)
        fig.tight_layout()
        charts["null_rates.png"] = _fig_to_bytes(fig)

    fig_bytes = _numeric_dist_chart(weather_df, WEATHER_NUMERIC, "Weather — numeric distributions")
    if fig_bytes:
        charts["weather_distributions.png"] = fig_bytes

    return charts


# task helpers

def _anomaly_flags(df: pd.DataFrame) -> List[str]:
    """
    Simple rule-based anomaly checks. Returns a list of warning strings.
    Extend these thresholds to match your domain expectations.
    """
    flags = []

    if "trip_distance" in df.columns:
        zero_dist = (df["trip_distance"] == 0).mean()
        if zero_dist > ZERO_DISTANCE_RATE_THRESHOLD:
            flags.append(f"⚠ {zero_dist:.1%} of trips have zero trip_distance")

    if "fare_amount" in df.columns:
        neg_fare = (df["fare_amount"] < 0).sum()
        if neg_fare > 0:
            flags.append(f"⚠ {neg_fare:,} rows have negative fare_amount")

    if "total_amount" in df.columns:
        high_fare = (df["total_amount"] > HIGH_FARE_THRESHOLD).sum()
        if high_fare > 0:
            flags.append(f"⚠ {high_fare:,} rows have total_amount > ${HIGH_FARE_THRESHOLD} (possible outlier)")

    if "pickup_datetime" in df.columns and "dropoff_datetime" in df.columns:
        invalid_time = (df["pickup_datetime"] >= df["dropoff_datetime"]).sum()
        if invalid_time > 0:
            flags.append(f"⚠ {invalid_time:,} rows have pickup_datetime >= dropoff_datetime")

    if "passenger_count" in df.columns:
        zero_pax = (df["passenger_count"] == 0).mean()
        if zero_pax > ZERO_PAX_RATE_THRESHOLD:
            flags.append(f"⚠ {zero_pax:.1%} of trips have zero passenger_count")

    # null rate check across all columns
    null_rates = df.isnull().mean()
    high_null = null_rates[null_rates > HIGH_NULL_RATE_THRESHOLD]
    for col, rate in high_null.items():
        flags.append(f"⚠ Column '{col}' has {rate:.1%} null values")

    return flags


# data loaders

# joining columns for a targeted SELECT instead of SELECT * (more efficient!)
FACT_COLUMNS = list(set(
    FACT_NUMERIC + FACT_CATEGORICAL +
    ["pickup_datetime", "dropoff_datetime", "request_datetime", "on_scene_datetime"]
))
WEATHER_COLUMNS = WEATHER_NUMERIC + ["date"]


def load_fact_sample(period: str) -> pd.DataFrame:
    cols = ", ".join(FACT_COLUMNS)
    try:
        return pd.read_sql(
            f"""
            SELECT {cols}
            FROM fact_trips TABLESAMPLE SYSTEM({TABLESAMPLE_PCT})
            WHERE DATE_TRUNC('month', pickup_datetime) = '{period}-01'
            LIMIT {SAMPLE_ROW_LIMIT}
            """,
            engine,
            parse_dates=["pickup_datetime", "dropoff_datetime",
                         "request_datetime", "on_scene_datetime"],
        )
    except Exception:
        return pd.read_sql(
            f"""
            SELECT {cols}
            FROM fact_trips
            WHERE DATE_TRUNC('month', pickup_datetime) = '{period}-01'
            LIMIT {FALLBACK_ROW_LIMIT}
            """,
            engine,
            parse_dates=["pickup_datetime", "dropoff_datetime"],
        )


def load_weather(period: str) -> pd.DataFrame:
    cols = ", ".join(WEATHER_COLUMNS)
    try:
        return pd.read_sql(
            f"""
            SELECT {cols}
            FROM dim_weather
            WHERE DATE_TRUNC('month', date) = '{period}-01'
            """,
            engine,
            parse_dates=["date"],
        )
    except Exception:
        return pd.DataFrame()


# email section builders

def _build_fact_section(fact_df: pd.DataFrame) -> str:
    """Stats table for fact_trips — charts are sent as attachments."""
    stats_html = _stats_html(fact_df, FACT_NUMERIC)

    return f"""
      <h3>Fact trips — descriptive statistics</h3>
      {stats_html}
      <h3>Categorical distributions</h3>
      <p style="color:#666;font-size:12px">See attached: categorical_*.png</p>
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
        Sampled <b>{n_fact:,}</b> fact_trips rows &nbsp;|&nbsp;
        <b>{n_weather:,}</b> dim_weather rows
      </p>

      {_build_flag_block(flags)}

      <hr style="border:none;border-top:1px solid #e0e0e0;margin:16px 0"/>

      {_build_fact_section(fact_df)}
      {_build_null_section(fact_df)}
      {_build_weather_section(weather_df)}

      <p style="color:#999;font-size:11px;margin-top:24px">
        Generated by Airflow DAG · {period} · charts based on a {TABLESAMPLE_PCT}% sample of fact_trips
      </p>
    </div>
    """


# email sending

def _send_email_with_charts(subject: str, html_body: str, images: dict):
    """Send email with chart images as PNG attachments.
    images: {filename: png_bytes}
    """
    from airflow.models import Variable

    from_addr = "bt4301groupeight@gmail.com"
    password  = Variable.get("smtp_password")
    to_list   = ALERT_EMAILS if isinstance(ALERT_EMAILS, list) else [ALERT_EMAILS]

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"]    = from_addr
    msg["To"]      = ", ".join(to_list)
    msg.attach(MIMEText(html_body, "html"))

    for filename, png_bytes in images.items():
        img = MIMEImage(png_bytes, name=filename)
        img.add_header("Content-Disposition", "attachment", filename=filename)
        msg.attach(img)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(from_addr, password)
        server.sendmail(from_addr, to_list, msg.as_string())


# main task

@monitor
def report_data(**context):
    execution_date = context["execution_date"]
    year  = execution_date.year
    month = execution_date.month
    period = f"{year}-{month:02d}"

    logger.info(f"Generating data quality report for {period}")

    fact_df    = load_fact_sample(period)
    weather_df = load_weather(period)
    logger.info(f"Sampled {len(fact_df):,} fact_trips rows, {len(weather_df):,} weather rows")

    flags   = _anomaly_flags(fact_df)
    charts  = _collect_charts(fact_df, weather_df)
    body    = _build_email_body(period, len(fact_df), len(weather_df), flags, fact_df, weather_df)
    subject = f"[DataOps] Quality Report — {period}" + (" ⚠ ANOMALIES DETECTED" if flags else " ✔ Clean")

    _send_email_with_charts(subject, body, charts)
    logger.info(f"Report email sent for {period} ({len(flags)} flags)")