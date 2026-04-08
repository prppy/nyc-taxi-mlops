import os
import re
import pandas as pd
import matplotlib.pyplot as plt

plt.style.use('seaborn-v0_8-darkgrid')
BLUE_COLOR = '#1f77b4'

BASE_PATH = "data/processed"
ZONE_PATH = os.path.join(BASE_PATH, "dim_zone.csv")
WEATHER_PATH = os.path.join(BASE_PATH, "dim_weather")
FACT_PICKUP_PATH = os.path.join(BASE_PATH, "fact_trips_pickup")
# FACT_PAIR_PATH = os.path.join(BASE_PATH, "fact_trips_pair")

EDA_PATH = os.path.join(BASE_PATH, "eda")
JOINED_PATH = os.path.join(EDA_PATH, "joined")
PLOTS_PATH = os.path.join(EDA_PATH, "plots")

OUTPUT_PICKUP_PATH = os.path.join(JOINED_PATH, "pickup.parquet")
OUTPUT_PAIR_PATH = os.path.join(JOINED_PATH, "pair.parquet")

os.makedirs(JOINED_PATH, exist_ok=True)
os.makedirs(PLOTS_PATH, exist_ok=True)


def load_all_monthly_parquet(base_path: str, months: list = None):
    """Load monthly parquet folders and tag each row with data_month."""
    if not os.path.exists(base_path):
        raise ValueError(f"Path does not exist: {base_path}")

    month_folders = sorted([
        f for f in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, f))
        and re.match(r"^\d{4}-\d{2}$", f)
    ])

    if months:
        month_folders = [m for m in month_folders if m in months]
        if not month_folders:
            raise ValueError(f"None of the requested months {months} found in {base_path}")

    dfs = []
    for month in month_folders:
        path = os.path.join(base_path, month)
        try:
            df = pd.read_parquet(path)
            df["data_month"] = month
            count = len(df)
            dfs.append(df)
            print(f"  Loaded: {month} — {count:,} rows")
        except Exception as e:
            print(f"  Failed to load {path}: {e}")

    if not dfs:
        raise ValueError(f"No parquet files loaded from {base_path}")

    result = pd.concat(dfs, ignore_index=True)

    return result


def load_zone():
    if not os.path.exists(ZONE_PATH):
        raise ValueError(f"Path does not exist: {ZONE_PATH}")

    df = pd.read_csv(ZONE_PATH).drop_duplicates(subset=["location_id"])
    print(f"  Loaded dim_zone: {len(df):,} rows")
    return df


def load_weather(months=None):
    df = load_all_monthly_parquet(WEATHER_PATH, months=months)
    df["date"] = pd.to_datetime(df["date"])
    df = df.drop(columns=["data_month"])
    return df


def join_dimensions(fact_df, zone_df, weather_df):
    # Prepare zone lookup
    zone_cols = zone_df[["location_id", "borough", "zone", "service_zone"]].copy()
    
    # Prepare weather data
    weather_df = weather_df.rename(columns={"date": "weather_date"}).copy()
    weather_df["weather_date"] = pd.to_datetime(weather_df["weather_date"], errors="coerce").dt.date
    
    # Join with zone
    fact_zone = fact_df.merge(
        zone_cols,
        left_on="pulocationid",
        right_on="location_id",
        how="left"
    )
    fact_zone = fact_zone.drop(columns=["location_id"])
    fact_zone["pickup_date"] = pd.to_datetime(fact_zone["hour_ts"]).dt.date
    
    # Join with weather
    joined = fact_zone.merge(
        weather_df,
        left_on=["pickup_date", "borough"],
        right_on=["weather_date", "borough"],
        how="left"
    )
    joined = joined.drop(columns=["weather_date"])
    
    total = len(joined)
    loc_pct = (joined["borough"].notna().sum() / total * 100) if total else 0
    weather_pct = (joined["temperature_mean"].notna().sum() / total * 100) if total else 0
    
    print(f"Joined shape : {total:,} rows")
    print(f"Location join: {loc_pct:.1f}%")
    print(f"Weather join : {weather_pct:.1f}%")
    
    return joined


def eda_features(df):
    df = df.copy()
    df["hour_ts"] = pd.to_datetime(df["hour_ts"])
    df["hour"] = df["hour_ts"].dt.hour
    df["day_of_week"] = (df["hour_ts"].dt.dayofweek + 1) % 7
    df["month"] = df["hour_ts"].dt.month
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    return df


def print_summary(pdf, label):
    print(f"\n=== SUMMARY ({label}) ===")
    print(f"Columns: {list(pdf.columns)}")
    
    numeric_cols = [
        "demand",
        "avg_trip_distance",
        "avg_total_amount",
        "temperature_mean",
        "precipitation_sum",
    ]
    existing = [c for c in numeric_cols if c in pdf.columns]
    
    if existing:
        print(pdf[existing].describe())
    else:
        print("No numeric columns found for summary.")


def add_bar_labels(ax, orientation="vertical", fmt="{:.1f}"):
    """Add labels on bars."""
    for patch in ax.patches:
        if orientation == "vertical":
            height = patch.get_height()
            if pd.notnull(height):
                ax.annotate(
                    fmt.format(height),
                    (patch.get_x() + patch.get_width() / 2, height),
                    ha="center",
                    va="bottom",
                    xytext=(0, 4),
                    textcoords="offset points",
                    fontsize=9
                )
        else:
            width = patch.get_width()
            if pd.notnull(width):
                ax.annotate(
                    fmt.format(width),
                    (width, patch.get_y() + patch.get_height() / 2),
                    ha="left",
                    va="center",
                    xytext=(4, 0),
                    textcoords="offset points",
                    fontsize=9
                )


def plot_demand_by_hour(pdf, label, save_path):
    hourly = (
        pdf.groupby("hour", dropna=False)["demand"]
        .mean()
        .reindex(range(24), fill_value=0)
    )
    
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(hourly.index, hourly.values, color=BLUE_COLOR, edgecolor="black", alpha=0.7)
    ax.set_title(f"Avg Demand by Hour ({label})", fontsize=14, fontweight="bold")
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Avg Demand")
    ax.set_xticks(range(24))
    ax.grid(axis="y", alpha=0.3)
    
    add_bar_labels(ax, orientation="vertical", fmt="{:.1f}")
    
    plt.tight_layout()
    plt.savefig(os.path.join(save_path, f"demand_by_hour_{label}.png"), dpi=100)
    plt.close()
    print(f"Saved: demand_by_hour_{label}.png")


def plot_demand_by_day(pdf, label, save_path):
    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    daily = (
        pdf.groupby("day_of_week", dropna=False)["demand"]
        .mean()
        .reindex(range(7), fill_value=0)
    )
    
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(day_names, daily.values, color=BLUE_COLOR, edgecolor="black", alpha=0.7)
    ax.set_title(f"Avg Demand by Day of Week ({label})", fontsize=14, fontweight="bold")
    ax.set_xlabel("Day of Week")
    ax.set_ylabel("Avg Demand")
    ax.grid(axis="y", alpha=0.3)
    
    add_bar_labels(ax, orientation="vertical", fmt="{:.1f}")
    
    plt.tight_layout()
    plt.savefig(os.path.join(save_path, f"demand_by_day_{label}.png"), dpi=100)
    plt.close()
    print(f"Saved: demand_by_day_{label}.png")


def plot_demand_by_borough(pdf, label, save_path):
    if "borough" not in pdf.columns:
        return
    
    borough_df = (
        pdf.groupby("borough", dropna=False)["demand"]
        .mean()
        .sort_values()
    )
    
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.barh(
        borough_df.index.astype(str),
        borough_df.values,
        color=BLUE_COLOR,
        edgecolor="black",
        alpha=0.7
    )
    ax.set_title(f"Avg Demand by Borough ({label})", fontsize=14, fontweight="bold")
    ax.set_xlabel("Avg Demand")
    ax.grid(axis="x", alpha=0.3)
    
    add_bar_labels(ax, orientation="horizontal", fmt="{:.1f}")
    
    plt.tight_layout()
    plt.savefig(os.path.join(save_path, f"demand_by_borough_{label}.png"), dpi=100)
    plt.close()
    print(f"Saved: demand_by_borough_{label}.png")


def plot_weather_impact(pdf, label, save_path):
    if "precipitation_sum" not in pdf.columns:
        return
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    pdf = pdf.copy()
    pdf["is_rainy"] = (pdf["precipitation_sum"] > 0).astype(int)
    rain_mean = pdf.groupby("is_rainy")["demand"].mean().reindex([0, 1], fill_value=0)
    
    axes[0].bar(
        ["No Rain", "Rain"],
        rain_mean.values,
        color=BLUE_COLOR,
        edgecolor="black",
        alpha=0.7
    )
    axes[0].set_title(f"Avg Demand: Rain vs No Rain ({label})", fontweight="bold")
    axes[0].set_ylabel("Avg Demand")
    axes[0].grid(axis="y", alpha=0.3)
    add_bar_labels(axes[0], orientation="vertical", fmt="{:.1f}")
    
    if "temperature_mean" in pdf.columns:
        temp_df = pdf.dropna(subset=["temperature_mean", "demand"]).copy()
        
        if not temp_df.empty and temp_df["temperature_mean"].min() < temp_df["temperature_mean"].max():
            temp_df["temp_bin"] = pd.cut(temp_df["temperature_mean"], bins=8)
            temp_mean = temp_df.groupby("temp_bin", observed=False)["demand"].mean()
            
            axes[1].bar(
                range(len(temp_mean)),
                temp_mean.values,
                color=BLUE_COLOR,
                edgecolor="black",
                alpha=0.7
            )
            axes[1].set_xticks(range(len(temp_mean)))
            axes[1].set_xticklabels([str(x) for x in temp_mean.index], rotation=45)
            axes[1].set_title(f"Avg Demand by Temperature ({label})", fontweight="bold")
            axes[1].set_ylabel("Avg Demand")
            axes[1].grid(axis="y", alpha=0.3)
            add_bar_labels(axes[1], orientation="vertical", fmt="{:.1f}")
        else:
            axes[1].text(0.5, 0.5, "Not enough temperature variation", ha="center", va="center")
            axes[1].set_axis_off()
    
    plt.tight_layout()
    plt.savefig(os.path.join(save_path, f"weather_impact_{label}.png"), dpi=100)
    plt.close()
    print(f"Saved: weather_impact_{label}.png")


def plot_demand_heatmap(df, save_path, label):
    """Generate heatmap for hourly demand by day of week."""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    if 'hour' in df.columns and 'day_of_week' in df.columns and 'demand' in df.columns:
        # Create pivot table for heatmap
        hour_day_pivot = df.groupby(['day_of_week', 'hour'])['demand'].mean().unstack(fill_value=0)
        day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        
        im = ax.imshow(hour_day_pivot.values, aspect='auto', cmap='YlOrRd')
        ax.set_xlabel('Hour of Day', fontsize=12)
        ax.set_ylabel('Day of Week', fontsize=12)
        ax.set_title(f'Average Demand Heatmap: Hour vs Day of Week ({label})', 
                    fontsize=14, fontweight='bold')
        ax.set_xticks(range(0, 24, 3))
        ax.set_xticklabels(range(0, 24, 3))
        ax.set_yticks(range(len(day_names)))
        ax.set_yticklabels(day_names)
        plt.colorbar(im, ax=ax, label='Average Demand')
    
    plt.tight_layout()
    plt.savefig(os.path.join(save_path, f'demand_heatmap_{label}.png'), dpi=100, bbox_inches='tight')
    plt.close()
    print(f"Saved: demand_heatmap_{label}.png")
    

def generate_all_plots(pdf, label, save_path):
    print(f"\n=== GENERATING PLOTS ({label}) ===")
    plot_demand_by_hour(pdf, label, save_path)
    plot_demand_by_day(pdf, label, save_path)
    plot_demand_by_borough(pdf, label, save_path)
    plot_weather_impact(pdf, label, save_path)
    plot_demand_heatmap(pdf, save_path, label)


def main():
    months = None  # eg ["2025-03", "2025-04"] to load selected months only; set to None to load all months

    print("\nLoading dim_zone...")
    zone_df = load_zone()
    
    print("\nLoading dim_weather...")
    weather_df = load_weather(months=months)
    
    print("\n" + "=" * 60)
    print("VERSION 1: PICKUP ZONE")
    print("=" * 60)
    
    print("\nLoading fact_trips_pickup...")
    pickup_df = load_all_monthly_parquet(FACT_PICKUP_PATH, months=months)
    pickup_df = pickup_df.drop(columns=["data_month"])
    
    print("\nJoining dimensions...")
    pickup_joined = join_dimensions(pickup_df, zone_df, weather_df)
    pickup_joined = eda_features(pickup_joined)
    
    print_summary(pickup_joined, "PICKUP")
    generate_all_plots(pickup_joined, "pickup", PLOTS_PATH)
    
    pickup_joined.to_parquet(OUTPUT_PICKUP_PATH, index=False)
    print(f"\nSaved: {OUTPUT_PICKUP_PATH}")
    
    '''
    print("\n" + "=" * 60)
    print("VERSION 2: PICKUP-DROPOFF PAIR")
    print("=" * 60)
    
    print("\nLoading fact_trips_pair...")
    pair_df = load_all_monthly_parquet(FACT_PAIR_PATH, months=months)
    pair_df = pair_df.drop(columns=["data_month"])
    
    print("\nJoining dimensions...")
    pair_joined = join_dimensions(pair_df, zone_df, weather_df)
    pair_joined = eda_features(pair_joined)
    
    print_summary(pair_joined, "PICKUP-DROPOFF PAIR")
    generate_all_plots(pair_joined, "pair", PLOTS_PATH)
    
    pair_joined.to_parquet(OUTPUT_PAIR_PATH, index=False)
    print(f"\nSaved: {OUTPUT_PAIR_PATH}")
    '''
    
    print("\nEDA completed.")


if __name__ == "__main__":
    main()