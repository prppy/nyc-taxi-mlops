import os
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

plt.style.use('seaborn-v0_8-darkgrid')
BLUE_COLOR = '#1f77b4' 
LIGHT_BLUE = '#6baed6'
DARK_BLUE = '#08519c'

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)

BASE_PATH = "data/processed"
FACT_PATH = os.path.join(BASE_PATH, "fact_trips")
ZONE_PATH = os.path.join(BASE_PATH, "dim_zone")
WEATHER_PATH = os.path.join(BASE_PATH, "dim_weather")

# output directory for plots
PLOTS_PATH = os.path.join(BASE_PATH, "eda_plots")
os.makedirs(PLOTS_PATH, exist_ok=True)


def load_all_monthly_parquet(base_path: str):
    dfs = []

    if not os.path.exists(base_path):
        raise ValueError(f"Path does not exist: {base_path}")

    month_folders = sorted(
        [
            folder for folder in os.listdir(base_path)
            if os.path.isdir(os.path.join(base_path, folder))
            and re.match(r"^\d{4}-\d{2}$", folder)
        ]
    )

    if not month_folders:
        raise ValueError(f"No month folders found in {base_path}")

    for month in month_folders:
        path = os.path.join(base_path, month)
        try:
            df = pd.read_parquet(path)
            df["data_month"] = month
            dfs.append(df)
            print(f"Loaded: {path}")
        except Exception as e:
            print(f"Failed to load {path}: {e}")

    if not dfs:
        raise ValueError(f"No parquet files could be loaded from {base_path}")

    return pd.concat(dfs, ignore_index=True)

def create_joined_dataset(fact_path: str, zone_path: str, weather_path: str):
    print("Loading fact_trips...")
    fact_df = load_all_monthly_parquet(fact_path)

    print("Loading dim_zone...")
    zone_df = load_all_monthly_parquet(zone_path)
    zone_df = zone_df.drop_duplicates(subset=["location_id"])
    zone_df = zone_df.drop('data_month', axis=1)

    print("Loading dim_weather...")
    weather_df = load_all_monthly_parquet(weather_path)
    weather_df["date"] = pd.to_datetime(weather_df["date"], errors="coerce").dt.date
    weather_df = weather_df.drop('data_month', axis=1)

    print("\n=== DATA PREVIEW ===")
    print("\nFact Trips Shape:", fact_df.shape)
    print(f"Fact Trips Columns ({len(fact_df.columns)}): {fact_df.columns.tolist()}")
    print("Null values in Fact Trips:\n", fact_df.isnull().sum())
    print("\nDim Zone Shape:", zone_df.shape)
    print(f"Dim Zone Columns ({len(zone_df.columns)}): {zone_df.columns.tolist()}")
    print("Null values in Dim Zone:\n", zone_df.isnull().sum())
    print("\nDim Weather Shape:", weather_df.shape)
    print(f"Dim Weather Columns ({len(weather_df.columns)}): {weather_df.columns.tolist()}")
    print("Null values in Dim Weather:\n", weather_df.isnull().sum())

    # convert datetime columns
    fact_df["pickup_datetime"] = pd.to_datetime(fact_df["pickup_datetime"], errors="coerce")
    fact_df["dropoff_datetime"] = pd.to_datetime(fact_df["dropoff_datetime"], errors="coerce")

    # join tables
    print("\n=== JOINING TABLES ===")
    joined_df = fact_df.merge(
        zone_df,
        left_on="pulocationid",
        right_on="location_id",
        how="left"
    )

    joined_df["pickup_date"] = joined_df["pickup_datetime"].dt.date
    joined_df = joined_df.merge(
        weather_df,
        left_on=["pickup_date", "borough"],
        right_on=["date", "borough"],
        how="left"
    )

    print(f"Joined data shape: {joined_df.shape}")
    
    # check join quality
    print(f"\nJoin coverage:")
    print(f"  Location join success: {(joined_df['borough'].notna().sum() / len(joined_df) * 100):.1f}%")
    print(f"  Weather join success: {(joined_df['temperature_mean'].notna().sum() / len(joined_df) * 100):.1f}%")
    
    return joined_df

'''
def eda_features(df):
    df = df.copy()
    df["hour"] = df["pickup_datetime"].dt.hour
    df["day_of_week"] = df["pickup_datetime"].dt.dayofweek
    df["month"] = df["pickup_datetime"].dt.month
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    return df

def plot_trips_by_hour(df, save_path):
    plt.figure(figsize=(12, 6))
    hourly_trips = df.groupby("hour").size()
    
    plt.bar(hourly_trips.index, hourly_trips.values, alpha=0.7, edgecolor='black', color=BLUE_COLOR)
    plt.xlabel('Hour of Day', fontsize=12)
    plt.ylabel('Number of Trips', fontsize=12)
    plt.title('Distribution of Trips by Hour', fontsize=14, fontweight='bold')
    plt.xticks(range(0, 24))
    plt.grid(axis='y', alpha=0.3)
    
    # add value labels on top of bars
    for i, v in enumerate(hourly_trips.values):
        plt.text(i, v + (max(hourly_trips.values) * 0.01), str(v), 
                ha='center', fontsize=8)
    
    plt.tight_layout()
    plt.savefig(os.path.join(save_path, 'trips_by_hour.png'), dpi=100, bbox_inches='tight')
    plt.close()


def plot_trips_by_day(df, save_path):
    plt.figure(figsize=(10, 6))
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    daily_trips = df.groupby("day_of_week").size()
    
    plt.bar(day_names, daily_trips.values, alpha=0.7, edgecolor='black', color=BLUE_COLOR)
    plt.xlabel('Day of Week', fontsize=12)
    plt.ylabel('Number of Trips', fontsize=12)
    plt.title('Distribution of Trips by Day of Week', fontsize=14, fontweight='bold')
    plt.xticks(rotation=45)
    plt.grid(axis='y', alpha=0.3)
    
    # add value labels
    for i, v in enumerate(daily_trips.values):
        plt.text(i, v + (max(daily_trips.values) * 0.01), f'{v:,}', 
                ha='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(os.path.join(save_path, 'trips_by_day.png'), dpi=100, bbox_inches='tight')
    plt.close()


def plot_top_zones(df, save_path):
    plt.figure(figsize=(12, 6))
    top_zones = df.groupby("pulocationid").size().sort_values(ascending=False).head(10)
    
    if 'zone' in df.columns:
        zone_names = df.groupby("pulocationid")['zone'].first().loc[top_zones.index]
        labels = [f"{zone_names[loc]} (ID: {loc})" for loc in top_zones.index]
    else:
        labels = [f"Zone ID: {loc}" for loc in top_zones.index]
    
    plt.barh(range(len(top_zones)), top_zones.values, alpha=0.7, edgecolor='black', color=BLUE_COLOR)
    plt.yticks(range(len(top_zones)), labels)
    plt.xlabel('Number of Trips', fontsize=12)
    plt.ylabel('Pickup Zone', fontsize=12)
    plt.title('Top 10 Pickup Zones', fontsize=14, fontweight='bold')
    plt.gca().invert_yaxis() 
    plt.grid(axis='x', alpha=0.3)
    
    # add value labels
    for i, v in enumerate(top_zones.values):
        plt.text(v + (max(top_zones.values) * 0.01), i, f'{v:,}', 
                va='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(os.path.join(save_path, 'top_pickup_zones.png'), dpi=100, bbox_inches='tight')
    plt.close()

def plot_weather_impact(df, save_path):
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # temperature impact
    if 'temperature_mean' in df.columns and df['temperature_mean'].notna().any():
        # bin temperatures
        df['temp_bin'] = pd.cut(df['temperature_mean'], bins=10)
        trips_by_temp = df.groupby('temp_bin', observed=True).size()
        
        axes[0].bar(range(len(trips_by_temp)), trips_by_temp.values, alpha=0.7, 
                   edgecolor='black', color=BLUE_COLOR)
        axes[0].set_xlabel('Temperature Range (°C)', fontsize=12)
        axes[0].set_ylabel('Number of Trips', fontsize=12)
        axes[0].set_title('Trip Volume by Temperature', fontsize=12, fontweight='bold')
        axes[0].set_xticks(range(len(trips_by_temp)))
        axes[0].set_xticklabels([f'{int(interval.left)}-{int(interval.right)}' 
                                 for interval in trips_by_temp.index], rotation=45)
        axes[0].grid(axis='y', alpha=0.3)
    
    # precipitation impact
    if 'precipitation_sum' in df.columns and df['precipitation_sum'].notna().any():
        df['is_rainy'] = (df['precipitation_sum'] > 0).astype(int)
        rainy_counts = df.groupby('is_rainy').size()
        labels = ['No Rain', 'Rain']
        
        axes[1].bar(labels, rainy_counts.values, alpha=0.7, edgecolor='black', color=BLUE_COLOR)
        axes[1].set_xlabel('Weather Condition', fontsize=12)
        axes[1].set_ylabel('Number of Trips', fontsize=12)
        axes[1].set_title('Trip Volume by Rain Condition', fontsize=12, fontweight='bold')
        axes[1].grid(axis='y', alpha=0.3)
        
        # add value labels
        for i, v in enumerate(rainy_counts.values):
            axes[1].text(i, v + (max(rainy_counts.values) * 0.01), f'{v:,}', 
                        ha='center', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(os.path.join(save_path, 'weather_impact.png'), dpi=100, bbox_inches='tight')
    plt.close()
    
    # clean up
    if 'temp_bin' in df.columns:
        df.drop('temp_bin', axis=1, inplace=True)
    if 'is_rainy' in df.columns:
        df.drop('is_rainy', axis=1, inplace=True)


def plot_borough_distribution(df, save_path):
    if 'borough' in df.columns and df['borough'].notna().any():
        plt.figure(figsize=(10, 6))
        borough_trips = df['borough'].value_counts()
        
        plt.bar(borough_trips.index, borough_trips.values, alpha=0.7, 
                edgecolor='black', color=BLUE_COLOR)
        plt.xlabel('Borough', fontsize=12)
        plt.ylabel('Number of Trips', fontsize=12)
        plt.title('Trip Distribution by Borough', fontsize=14, fontweight='bold')
        plt.xticks(rotation=45)
        plt.grid(axis='y', alpha=0.3)
        
        # add value labels on top of bars
        for i, v in enumerate(borough_trips.values):
            plt.text(i, v + (max(borough_trips.values) * 0.01), f'{v:,}', 
                    ha='center', fontsize=10)
        
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, 'borough_distribution.png'), dpi=100, bbox_inches='tight')
        plt.close()


def plot_time_series_trends(df, save_path):
    fig, axes = plt.subplots(2, 1, figsize=(14, 10))
    
    # daily trends
    if 'pickup_date' in df.columns:
        daily_trips = df.groupby('pickup_date').size()
        axes[0].plot(daily_trips.index, daily_trips.values, alpha=0.7, 
                    linewidth=1, color=BLUE_COLOR)
        axes[0].set_xlabel('Date', fontsize=12)
        axes[0].set_ylabel('Number of Trips', fontsize=12)
        axes[0].set_title('Daily Trip Volume Over Time', fontsize=12, fontweight='bold')
        axes[0].grid(alpha=0.3)
        axes[0].tick_params(axis='x', rotation=45)
    
    # hourly heatmap by day of week
    if 'hour' in df.columns and 'day_of_week' in df.columns:
        hour_day_pivot = df.groupby(['day_of_week', 'hour']).size().unstack(fill_value=0)
        day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        im = axes[1].imshow(hour_day_pivot.values, aspect='auto', cmap='Blues')
        axes[1].set_xlabel('Hour of Day', fontsize=12)
        axes[1].set_ylabel('Day of Week', fontsize=12)
        axes[1].set_title('Trip Volume Heatmap: Hour vs Day of Week', fontsize=12, fontweight='bold')
        axes[1].set_xticks(range(0, 24, 2))
        axes[1].set_yticks(range(len(day_names)))
        axes[1].set_yticklabels(day_names)
        plt.colorbar(im, ax=axes[1], label='Number of Trips')
    
    plt.tight_layout()
    plt.savefig(os.path.join(save_path, 'time_series_trends.png'), dpi=100, bbox_inches='tight')
    plt.close()

def generate_all_plots(df, plots_path: str):
    print("\n=== GENERATING PLOTS ===")
    print(f"Saving plots to: {plots_path}")
    plot_trips_by_hour(df, plots_path)
    plot_trips_by_day(df, plots_path)
    plot_top_zones(df, plots_path)
    plot_weather_impact(df, plots_path)
    plot_borough_distribution(df, plots_path)
    plot_time_series_trends(df, plots_path)

def print_summary_statistics(df):
    print("\n=== NUMERIC SUMMARY ===")
    numeric_cols = ["trip_distance", "fare_amount", "total_amount", 
                    "temperature_mean", "precipitation_sum", "trip_duration_min", "speed_mph"]
    existing_numeric = [c for c in numeric_cols if c in df.columns]
    print(df[existing_numeric].describe())
'''

def main():
    # 1. load and join data
    joined_df = create_joined_dataset(FACT_PATH, ZONE_PATH, WEATHER_PATH)
    print(f"\nTotal number of rows: {len(joined_df):,} rows")
    
    # 2. Engineer features
    #joined_df = eda_features(joined_df)
    #print(f"\nTotal number of rows after adding EDA features: {len(joined_df):,} rows")
    
    # 2. generate EDA plots
    #generate_all_plots(joined_df, PLOTS_PATH)
    
    # 3. print summary statistics
    #print_summary_statistics(joined_df)

    # 4. save final dataset
    output_path = os.path.join(BASE_PATH, "eda_joined.parquet")
    joined_df.to_parquet(output_path, index=False)
    print(f"\nSaved joined EDA dataset to: {output_path}")
    #print(f"Plots saved to: {PLOTS_PATH}")

if __name__ == "__main__":
    main()