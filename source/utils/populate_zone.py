import pandas as pd
from utils.db import engine

print("Loading CSV...")
try:
    # Make sure this path points to where your CSV is saved
    df = pd.read_csv("taxi_zone_lookup.csv")
except FileNotFoundError:
    raise Exception("Could not find taxi_zone_lookup.csv! Check the file path.")

# 2. Clean the column names to match what PySpark expects
# The raw CSV has 'LocationID', but your code expects 'location_id'
df.columns = [col.lower() for col in df.columns]
df = df.rename(columns={"locationid": "location_id"})

print(f"Found {len(df)} zones. Pushing to database...")

# 3. Push to the dim_zone table
df.to_sql("dim_zone", engine, if_exists="replace", index=False)

print("✅ Successfully populated the dim_zone table!")