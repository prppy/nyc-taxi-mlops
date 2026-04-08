import os
import pandas as pd

BASE_PATH = "data/processed"
FE_PATH = os.path.join(BASE_PATH, "feature_engineered")

INPUT_PICKUP_PATH = os.path.join(FE_PATH, "pickup_features.parquet")


def load_feature_data(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise ValueError(f"Path does not exist: {path}")

    df = pd.read_parquet(path)
    print(f"Loaded: {path}")
    print(f"Shape : {df.shape[0]:,} rows x {df.shape[1]:,} cols")
    return df


def prepare_datetime(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "hour_ts" not in df.columns:
        raise ValueError("Column 'hour_ts' not found in dataframe")

    df["hour_ts"] = pd.to_datetime(df["hour_ts"], errors="coerce")
    df = df.dropna(subset=["hour_ts"]).copy()

    if df.empty:
        raise ValueError("Dataframe is empty after dropping invalid hour_ts values")

    df = df.sort_values("hour_ts").reset_index(drop=True)
    df["year_month"] = df["hour_ts"].dt.to_period("M")

    return df


def rolling_split(
    df: pd.DataFrame,
    test_month: str,
    val_months: int = 1
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Rolling time-based split.

    Parameters
    ----------
    df : pd.DataFrame
        Full feature-engineered dataset.
    test_month : str
        Month to test on, in 'YYYY-MM' format, eg '2025-01'.
    val_months : int
        Number of full months immediately before test month to use for validation.

    Returns
    -------
    train_df, val_df, test_df
    """
    df = prepare_datetime(df)

    test_period = pd.Period(test_month, freq="M")
    val_start_period = test_period - val_months

    test_mask = df["year_month"] == test_period
    val_mask = (df["year_month"] >= val_start_period) & (df["year_month"] < test_period)
    train_mask = df["year_month"] < val_start_period

    train_df = df.loc[train_mask].copy()
    val_df = df.loc[val_mask].copy()
    test_df = df.loc[test_mask].copy()

    if train_df.empty:
        raise ValueError(f"Train set is empty for test_month={test_month}")

    if val_df.empty:
        raise ValueError(
            f"Validation set is empty for test_month={test_month}. "
            f"Try reducing val_months or using a later test month."
        )

    if test_df.empty:
        raise ValueError(
            f"Test set is empty for test_month={test_month}. "
            f"No rows found for that month."
        )

    return train_df, val_df, test_df


def print_split_summary(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    test_df: pd.DataFrame,
    label: str,
    test_month: str,
) -> None:
    print(f"\n=== ROLLING SPLIT SUMMARY ({label}) ===")
    print(f"Test month: {test_month}")

    def _summary(name: str, split_df: pd.DataFrame) -> None:
        min_ts = split_df["hour_ts"].min()
        max_ts = split_df["hour_ts"].max()

        print(f"\n{name}")
        print(f"Rows      : {split_df.shape[0]:,}")
        print(f"Time range: {min_ts} -> {max_ts}")

        if "target_demand" in split_df.columns:
            print(f"Mean target_demand: {split_df['target_demand'].mean():.3f}")

    total_rows = train_df.shape[0] + val_df.shape[0] + test_df.shape[0]
    print(f"Total rows used: {total_rows:,}")

    _summary("TRAIN", train_df)
    _summary("VAL", val_df)
    _summary("TEST", test_df)

    print("\nRow share:")
    print(f"Train: {train_df.shape[0] / total_rows:.1%}")
    print(f"Val  : {val_df.shape[0] / total_rows:.1%}")
    print(f"Test : {test_df.shape[0] / total_rows:.1%}")


def main():
    TEST_MONTH = "2025-01" # to update based on testing month
    VAL_MONTHS = 1 # to update based on number of months to use for validation (immediately before test month)

    print("\n" + "=" * 60)
    print("ROLLING SPLIT")
    print("=" * 60)

    pickup_df = load_feature_data(INPUT_PICKUP_PATH)
    pickup_train, pickup_val, pickup_test = rolling_split(
        pickup_df,
        test_month=TEST_MONTH,
        val_months=VAL_MONTHS,
    )
    print_split_summary(
        pickup_train,
        pickup_val,
        pickup_test,
        label="PICKUP",
        test_month=TEST_MONTH,
    )

    print("\nRolling split completed.")


if __name__ == "__main__":
    main()