import pandas as pd


def prepare_datetime(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["hour_ts"] = pd.to_datetime(df["hour_ts"], errors="coerce")
    df = df.dropna(subset=["hour_ts"])

    df = df.sort_values("hour_ts").reset_index(drop=True)
    df["year_month"] = df["hour_ts"].dt.to_period("M")

    return df


def rolling_split(df, test_month, val_months=1):

    df = prepare_datetime(df)

    test_period = pd.Period(test_month, freq="M")
    val_start_period = test_period - val_months

    train_df = df[df["year_month"] < val_start_period]
    val_df = df[
        (df["year_month"] >= val_start_period)
        & (df["year_month"] < test_period)
    ]
    test_df = df[df["year_month"] == test_period]

    if train_df.empty:
        raise ValueError("Train set is empty")

    if val_df.empty:
        raise ValueError("Validation set is empty")

    if test_df.empty:
        raise ValueError("Test set is empty")

    return train_df, val_df, test_df