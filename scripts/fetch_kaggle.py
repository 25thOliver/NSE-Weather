import logging
from datetime import datetime
import pandas as pd
import os

logging.basicConfig(level=logging.INFO)

def clean_volume(x):
    if isinstance(x, str):
        x = x.strip().replace(",", "")
        if x.endswith("K"):
            return float(x[:-1]) * 1_000
        elif x.endswith("M"):
            return float(x[:-1]) * 1_000_000
        elif x.endswith("B"):
            return float(x[:-1]) * 1_000_000_000
        elif x in ["-", ""]:
            return 0.0
        else:
            return float(x)
    return float(x) if pd.notna(x) else 0.0

# Point to the Dataset in the project folder
project_folder = os.path.dirname(os.path.abspath(__file__))
local_csv = os.path.join(project_folder, "south_africa_top40.csv")

def fetch_kaggle():
    # Config
    bucket = "nse-weather"
    s3_prefix = "raw/equities"

    # MinIO creds
    storage_options = {
        "key": "oliveradmin",
        "secret": "oliver#0L",
        "client_kwargs": {"endpoint_url": "http://172.17.0.1:9000"},
    }

    # Read CSV
    df = pd.read_csv(local_csv)

    # Clean Columns
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True)

    for col in ["Price", "Open", "High", "Low"]:
        df[col] = df[col].str.replace(",", "", regex=False).astype(float)

    df["Change %"] = df["Change %"].str.replace("%", "", regex=False).astype(float)

    df["Vol."] = df["Vol."].fillna("0").apply(clean_volume)

    # Saving the latest date to mimic daily fetch
    latest_date = df["Date"].max()
    latest_data = df[df["Date"] == latest_date]

    filename = f"{latest_date.strftime('%Y-%m-%d')}.csv"
    s3_path = f"s3://{bucket}/{s3_prefix}/{filename}"

    # Upload to MinIO
    latest_data.to_csv(s3_path, index=False, storage_options=storage_options)

    logging.info(f"Uploaded Kaggle equities data to {s3_path}")
    return s3_path

if __name__ == "__main__":
    fetch_kaggle()
