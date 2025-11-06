import pandas as pd
import numpy as np
import json
from extract import extract_data

def clean_data():

    # Read raw data from file
    with open("raw_data.json") as f:
        data = json.load(f)

    # Handle the case where data is a dictionary
    if isinstance(data, dict):
        # Ensure all values are lists; wrap single values in a list
        fixed_data1 = {k: v if isinstance(v, list) else [v] for k, v in data.items()}
        # Find the maximum length among all lists
        max_col_len = max(len(v) for v in fixed_data1.values())
        # Pad shorter lists with None to match the maximum length
        fixed_data2 = {k: v + ([None] * (max_col_len - len(v))) for k, v in fixed_data1.items()}
        # Create DataFrame from the fixed dictionary
        df = pd.DataFrame(fixed_data2)

    # Handle the case where data is a list
    elif isinstance(data, list):
        df = pd.DataFrame(data)

    # Handle invalid data type
    else:
        print("Invalid data type")

    # Add global columns to the DataFrame (same value for all rows)
    for col in ["lat", "lon", "timezone", "timezone_offset"]:
        df[col] = data.get(col)

    # Convert all dict/list columns in df to JSON strings for SQL compatibility
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    # Remove the 'current' column if present
    df.drop("current", axis=1, inplace=True)

    # Save the cleaned DataFrame to CSV
    df.to_csv("romeWeather.csv")

    # --- Hourly Data ---
    # Extract hourly data from the main data dictionary
    hourly = data.get("hourly", [])

    if isinstance(hourly, list):
        # Create DataFrame for hourly data
        df_hourly = pd.DataFrame(hourly)
        # Convert dict/list columns to JSON strings if present
        for col in df_hourly.columns:
            if df_hourly[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df_hourly[col] = df_hourly[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
        # Add global columns to hourly DataFrame
        for col in ["lat", "lon", "timezone", "timezone_offset"]:
            df_hourly[col] = data.get(col)
        # Save hourly DataFrame to CSV
        df_hourly.to_csv("romeWeatherHourly.csv")
    else:
        print("hourly isn't a list")

    # --- Daily Data ---
    # Extract daily data from the main data dictionary
    daily = data.get("daily", [])

    if isinstance(daily, list):
        # Create DataFrame for daily data
        df_daily = pd.DataFrame(daily)
        # Convert dict/list columns to JSON strings if present
        for col in df_daily.columns:
            if df_daily[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df_daily[col] = df_daily[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
        # Add global columns to daily DataFrame
        for col in ["lat", "lon", "timezone", "timezone_offset"]:
            df_daily[col] = data.get(col)
        # Save daily DataFrame to CSV
        df_daily.to_csv("romeWeatherDaily.csv")