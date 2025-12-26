from datetime import date
import requests
import pandas as pd

DESTINATION_FOLDER = "destination"


def weather_pipeline():
    today_str = date.today().isoformat()

    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": "51.50",
        "longitude": "-0.11",
        "start_date": today_str,
        "end_date": today_str,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m",
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    data_df = pd.DataFrame(data["hourly"])
    transformed_df = (
        data_df.assign(
            lat=round(data["latitude"], 2),
            lon=round(data["longitude"], 2),
            time=lambda df: pd.to_datetime(df["time"], utc=True),
            approx_heat_index=lambda df: df["temperature_2m"]  + 0.33*df["relative_humidity_2m"] - 0.7*df["wind_speed_10m"] - 4
        )
        .loc[:, ["time", "temperature_2m", "approx_heat_index"]]
    )

    transformed_df.to_json(
        f"./{DESTINATION_FOLDER}/output.jsonl",
        orient="records",
        lines=True,
        date_format="iso",
    )


if __name__ == "__main__":
    weather_pipeline()
