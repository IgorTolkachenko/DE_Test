import pandas as pd
from pandas.testing import assert_frame_equal

from weather_pipeline import weather_pipeline


def test_etl_weather_data():
    weather_pipeline()

    weather_data = pd.read_json(
        "destination/output.jsonl",
        orient="records",
        lines=True,
    )
    expected_first_rows = pd.DataFrame(
        [
            {
                "time": "2025-07-01T00:00:00.000Z",
                "temperature_2m": 25.1,
                "approx_heat_index": 39.15,
            },
            {
                "time": "2025-07-01T01:00:00.000Z",
                "temperature_2m": 24.7,
                "approx_heat_index": 39.50,
            },
        ]
    )

    assert_frame_equal(weather_data.head(2), expected_first_rows)
