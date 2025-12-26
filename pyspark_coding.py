from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Weather Data Transformation") \
    .getOrCreate()

# Sample JSON data
json_data = """
{"time":"2025-12-26T00:00:00.000Z","temperature_2m":4.0,"approx_heat_index":14.06}
{"time":"2025-12-26T01:00:00.000Z","temperature_2m":3.9,"approx_heat_index":14.59}
{"time":"2025-12-26T02:00:00.000Z","temperature_2m":3.9,"approx_heat_index":14.71}
{"time":"2025-12-26T03:00:00.000Z","temperature_2m":3.6,"approx_heat_index":14.65}
{"time":"2025-12-26T04:00:00.000Z","temperature_2m":3.8,"approx_heat_index":16.35}
{"time":"2025-12-26T05:00:00.000Z","temperature_2m":4.0,"approx_heat_index":16.93}
{"time":"2025-12-26T06:00:00.000Z","temperature_2m":4.0,"approx_heat_index":16.06}
{"time":"2025-12-26T07:00:00.000Z","temperature_2m":4.0,"approx_heat_index":14.59}
{"time":"2025-12-26T08:00:00.000Z","temperature_2m":3.9,"approx_heat_index":13.58}
{"time":"2025-12-26T09:00:00.000Z","temperature_2m":4.4,"approx_heat_index":16.65}
{"time":"2025-12-26T10:00:00.000Z","temperature_2m":4.8,"approx_heat_index":15.32}
{"time":"2025-12-26T11:00:00.000Z","temperature_2m":5.5,"approx_heat_index":14.77}
{"time":"2025-12-26T12:00:00.000Z","temperature_2m":6.1,"approx_heat_index":14.48}
{"time":"2025-12-26T13:00:00.000Z","temperature_2m":6.2,"approx_heat_index":15.0}
{"time":"2025-12-26T14:00:00.000Z","temperature_2m":6.2,"approx_heat_index":15.02}
{"time":"2025-12-26T15:00:00.000Z","temperature_2m":6.1,"approx_heat_index":15.35}
{"time":"2025-12-26T16:00:00.000Z","temperature_2m":6.0,"approx_heat_index":15.32}
{"time":"2025-12-26T17:00:00.000Z","temperature_2m":5.7,"approx_heat_index":15.98}
{"time":"2025-12-26T18:00:00.000Z","temperature_2m":5.7,"approx_heat_index":17.55}
{"time":"2025-12-26T19:00:00.000Z","temperature_2m":5.6,"approx_heat_index":18.18}
{"time":"2025-12-26T20:00:00.000Z","temperature_2m":5.7,"approx_heat_index":18.91}
{"time":"2025-12-26T21:00:00.000Z","temperature_2m":5.9,"approx_heat_index":19.41}
{"time":"2025-12-26T22:00:00.000Z","temperature_2m":5.9,"approx_heat_index":20.11}
{"time":"2025-12-26T23:00:00.000Z","temperature_2m":5.6,"approx_heat_index":21.83}
"""

# Write JSON data to a temporary file (simulating data source)
with open("/tmp/weather_data.json", "w") as f:
    f.write(json_data)

# Read JSON data into DataFrame
df = spark.read.json("/tmp/weather_data.json")

# Display original DataFrame
print("Original DataFrame:")
df.show(5, truncate=False)

# Write your solution code here.
# df_transformed = 

# Display final transformed DataFrame
print("\nTransformed DataFrame with comfort_level and heat_index_diff:")
df_transformed.show(truncate=False)

# Optional: Show schema
print("\nFinal Schema:")
df_transformed.printSchema()

# Optional: Show distribution of comfort levels
print("\nComfort Level Distribution:")
df_transformed.groupBy("comfort_level").count().show()
