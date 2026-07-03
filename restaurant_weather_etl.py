import os
os.environ["PYSPARK_PYTHON"] = os.path.abspath(".venv/Scripts/python.exe")
os.environ["PYSPARK_DRIVER_PYTHON"] = os.path.abspath(".venv/Scripts/python.exe")
import requests
import geohash2
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType, StringType


load_dotenv()

RESTAURANT_PATH = "data/input/restaurants"
WEATHER_PATH = "data/input/weather"
OUTPUT_PATH = "data/output/enriched_restaurant_weather"

def create_spark():
    python_exec = os.path.abspath(
        ".venv/Scripts/python.exe"
    )

    return (
        SparkSession.builder
        .appName("RestaurantWeatherETL")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.pyspark.python", python_exec)
        .config("spark.pyspark.driver.python", python_exec)
        .getOrCreate()
    )


def read_restaurant_data(spark):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(RESTAURANT_PATH)
    )


def read_weather_data(spark):
    return (
        spark.read
        .option("recursiveFileLookup", "true")
        .parquet(WEATHER_PATH)
    )


def get_coordinates_from_opencage(city, country):
    api_key = os.getenv("OPENCAGE_API_KEY")

    if not api_key:
        return None, None

    address = f"{city}, {country}"

    url = "https://api.opencagedata.com/geocode/v1/json"

    params = {
        "q": address,
        "key": api_key,
        "limit": 1
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data.get("results"):
            return None, None

        geometry = data["results"][0]["geometry"]
        return float(geometry["lat"]), float(geometry["lng"])

    except Exception:
        return None, None


def geohash_4(lat, lng):
    if lat is None or lng is None:
        return None

    try:
        return geohash2.encode(float(lat), float(lng), precision=4)
    except Exception:
        return None


def _build_coordinates_mapping(spark, restaurant_df):
    missing_locations = (
        restaurant_df
        .filter((F.col("lat").isNull()) | (F.col("lng").isNull()))
        .select("city", "country")
        .where(F.col("city").isNotNull() & F.col("country").isNotNull())
        .distinct()
    )

    mapping_rows = []
    for row in missing_locations.toLocalIterator():
        city = row["city"]
        country = row["country"]
        mapped_lat, mapped_lng = get_coordinates_from_opencage(city, country)

        mapping_rows.append({
            "city": city,
            "country": country,
            "mapped_lat": mapped_lat,
            "mapped_lng": mapped_lng
        })

    if not mapping_rows:
        return spark.createDataFrame([], "city string, country string, mapped_lat double, mapped_lng double")

    return spark.createDataFrame(mapping_rows)


def fix_restaurant_coordinates(spark, restaurant_df):
    mapping_df = _build_coordinates_mapping(spark, restaurant_df)

    return (
        restaurant_df
        .join(mapping_df, on=["city", "country"], how="left")
        .withColumn("lat", F.coalesce(F.col("lat"), F.col("mapped_lat")))
        .withColumn("lng", F.coalesce(F.col("lng"), F.col("mapped_lng")))
        .drop("mapped_lat", "mapped_lng")
    )


from pyspark.sql.types import StringType

geohash_udf = F.udf(
    lambda lat, lng: geohash_4(lat, lng),
    StringType()
)

def add_geohash_column(df):
    return df.withColumn(
        "geohash",
        geohash_udf(F.col("lat"), F.col("lng"))
    )


def prepare_restaurants(spark, restaurant_df):
    restaurant_df = (
        restaurant_df
        .withColumn("lat", F.col("lat").cast(DoubleType()))
        .withColumn("lng", F.col("lng").cast(DoubleType()))
    )

    restaurant_df = fix_restaurant_coordinates(spark, restaurant_df)
    restaurant_df = add_geohash_column(restaurant_df)

    return restaurant_df


def prepare_weather(spark, weather_df):
    weather_df = (
        weather_df
        .withColumn("lat", F.col("lat").cast(DoubleType()))
        .withColumn("lng", F.col("lng").cast(DoubleType()))
    )

    weather_df = add_geohash_column(weather_df)

    weather_df = weather_df.dropDuplicates(["geohash", "wthr_date"])

    w = Window.partitionBy("geohash").orderBy(F.col("wthr_date").desc())

    weather_df = (
        weather_df
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumnRenamed("lat", "weather_lat")
        .withColumnRenamed("lng", "weather_lng")
    )

    return weather_df

def join_restaurant_weather(restaurant_df, weather_df):
    return restaurant_df.join(
        weather_df,
        on="geohash",
        how="left"
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print("Reading restaurant data...")
    restaurant_df = read_restaurant_data(spark)

    print("Reading weather data...")
    weather_df = read_weather_data(spark)

    print("Restaurant columns:")
    print(restaurant_df.columns)

    print("Weather columns:")
    print(weather_df.columns)

    print("Null restaurant lat:", restaurant_df.filter(F.col("lat").isNull()).count())
    print("Null restaurant lng:", restaurant_df.filter(F.col("lng").isNull()).count())

    print("Preparing restaurant data...")
    restaurant_df = prepare_restaurants(spark, restaurant_df)

    print("Preparing weather data...")
    weather_df = prepare_weather(spark, weather_df)

    print("Joining datasets...")
    enriched_df = join_restaurant_weather(restaurant_df, weather_df)

    print("Saving output as partitioned parquet...")
    (
        enriched_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .partitionBy("geohash")
        .parquet(OUTPUT_PATH)
    )

    print(f"ETL completed. Output saved to: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
