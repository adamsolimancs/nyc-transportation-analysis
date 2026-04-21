import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder()
  .appName("NYCWeatherCleaning")
  .enableHiveSupport()
  .getOrCreate()

import spark.implicits._

val raw_data = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("NYC_Weather_2016_2022.csv")

raw_data.printSchema()
println(s"Raw row count: ${raw_data.count()}")

val dropped_cols = raw_data.drop(
  "cloudcover_low (%)",
  "cloudcover_mid (%)",
  "cloudcover_high (%)"
)

val renamed_data = dropped_cols
  .withColumnRenamed("time",                  "timestamp")
  .withColumnRenamed("temperature_2m (°C)",   "temp_c")
  .withColumnRenamed("precipitation (mm)",    "precipitation_mm")
  .withColumnRenamed("rain (mm)",             "rain_mm")
  .withColumnRenamed("cloudcover (%)",        "cloudcover_pct")
  .withColumnRenamed("windspeed_10m (km/h)",  "windspeed_kmh")
  .withColumnRenamed("winddirection_10m (°)", "wind_direction_deg")

val casted_data = renamed_data
  .withColumn("timestamp",         to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm"))
  .withColumn("temp_c",            col("temp_c").cast(DoubleType))
  .withColumn("precipitation_mm",  col("precipitation_mm").cast(DoubleType))
  .withColumn("rain_mm",           col("rain_mm").cast(DoubleType))
  .withColumn("cloudcover_pct",    col("cloudcover_pct").cast(DoubleType))
  .withColumn("windspeed_kmh",     col("windspeed_kmh").cast(DoubleType))
  .withColumn("wind_direction_deg",col("wind_direction_deg").cast(DoubleType))

val enriched = casted_data
  .withColumn("date",  to_date(col("timestamp")))
  .withColumn("hour",  hour(col("timestamp")).cast(IntegerType))
  .withColumn("month", month(col("timestamp")).cast(IntegerType))
  .withColumn("year",  year(col("timestamp")).cast(StringType))

val converted_data = enriched
  .withColumn("temp_f",             round(col("temp_c") * 9.0 / 5.0 + 32.0, 2)) // converts C -> F
  .withColumn("precipitation_mm",  round(col("precipitation_mm"), 2))
  .withColumn("rain_mm",           round(col("rain_mm"), 2))
  .withColumn("cloudcover_pct",    round(col("cloudcover_pct"), 2))
  .withColumn("windspeed_kmh",     round(col("windspeed_kmh"), 2))
  .withColumn("wind_direction_deg",round(col("wind_direction_deg"), 2))

val cleaned_data = converted_data.na.drop( // any value with null in 1 of these columns, the row gets droppped
  cols = Seq(
    "timestamp", "temp_f",
    "precipitation_mm", "rain_mm",
    "cloudcover_pct", "windspeed_kmh", "wind_direction_deg",
    "date", "hour", "month", "year"
  )
)

val validated_data = cleaned_data
  .filter(col("temp_f").between(-20.0, 115.0)) // https://www.weather.gov/media/okx/Climate/CentralPark/extremes.pdf: coldest temp: -15 F, highest temp: 106 F, gave a little more leeway
  .drop("temp_c")
  .filter(col("precipitation_mm") >= 0.0)
  .filter(col("rain_mm") >= 0.0)
  .filter(col("cloudcover_pct").between(0.0, 100.0)) // 0 - 100% cloud cover
  .filter(col("windspeed_kmh") >= 0.0)
  .filter(col("wind_direction_deg").between(0.0, 360.0)) // 0 - 360 deg
  .filter(col("hour").between(0, 23))

validated_data.printSchema()
println(s"Cleaned row count: ${validated_data.count()}")
validated_data.show(200, truncate = false)

val output = validated_data.select(
  "timestamp", "date", "hour", "month", "year",
  "temp_f",
  "precipitation_mm", "rain_mm",
  "cloudcover_pct", "windspeed_kmh", "wind_direction_deg"
)

output
  .coalesce(1)
  .write
  .option("header", "true")
  .mode("overwrite")
  .parquet("hdfs:///user/aj3556_nyu_edu/actual_hw6_dir")