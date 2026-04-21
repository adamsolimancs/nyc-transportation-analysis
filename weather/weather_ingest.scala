import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("ProcessWeather").enableHiveSupport().getOrCreate()
import spark.implicits._

val weather = spark.read.parquet("hdfs:///user/aj3556_nyu_edu/actual_hw7_dir")
println(s"Weather record count: ${weather.count()}")
weather.printSchema()

// rename hour to time_hours to match taxi/citibike schema
val processed_data = weather
  .withColumn("time_hours", col("hour"))
  .select("date", "time_hours", "temp_f", "precipitation_mm", "rain_mm", "cloudcover_pct", "windspeed_kmh", "wind_direction_deg", "season", "beaufort_scale", "isHot", "isWindy")

processed_data.show(20, false)
println(s"Processed weather count: ${processed_data.count()}")
processed_data.coalesce(1).write.option("header", "true").mode("overwrite").parquet("hdfs:///user/aj3556_nyu_edu/final_project/processed_weather")
processed_data.coalesce(1).write.option("header", "true").mode("overwrite").parquet("hdfs:///user/aes10130_nyu_edu/final_project/processed_weather")