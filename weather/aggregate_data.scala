import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("AggregateForViz").enableHiveSupport().getOrCreate()
import spark.implicits._

val merged_data = spark.read.parquet("hdfs:///user/aes10130_nyu_edu/final_project/merged_data")
println(s"Merged records: ${merged_data.count()}")
merged_data.printSchema()

// taxi vs citibike trips by temperature
val taxi_bike_temp = merged_data
  .withColumn("temp_bucket", round(col("temp_f") / 5, 0) * 5)
  .groupBy("temp_bucket", "is_taxi")
  .count()
  .orderBy("temp_bucket")

println("taxi vs citibike by temp")
taxi_bike_temp.show(100, false)

taxi_bike_temp.coalesce(1).write.option("header", "true").mode("overwrite").csv("hdfs:///user/aes10130_nyu_edu/final_project/taxi_bike_temp")
taxi_bike_temp.coalesce(1).write.option("header", "true").mode("overwrite").csv("hdfs:///user/aj3556_nyu_edu/final_project/taxi_bike_temp")

// Weather impact on trip duration
val weather_duration = merged_data
  .withColumn("temp_bucket", round(col("temp_f") / 5, 0) * 5)
  .groupBy("temp_bucket", "is_taxi")
  .agg(
    round(mean("trip_duration"), 2).alias("avg_duration"),
    round(mean("precipitation_mm"), 2).alias("avg_precip"),
    round(mean("windspeed_kmh"), 2).alias("avg_wind"),
    count("*").alias("trip_count")
  )
  .orderBy("temp_bucket")

println("weather impact on trip durations")
weather_duration.show(100, false)

weather_duration.coalesce(1).write.option("header", "true").mode("overwrite").csv("hdfs:///user/aes10130_nyu_edu/final_project/weather_duration")
weather_duration.coalesce(1).write.option("header", "true").mode("overwrite").csv("hdfs:///user/aj3556_nyu_edu/final_project/weather_duration")

println("  hdfs dfs -get /user/aes10130_nyu_edu/final_project/taxi_bike_temp")
println("  hdfs dfs -get /user/aes10130_nyu_edu/final_project/weather_duration")