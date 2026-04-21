import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
val spark = SparkSession.builder().appName("CleanMerged").enableHiveSupport().getOrCreate()
import spark.implicits._
val raw_data = spark.read.parquet("hdfs:///user/aj3556_nyu_edu/final_project/merged_data")

println(s"Raw merged record count: ${raw_data.count()}") // total count for all 3 datasets
raw_data.printSchema()

val data_2019 = raw_data.filter(substring(col("date"), 1, 4) === "2019")
println(s"After 2019 filter: ${data_2019.count()}")

// drop rows where key trip columns are null
val non_Nulls = data_2019.na.drop(cols = Seq("date", "time_hours", "start_longitude", "start_latitude", "trip_duration", "distance", "is_taxi"))
println(s"After dropping null trips: ${non_Nulls.count()}")


val validated_datat = non_Nulls
  .filter(col("trip_duration") > 0)
  .filter(col("trip_duration") < 86400)           // less than 24 hours in seconds
  .filter(col("distance") >= 0)
  .filter(col("distance") < 200)                  // max travel distance
  .filter(col("time_hours").between(0, 23))
  .filter(col("start_latitude").between(40.0, 41.5))
  .filter(col("start_longitude").between(-74.5, -73.0))
println(s"After outlier filter: ${validated_datat.count()}")

// replace missing weather values with column averages
val avg_TempF = validated_datat.select(mean("temp_f")).first().getDouble(0)
val avg_Precip = validated_datat.select(mean("precipitation_mm")).first().getDouble(0)
val avg_Rain = validated_datat.select(mean("rain_mm")).first().getDouble(0)
val avg_Cloud = validated_datat.select(mean("cloudcover_pct")).first().getDouble(0)
val avg_Wind = validated_datat.select(mean("windspeed_kmh")).first().getDouble(0)
val avg_WindDir = validated_datat.select(mean("wind_direction_deg")).first().getDouble(0)

val filled = validated_datat
  .withColumn("temp_f",             when(col("temp_f").isNull, lit(avg_TempF)).otherwise(col("temp_f")))
  .withColumn("precipitation_mm",   when(col("precipitation_mm").isNull, lit(avg_Precip)).otherwise(col("precipitation_mm")))
  .withColumn("rain_mm",            when(col("rain_mm").isNull, lit(avg_Rain)).otherwise(col("rain_mm")))
  .withColumn("cloudcover_pct",     when(col("cloudcover_pct").isNull, lit(avg_Cloud)).otherwise(col("cloudcover_pct")))
  .withColumn("windspeed_kmh",      when(col("windspeed_kmh").isNull, lit(avg_Wind)).otherwise(col("windspeed_kmh")))
  .withColumn("wind_direction_deg", when(col("wind_direction_deg").isNull, lit(avg_WindDir)).otherwise(col("wind_direction_deg")))
println(s"After filling missing weather values: ${filled.count()}")

// divide the month into 2 segments: early = days 1-15 and late = days 16-31
val month_segments = filled
  .withColumn("day_of_month", dayofmonth(to_date(col("date"), "yyyy-MM-dd")))
  .withColumn("month", substring(col("date"), 6, 2).cast("int"))
  .withColumn("month_segment", when(col("day_of_month") <= 15, "early").otherwise("late"))
println(s"Final cleaned record count: ${month_segments.count()}")

// displays month and the counts of earlier in the month and later in the month, to see monthly distribution
month_segments.groupBy("month", "month_segment").count().orderBy("month", "month_segment").show(24, false)


month_segments.show(20, false)

// count all the nulls of each of the columns of the merged dataset to see if cleaning worked or not
println("Null record counts after cleaning:")
month_segments.select(
  sum(when(col("temp_f").isNull, 1).otherwise(0)).alias("null_temp_f"),
  sum(when(col("precipitation_mm").isNull, 1).otherwise(0)).alias("null_precip"),
  sum(when(col("rain_mm").isNull, 1).otherwise(0)).alias("null_rain"),
  sum(when(col("cloudcover_pct").isNull, 1).otherwise(0)).alias("null_cloud"),
  sum(when(col("windspeed_kmh").isNull, 1).otherwise(0)).alias("null_wind"),
  sum(when(col("trip_duration").isNull, 1).otherwise(0)).alias("null_trip_dur"),
  sum(when(col("distance").isNull, 1).otherwise(0)).alias("null_distance")
).show(false)

spark.sql("DROP TABLE IF EXISTS aj3556_nyu_edu.merged_cleaned_2019")
// spark.sql("DROP TABLE IF EXISTS aes10130_nyu_edu.merged_trips_weather_2019")

month_segments.coalesce(1).write.option("header", "true").mode("overwrite").parquet("hdfs:///user/aj3556_nyu_edu/final_project/merged_cleaned")
month_segments.write.mode("overwrite").saveAsTable("aj3556_nyu_edu.merged_cleaned_2019")

month_segments.coalesce(1).write.option("header", "true").mode("overwrite").parquet("hdfs:///user/aes10130_nyu_edu/final_project/merged_data")
// month_segments.write.mode("overwrite").saveAsTable("aes10130_nyu_edu.merged_trips_weather_2019")

