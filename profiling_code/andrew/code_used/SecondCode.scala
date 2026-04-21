import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder().appName("FirstCode2019").enableHiveSupport().getOrCreate()
import spark.implicits._
val cleaned = spark.read.parquet("hdfs:///user/aj3556_nyu_edu/actual_hw6_dir")

// filter by 2019
val df = cleaned.filter(col("year") === "2019")
println(s"2019 record count: ${df.count()}")

// Date formatting
val withIsoDate = df.withColumn("date_iso", date_format(col("timestamp"), "yyyy-MM-dd'T'HH:mm"))
withIsoDate.select("timestamp", "date_iso").distinct().orderBy("timestamp").show(10, false)

val withSeasons = withIsoDate.withColumn("season",
  when(col("month").isin(12, 1, 2), "WINTER")
    .when(col("month").isin(3, 4, 5), "SPRING")
    .when(col("month").isin(6, 7, 8), "SUMMER")
    .otherwise("FALL")
)

// integrated the beaufort scale to 
// https://www.rmets.org/metmatters/beaufort-wind-scale
val withBeaufort = withSeasons.withColumn("beaufort_scale",
  when(col("windspeed_kmh") < 1, lit("0 - Calm"))
    .when(col("windspeed_kmh").between(1, 5), lit("1 - Light Air"))
    .when(col("windspeed_kmh").between(6, 11), lit("2 - Light Breeze"))
    .when(col("windspeed_kmh").between(12, 19), lit("3 - Gentle Breeze"))
    .when(col("windspeed_kmh").between(20, 28), lit("4 - Moderate Breeze"))
    .when(col("windspeed_kmh").between(29, 38), lit("5 - Fresh Breeze"))
    .when(col("windspeed_kmh").between(39, 49), lit("6 - Strong Breeze"))
    .when(col("windspeed_kmh").between(50, 61), lit("7 - Near Gale"))
    .when(col("windspeed_kmh").between(62, 74), lit("8 - Gale"))
    .when(col("windspeed_kmh").between(75, 88), lit("9 - Strong Gale"))
    .when(col("windspeed_kmh").between(89, 102), lit("10 - Storm"))
    .when(col("windspeed_kmh").between(103, 117), lit("11 - Violent Storm"))
    .when(col("windspeed_kmh") >= 118, lit("12 - Hurricane"))
    .otherwise(lit("Unknown"))
)

// Binary column - isHot (temp >= 80F) and isWindy (wind >= 29 km/h)
val withBinary = withBeaufort
  .withColumn("isHot",   when(col("temp_f") >= 80.0, 1).otherwise(0)) // if 80 -> temp is hot
  .withColumn("isWindy", when(col("windspeed_kmh") >= 29, 1).otherwise(0)) // if wind is 29 and above -> windy else not

val finalDF = withBinary
finalDF.cache()

finalDF.groupBy("isHot").count().orderBy("isHot").show()
finalDF.groupBy("isWindy").count().orderBy("isWindy").show()
finalDF.groupBy("beaufort_scale").count().orderBy("beaufort_scale").show(false)

// gets median
finalDF.select(
  percentile_approx(col("temp_f"), lit(0.5), lit(10000)).alias("median_temp_f"),
  percentile_approx(col("hour"),lit(0.5), lit(10000)).alias("median_hour"),
  percentile_approx(col("precipitation_mm"),lit(0.5), lit(10000)).alias("median_precipitation_mm"),
  percentile_approx(col("windspeed_kmh"), lit(0.5), lit(10000)).alias("median_windspeed_kmh")
).show(false)

// gets avg
finalDF.select(
  round(mean("temp_f"), 4).alias("mean_temp_f"),
  round(stddev("temp_f"),4).alias("stddev_temp_f"),
  round(mean("hour"),4).alias("mean_hour"),
  round(mean("precipitation_mm"),4).alias("mean_precipitation_mm"),
  round(mean("rain_mm"), 4).alias("mean_rain_mm"),
  round(mean("cloudcover_pct"), 4).alias("mean_cloudcover_pct"),
  round(mean("windspeed_kmh"), 4).alias("mean_windspeed_kmh"),
  round(mean("wind_direction_deg"), 4).alias("mean_wind_direction_deg")
).show(false)

// prints out the mode
val tempMode = finalDF.rdd.map(r => (r.getAs[Double]("temp_f"), 1)).reduceByKey((a, b) => a + b).sortBy({ case (temp, count) => count }, ascending = false).first()
println(s"Mode temp_f: ${tempMode._1} (appears ${tempMode._2} times)")
val hourMode = finalDF.rdd.map(r => (r.getAs[Int]("hour"), 1)).reduceByKey((a, b) => a + b).sortBy({ case (hour, count) => count }, ascending = false).first()
println(s"Mode hour: ${hourMode._1} (appears ${hourMode._2} times)")
val monthMode = finalDF.rdd.map(r => (r.getAs[Int]("month"), 1)).reduceByKey((a, b) => a + b).sortBy({ case (month, count) => count }, ascending = false).first()
println(s"Mode month: ${monthMode._1} (appears ${monthMode._2} times)")
val seasonMode = finalDF.rdd.map(r => (r.getAs[String]("season"), 1)).reduceByKey((a, b) => a + b).sortBy({ case (season, count) => count }, ascending = false).first()
println(s"Mode season: ${seasonMode._1} (appears ${seasonMode._2} times)")
val beaufortMode = finalDF.rdd.map(r => (r.getAs[String]("beaufort_scale"), 1)).reduceByKey((a, b) => a + b).sortBy({ case (bf, count) => count }, ascending = false).first()
println(s"Mode beaufort_scale: ${beaufortMode._1} (appears ${beaufortMode._2} times)")

finalDF.groupBy("month").agg(round(mean("temp_f"), 2).alias("avg_temp_f")).orderBy("month").show(false)
finalDF.groupBy("season").agg(round(mean("temp_f"), 2).alias("avg_temp_f")).orderBy(desc("avg_temp_f")).show(false)
finalDF.groupBy("beaufort_scale").agg(round(mean("temp_f"), 2).alias("avg_temp_f"), count("*").alias("count")).orderBy("beaufort_scale").show(false)

finalDF.coalesce(1).write.option("header", "true").mode("overwrite").parquet("hdfs:///user/aj3556_nyu_edu/actual_hw7_dir")
finalDF.write.mode("overwrite").saveAsTable("aj3556_nyu_edu.nyc_weather_2019")