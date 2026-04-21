import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .appName("NYCWeatherCountRecs")
  .enableHiveSupport()
  .getOrCreate()

import spark.implicits._

val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .parquet("hdfs:///user/aj3556_nyu_edu/actual_hw6_dir")

println(s"Total records: ${df.count()}")

val yearCounts = df.rdd
  .map(rec => (rec.getAs[String]("year"), 1))
  .countByKey()
yearCounts.foreach(println) // record counts each year,eg: (2019, 8760)

val monthCounts = df.rdd
  .map(rec => (rec.getAs[Int]("month"), 1))
  .countByKey()
monthCounts.foreach(println) // record counts for each month, eg: (1, 20000)

// prints distinct values for each of the columns
println(s"Distinct temp_f values: ${df.select("temp_f").distinct().count()}")
println(s"Distinct date values: ${df.select("date").distinct().count()}")
println(s"Distinct hour values: ${df.select("hour").distinct().count()}")
println(s"Distinct month values: ${df.select("month").distinct().count()}")
println(s"Distinct year values: ${df.select("year").distinct().count()}")
println(s"Distinct precipitation_mm values: ${df.select("precipitation_mm").distinct().count()}")
println(s"Distinct rain_mm values: ${df.select("rain_mm").distinct().count()}")
println(s"Distinct cloudcover_pct values: ${df.select("cloudcover_pct").distinct().count()}")
println(s"Distinct windspeed_kmh values: ${df.select("windspeed_kmh").distinct().count()}")
println(s"Distinct wind_direction_deg values: ${df.select("wind_direction_deg").distinct().count()}")

df.select("hour").distinct().orderBy("hour").show(Int.MaxValue, truncate = false)
df.select("month").distinct().orderBy("month").show(Int.MaxValue, truncate = false)
df.select("year").distinct().orderBy("year").show(Int.MaxValue, truncate = false)

df.select(
  min("temp_f"), max("temp_f"),
  min("hour"), max("hour"),
  min("month"), max("month"),
  min("precipitation_mm"), max("precipitation_mm"),
  min("rain_mm"), max("rain_mm"),
  min("cloudcover_pct"), max("cloudcover_pct"),
  min("windspeed_kmh"), max("windspeed_kmh"),
  min("wind_direction_deg"), max("wind_direction_deg"),
  min("date"), max("date")
).show()