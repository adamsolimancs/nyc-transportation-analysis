



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// analysis and cleaning for 2019 only data.
// Mean, median, and mode of numerical data.
// Standard Deviation for airtemp.
// Cleaning: will do date formatting, text formatting (borough to uppercase), binary column (isHot).

// columns used for the analysis
    // AirTemp,average hourly air temperature Fahrenheit,airtemp,Number
    // Day,Date,day,Floating Timestamp
    // Hour,Hour of day,hour,Number
    // Latitude,Latitude of sensor,latitude,Number
    // Longitude,Longitude of sensor,longitude,Number
    // Year,Year,year,Text
    // Borough,borough,borough,Text
    // ntacode,Neighborhood Tabulation Areas (NTA) code,ntacode,Text

val spark = SparkSession.builder()
  .appName("FirstCode2019")
  .enableHiveSupport()
  .getOrCreate()

import spark.implicits._

// load cleaned data from Clean.scala
val cleaned = spark.read
  .parquet("hdfs:///user/aj3556_nyu_edu/hw6_dir")

val df = cleaned.filter(col("year") === "2019") // filter to 2019 data
println(s"2019 record count: ${df.count()}")

// date formatting: add iso formatted date column (yyyy-MM-dd)
val withIsoDate = df
  .withColumn("day_iso", date_format(col("day"), "yyyy-MM-dd"))
withIsoDate.select("day", "day_iso").distinct().orderBy("day").show(10, truncate = false)

// text formatting: normalize borough to uppercase for safe joins
val withUpperBorough = withIsoDate
  .withColumn("borough", upper(trim(col("borough"))))
withUpperBorough.select("borough").distinct().orderBy("borough").show(truncate = false)

// binary column - isHot: 1 if airtemp >= 80 degrees fahrenheit, else 0
val finalDF = withUpperBorough
  .withColumn("isHot", when(col("airtemp") >= 80.0, 1).otherwise(0))

finalDF.groupBy("isHot").count().orderBy("isHot").show()
finalDF.cache()

// mean and standard deviation
finalDF.select(
  round(mean("airtemp"),     4).alias("mean_airtemp"),
  round(stddev("airtemp"),   4).alias("stddev_airtemp"),
  round(mean("hour"),        4).alias("mean_hour"),
  round(stddev("hour"),      4).alias("stddev_hour"),
  round(mean("latitude"),    6).alias("mean_latitude"),
  round(stddev("latitude"),  6).alias("stddev_latitude"),
  round(mean("longitude"),   6).alias("mean_longitude"),
  round(stddev("longitude"), 6).alias("stddev_longitude")
).show(truncate = false)

// median using percentile_approx
finalDF.select(
  percentile_approx(col("airtemp"),   lit(0.5)).alias("median_airtemp"),
  percentile_approx(col("hour"),      lit(0.5)).alias("median_hour"),
  percentile_approx(col("latitude"),  lit(0.5)).alias("median_latitude"),
  percentile_approx(col("longitude"), lit(0.5)).alias("median_longitude")
).show(truncate = false)

// mode of airtemp
val airtempMode = finalDF.rdd
  .map(r => (r.getAs[Double]("airtemp"), 1))
  .reduceByKey(_ + _)
  .sortBy(_._2, ascending = false)
  .first()
println(s"Mode airtemp: ${airtempMode._1} (appears ${airtempMode._2} times)")

// mode of hour
val hourMode = finalDF.rdd
  .map(r => (r.getAs[Int]("hour"), 1))
  .reduceByKey(_ + _)
  .sortBy(_._2, ascending = false)
  .first()
println(s"Mode hour: ${hourMode._1} (appears ${hourMode._2} times)")

// mode of borough
val boroughMode = finalDF.rdd
  .map(r => (r.getAs[String]("borough"), 1))
  .reduceByKey(_ + _)
  .sortBy(_._2, ascending = false)
  .first()
println(s"Mode borough: ${boroughMode._1} (appears ${boroughMode._2} times)")

// mode of ntacode
val ntaMode = finalDF.rdd
  .map(r => (r.getAs[String]("ntacode"), 1))
  .reduceByKey(_ + _)
  .sortBy(_._2, ascending = false)
  .first()
println(s"Mode ntacode: ${ntaMode._1} (appears ${ntaMode._2} times)")

// average airtemp via borough
finalDF
  .groupBy("borough")
  .agg(round(mean("airtemp"), 2).alias("avg_airtemp"))
  .orderBy(desc("avg_airtemp"))
  .show(truncate = false)

// write final 2019 dataset to hdfs
finalDF
  .coalesce(1)
  .write
  .option("header", "true")
  .mode("overwrite")
  .parquet("hdfs:///user/aj3556_nyu_edu/hw7_dir")