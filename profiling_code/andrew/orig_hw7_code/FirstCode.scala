
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// analysis and cleaning for 2019 only data.
// Mean, median, and mode of numerical data.
// Standard Deviation for airtemp.
// Cleaning: date formatting, text formatting (borough to uppercase), binary column (isHot).


val spark = SparkSession.builder().appName("FirstCode2019").enableHiveSupport().getOrCreate()
import spark.implicits._
val cleaned = spark.read.parquet("hdfs:///user/aj3556_nyu_edu/hw6_dir") // get cleaned data from Clean.scala


val df = cleaned.filter(col("year") === "2019") // only use 2019 data
println(s"2019 record count: ${df.count()}") // displays total 2019 recs

// date formatting: add day-isso column in yyyy-MM-dd format
val withIsoDate = df.withColumn("day_iso", date_format(col("day"), "yyyy-MM-dd"))
withIsoDate.select("day", "day_iso").distinct().orderBy("day").show(10, false)

// text formatting: uppercase borough names
val withUpperBorough = withIsoDate.withColumn("borough", upper(trim(col("borough"))))
withUpperBorough.select("borough").distinct().orderBy("borough").show(false)

// binary column: isHot is 1 if airtemp >= 80 fahrenhiet else 0
val finalDF = withUpperBorough.withColumn("isHot", when(col("airtemp") >= 80.0, 1).otherwise(0))
finalDF.groupBy("isHot").count().orderBy("isHot").show()
finalDF.cache()


// median - uses the 50th percentilee
finalDF.select(
  percentile_approx(col("airtemp"),   lit(0.5), lit(10000)).alias("median_airtemp"),
  percentile_approx(col("hour"),      lit(0.5), lit(10000)).alias("median_hour"),
  percentile_approx(col("latitude"),  lit(0.5), lit(10000)).alias("median_latitude"),
  percentile_approx(col("longitude"), lit(0.5), lit(10000)).alias("median_longitude")
).show(false)

// mean + standard deviationn
finalDF.select(
  round(mean("airtemp"),     4).alias("mean_airtemp"),
  round(stddev("airtemp"),   4).alias("stddev_airtemp"),
  round(mean("hour"),        4).alias("mean_hour"),
  round(mean("latitude"),    6).alias("mean_latitude"),
  round(mean("longitude"),   6).alias("mean_longitude")
).show(false)



// map airtemp to (value, 1) + finds the most frequent
// same logic for hour, borough, and ntacode
val airtempMode =finalDF.rdd.map(r =>(r.getAs[Double]("airtemp"), 1)).reduceByKey((a, b) => a + b).sortBy({ case (temp, count) => count}, ascending = false).first()
println(s"Mode airtemp: ${airtempMode._1} (appears ${airtempMode._2} times)")

val hourMode = finalDF.rdd.map(r => (r.getAs[Int]("hour"), 1)).reduceByKey((a, b) => a + b).sortBy({case (hour, count) => count }, ascending = false).first()
println(s"Mode hour: ${hourMode._1} (appears ${hourMode._2} times)")

val boroughMode = finalDF.rdd.map(r => (r.getAs[String]("borough"),1)).reduceByKey((a, b) => a + b).sortBy({ case (borough, count) => count}, ascending = false).first()
println(s"Mode borough: ${boroughMode._1} (appears ${boroughMode._2} times)")

val ntaMode = finalDF.rdd.map(r => (r.getAs[String]("ntacode"), 1)).reduceByKey((a, b) =>a + b).sortBy({ case(ntacode, count) => count },ascending = false).first()
println(s"Mode ntacode: ${ntaMode._1} (appears ${ntaMode._2} times)")

// average airtemp by borough
finalDF.groupBy("borough").agg(round(mean("airtemp"), 2).alias("avg_airtemp")).orderBy(desc("avg_airtemp")).show(false)
finalDF.coalesce(1).write.option("header", "true").mode("overwrite").parquet("hdfs:///user/aj3556_nyu_edu/hw7_dir")
finalDF.write.mode("overwrite").saveAsTable("aj3556_nyu_edu.hyperlocal_temp_2019")
