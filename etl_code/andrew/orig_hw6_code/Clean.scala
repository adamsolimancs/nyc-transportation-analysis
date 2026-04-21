// Data cleaning - to avoid nasty exceptions later on in your analytic.
// Drop columns you do not need, add columns with uniform values (if needed), drop rows if they are NULL if you decide they aren't to be included.

// columns that are going to be used for the analysis
    // AirTemp,average hourly air temperature Fahrenheit,airtemp,Number
    // Day,Date,day,Floating Timestamp
    // Hour,Hour of day,hour,Number
    // Latitude,Latitude of sensor,latitude,Number
    // Longitude,Longitude of sensor,longitude,Number
    // Year,Year,year,Text
    // Borough,borough,borough,Text
    // ntacode,Neighborhood Tabulation Areas (NTA) code,ntacode,Text

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder()
  .appName("HyperlocalTempCleaning")
  .enableHiveSupport()
  .getOrCreate()

import spark.implicits._

// load the raw csv
val raw = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("Hyperlocal_Temperature_Monitoring_20260326.csv")

raw.printSchema()
println(s"Raw row count: ${raw.count()}")

// drop columns
val dropped = raw.drop("Sensor.ID", "Install.Type")

// rename columns to lowercase to make sql expressions easer to write
val renamed = dropped
  .withColumnRenamed("AirTemp",   "airtemp")
  .withColumnRenamed("Day",       "day")
  .withColumnRenamed("Hour",      "hour")
  .withColumnRenamed("Latitude",  "latitude")
  .withColumnRenamed("Longitude", "longitude")
  .withColumnRenamed("Year",      "year")
  .withColumnRenamed("Borough",   "borough")

// fixed types 
val casted = renamed
  .withColumn("airtemp",   col("airtemp").cast(DoubleType))
  .withColumn("day",       to_date(col("day"), "MM/dd/yyyy"))
  .withColumn("hour",      col("hour").cast(IntegerType))
  .withColumn("latitude",  col("latitude").cast(DoubleType))
  .withColumn("longitude", col("longitude").cast(DoubleType))
  .withColumn("year",      col("year").cast(StringType))

// round airtemp to 2 decimal places
val rounded = casted
  .withColumn("airtemp", round(col("airtemp"), 2))

// drop rows where any of our key columns are null
val cleaned = rounded.na.drop(
  cols = Seq("airtemp", "day", "hour", "latitude", "longitude", "year", "borough", "ntacode")
)

// filter out bad/outliar sensor readings 
val validated = cleaned
  .filter(col("airtemp").between(-20.0, 130.0))
  .filter(col("hour").between(0, 23))
//   .filter(col("latitude").between(40.0, 41.5))   // NYC bounding box
//   .filter(col("longitude").between(-74.5, -73.5))

validated.printSchema()
println(s"Cleaned row count: ${validated.count()}")
validated.show(200, truncate = false) // shows only first 200 rows
// validated.show(validated.count().toInt, truncate = false) // shows all rows

// write in txt file
validated
  .coalesce(1)
  .write
  .option("header", "true")
  .mode("overwrite")
  .parquet("hdfs:///user/aj3556_nyu_edu/hw6_dir")