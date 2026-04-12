// Explore your data
// Count the number of records. 
// Map the records to a key and a value and count the number of records using map() function.
// Find the distinct values in each column (columns you are using for your analytic).

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

val spark = SparkSession.builder()
  .appName("HyperlocalTempCountRecs")
  .enableHiveSupport()
  .getOrCreate()

import spark.implicits._

// loadd cleaned data from Clean.scala
val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .parquet("hdfs:///user/aj3556_nyu_edu/hw6_dir")

println(s"Total records: ${df.count()}")

// map records to (borough, 1) and count per borough
val boroughCounts = df.rdd
  .map(rec => (rec.getAs[String]("borough"), 1))
  .countByKey()
boroughCounts.foreach(println)

// map records to (year, 1) and count per year
val yearCounts = df.rdd
  .map(rec => (rec.getAs[String]("year"), 1))
  .countByKey()
yearCounts.foreach(println)

// distinct values per column
println(s"Distinct airtemp values: ${df.select("airtemp").distinct().count()}")
println(s"Distinct day values: ${df.select("day").distinct().count()}")
println(s"Distinct hour values: ${df.select("hour").distinct().count()}")
println(s"Distinct latitude values: ${df.select("latitude").distinct().count()}")
println(s"Distinct longitude values: ${df.select("longitude").distinct().count()}")
println(s"Distinct year values: ${df.select("year").distinct().count()}")
println(s"Distinct borough values: ${df.select("borough").distinct().count()}")
println(s"Distinct ntacode values: ${df.select("ntacode").distinct().count()}")

// distinct values for the categorical columns
df.select("hour").distinct().orderBy("hour").show(Int.MaxValue, truncate = false)
df.select("year").distinct().orderBy("year").show(Int.MaxValue, truncate = false)
df.select("borough").distinct().orderBy("borough").show(Int.MaxValue, truncate = false)
df.select("ntacode").distinct().orderBy("ntacode").show(Int.MaxValue, truncate = false)

// min/max for numeric columns
df.select(
    
  min("airtemp"), max("airtemp"),
  min("hour"),    max("hour"),
  min("latitude"), max("latitude"),
  min("longitude"), max("longitude"),
  min("day"),     max("day")
).show()
