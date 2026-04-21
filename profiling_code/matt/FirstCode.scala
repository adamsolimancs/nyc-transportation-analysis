import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FirstCode {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CitiBikeFirstCode")
      .getOrCreate()

    import spark.implicits._

    // Read all 12 csvs from HDFS
    val rawData = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("hdfs:/user/mc9967_nyu_edu/citibike_data/*.csv")

    // Code cleaning: Converting station names to lowercase and trimming whitespaces
    val textCleanedDF = rawData.withColumn("start station name", lower(trim(col("start station name"))))
                               .withColumn("end station name", lower(trim(col("end station name"))))

    // Code cleaning: Converting raw string timestamps into Spark formatted timestamps
    val dateCleanedDF = textCleanedDF
      .withColumn("starttime", to_timestamp(col("starttime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("stoptime", to_timestamp(col("stoptime"), "yyyy-MM-dd HH:mm:ss"))

    // Code cleaning: Creating a binary column based on the condition of 'usertype'
    val finalDF = dateCleanedDF.withColumn("is_subscriber", when(col("usertype") === "Subscriber", 1).otherwise(0))

    // Calculating the mean for all numerical data and the standard deviation for latitude
    val meanDF = finalDF.select(
      lit("Mean").as("Stat_Type"),
      mean("start station latitude").alias("Start_Lat"),
      mean("start station longitude").alias("Start_Lng"),
      mean("end station latitude").alias("End_Lat"),
      mean("end station longitude").alias("End_Lng")
    )

    // Calculating the median for all numerical data
    val medLatStart = finalDF.stat.approxQuantile("start station latitude", Array(0.5), 0.001)(0)
    val medLngStart = finalDF.stat.approxQuantile("start station longitude", Array(0.5), 0.001)(0)
    val medLatEnd = finalDF.stat.approxQuantile("end station latitude", Array(0.5), 0.001)(0)
    val medLngEnd = finalDF.stat.approxQuantile("end station longitude", Array(0.5), 0.001)(0)

    val medians = spark.createDataFrame(Seq(
      ("Median", medLatStart, medLngStart, medLatEnd, medLngEnd)
    )).toDF("Stat_Type", "Start_Lat", "Start_Lng", "End_Lat", "End_Lng")

    // Calculating the mode for all numerical data
    val modeLatStartVal = finalDF.groupBy("start station latitude").count().orderBy(desc("count")).first().getDouble(0)
    val modeLngStartVal = finalDF.groupBy("start station longitude").count().orderBy(desc("count")).first().getDouble(0)
    val modeLatEndVal = finalDF.groupBy("end station latitude").count().orderBy(desc("count")).first().getDouble(0)
    val modeLngEndVal = finalDF.groupBy("end station longitude").count().orderBy(desc("count")).first().getDouble(0)

    val modes = spark.createDataFrame(Seq(
      ("Mode", modeLatStartVal, modeLngStartVal, modeLatEndVal, modeLngEndVal)
    )).toDF("Stat_Type", "Start_Lat", "Start_Lng", "End_Lat", "End_Lng")

    // Output the final combined table
    val finalStatsTable = meanDF.union(medians).union(modes)
    finalStatsTable.show()

    // Calculating the standard deviation of latitude
    val stdDevLat = finalDF.select(stddev("start station latitude")).first().getDouble(0)

    // Printing the standard deviation of start station
    println(s"Standard Deviation of Start Station Latitude: $stdDevLat")

    // Save output
    finalDF.write.mode("overwrite").parquet("hdfs:/user/mc9967_nyu_edu/output/cleaned_citibike_data")
    
    spark.stop()
  }
}