import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object Clean_Merged {
  private val DefaultPath = "hdfs:///user/aes10130_nyu_edu/final_project/merged_data"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CleanMerged")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val inputPath = if (args.nonEmpty) args(0) else DefaultPath
    val outputPath = if (args.length > 1) args(1) else DefaultPath
    val tempPath = if (args.length > 2) args(2) else s"${outputPath}_tmp"

    val rawDf = spark.read.parquet(inputPath)

    val requiredColumns = Seq(
      "date",
      "time_hours",
      "start_longitude",
      "start_latitude",
      "trip_duration",
      "distance",
      "is_taxi",
      "temp_f",
      "precipitation_mm",
      "rain_mm",
      "cloudcover_pct",
      "windspeed_kmh",
      "wind_direction_deg"
    ).filter(rawDf.columns.contains)

    val cleanedDf = rawDf
      .na.drop(requiredColumns)
      .filter(col("trip_duration") > 0)
      .filter(col("distance") >= 0)
      .filter(col("time_hours").between(0, 23))
      .filter(col("start_latitude").between(40.0, 41.5))
      .filter(col("start_longitude").between(-74.5, -73.0))
      .dropDuplicates()
      .cache()

    val rawCount = rawDf.count()
    val cleanedCount = cleanedDf.count()

    println(s"Input path: $inputPath")
    println(s"Output path: $outputPath")
    println(s"Rows before cleaning: $rawCount")
    println(s"Rows after cleaning: $cleanedCount")

    overwritePath(cleanedDf, tempPath, outputPath, spark)

    cleanedDf.unpersist()
    spark.stop()
  }

  private def overwritePath(df: DataFrame, tempPath: String, outputPath: String, spark: SparkSession): Unit = {
    val output = new Path(outputPath)
    val temp = new Path(tempPath)
    val fs = output.getFileSystem(spark.sparkContext.hadoopConfiguration)

    if (fs.exists(temp)) {
      fs.delete(temp, true)
    }

    df.write
      .mode("overwrite")
      .parquet(tempPath)

    if (fs.exists(output)) {
      fs.delete(output, true)
    }

    if (!fs.rename(temp, output)) {
      throw new RuntimeException(s"Failed to move $tempPath to $outputPath")
    }
  }
}
