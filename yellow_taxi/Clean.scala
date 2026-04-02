import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object Clean {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CleanYellowTaxiData")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // TODO: replace with the real dataset path when available.
    val inputPath =
      if (args.nonEmpty) args(0) else "yellow_taxi/mock_yellow_taxi.csv"

    // TODO: replace with the real HDFS destination.
    val outputPath =
      if (args.length > 1) args(1) else "hdfs://localhost:9000/user/adoma/yellow_taxi_cleaned"

    val rawDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)

    // TODO: list the columns you do not need once the schema is finalized.
    val columnsToDrop = Seq(
      // "unneeded_column"
    )

    val trimmedDf =
      if (columnsToDrop.nonEmpty) rawDf.drop(columnsToDrop: _*) else rawDf

    // Add uniform/default columns here if downstream analytics expect them.
    // TODO: confirm whether any standard metadata columns are required.
    val withUniformColumns = trimmedDf
      .withColumn("data_source", lit("yellow_taxi"))

    // TODO: decide which columns are required and should never be null.
    val requiredColumns = Seq(
      // "VendorID",
      // "tpep_pickup_datetime"
    )

    val cleanedDf =
      if (requiredColumns.nonEmpty) withUniformColumns.na.drop(requiredColumns)
      else withUniformColumns

    println(s"Input path: $inputPath")
    println(s"Output path: $outputPath")
    println(s"Records before cleaning: ${rawDf.count()}")
    println(s"Records after cleaning: ${cleanedDf.count()}")

    cleanedDf.write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)

    spark.stop()
  }
}
