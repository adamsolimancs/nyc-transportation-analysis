import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{asc, avg, col, date_format, desc, lit, stddev}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, TimestampType}

object EDA {
  private val DefaultInputPath = Paths.get("yellow_taxi", "data").toString
  private val TimestampPattern = "yyyy-MM-dd HH:mm:ss"

  private val RequiredColumns = Seq(
    ColumnSpec("tpep_pickup_datetime", TimestampType),
    ColumnSpec("tpep_dropoff_datetime", TimestampType),
    ColumnSpec("fare_amount", DoubleType),
    ColumnSpec("tip_amount", DoubleType),
    ColumnSpec("total_amount", DoubleType),
    ColumnSpec("passenger_count", DoubleType),
    ColumnSpec("trip_distance", DoubleType)
  )

  private val NumericColumns = Seq(
    "fare_amount",
    "tip_amount",
    "total_amount",
    "passenger_count",
    "trip_distance"
  )

  def main(args: Array[String]): Unit = {
    // build spark sesh
    val spark = SparkSession
      .builder()
      .appName("YellowTaxiEDA")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // get data from command line args, error check
    val inputPath = if (args.nonEmpty) args(0) else DefaultInputPath
    val parquetPaths = resolveParquetPaths(spark, inputPath)

    require(parquetPaths.nonEmpty, s"No parquet files found at $inputPath")

    // Run helper functions for each individual tasks
    val taxiDf = selectRequiredColumns(spark.read.parquet(parquetPaths: _*)).cache()
    val recordCount = taxiDf.count()
    val columnStats = computeColumnStats(taxiDf)
    val totalAmountStdDev = computeTotalAmountStdDev(taxiDf)
    val enrichedDf = addDerivedColumns(taxiDf)

    // output:
    println(s"Input path: $inputPath")
    println(s"Records loaded: $recordCount")
    println()
    println("Summary statistics:")
    columnStats.foreach(printColumnStats)
    println(s"total_amount standard deviation = $totalAmountStdDev")
    println()
    println("Preview with normalized datetimes and trip_time_length:")
    enrichedDf.show(20, truncate = false)

    taxiDf.unpersist()
    spark.stop()
  }

  /*
  * Helper Functions
  */

  private def selectRequiredColumns(df: DataFrame): DataFrame =
    df.select(RequiredColumns.map(spec => castOrNull(df, spec.name, spec.dataType).as(spec.name)): _*)

  private def computeColumnStats(df: DataFrame): Seq[ColumnStats] = {
    // create list of spark agg expressiosn to compute the mean of each numeric col
    val meanExpressions = NumericColumns.map(name => avg(col(name)).as(name))
    val meanRow = df.agg(meanExpressions.head, meanExpressions.tail: _*).first()
    val medians = df.stat.approxQuantile(NumericColumns.toArray, Array(0.5), 0.0)

    // create (col,index) pairs and map them to ColumnStats.
    // use case to destructure result of zipWithIndex (col, index)
    NumericColumns.zipWithIndex.map { case (columnName, index) =>
      ColumnStats(
        columnName = columnName,
        mean = rowDoubleOrNaN(meanRow, index),
        median = medians(index).headOption.getOrElse(Double.NaN),
        mode = computeMode(df, columnName).getOrElse(Double.NaN)
      )
    }
  }

  private def computeMode(df: DataFrame, columnName: String): Option[Double] =
    df.where(col(columnName).isNotNull)
      .groupBy(col(columnName))
      .count()
      .orderBy(desc("count"), asc(columnName))
      .limit(1)
      .collect()
      .headOption
      .map(_.getDouble(0))

  private def computeTotalAmountStdDev(df: DataFrame): Double =
    {
      val stdDevRow = df.agg(stddev(col("total_amount")).as("total_amount_stddev")).first()
      rowDoubleOrNaN(stdDevRow, stdDevRow.fieldIndex("total_amount_stddev"))
    }

  // add new column 'trip_time_length' and format dates in the datetime columns. 
  // Also do basic clean (cast or null).
  private def addDerivedColumns(df: DataFrame): DataFrame =
    df.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast(TimestampType))
      .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast(TimestampType))
      .withColumn("fare_amount", col("fare_amount").cast(DoubleType))
      .withColumn("tip_amount", col("tip_amount").cast(DoubleType))
      .withColumn("total_amount", col("total_amount").cast(DoubleType))
      .withColumn("passenger_count", col("passenger_count").cast(DoubleType))
      .withColumn("trip_distance", col("trip_distance").cast(DoubleType))
      .na.drop(Seq("tpep_pickup_datetime", "tpep_dropoff_datetime"))
      .withColumn(
        "trip_time_length",
        col("tpep_dropoff_datetime").cast(LongType) - col("tpep_pickup_datetime").cast(LongType)
      )
      .withColumn(
        "tpep_pickup_datetime",
        date_format(col("tpep_pickup_datetime"), TimestampPattern)
      )
      .withColumn(
        "tpep_dropoff_datetime",
        date_format(col("tpep_dropoff_datetime"), TimestampPattern)
      )

  private def printColumnStats(stats: ColumnStats): Unit = {
    println(s"${stats.columnName}:")
    println(s"  mean = ${stats.mean}")
    println(s"  median = ${stats.median}")
    println(s"  mode = ${stats.mode}")
  }

  private def resolveParquetPaths(spark: SparkSession, datasetPath: String): Seq[String] = {
    if (!hasUriScheme(datasetPath)) {
      val localPath = Paths.get(datasetPath)

      if (Files.isRegularFile(localPath) && datasetPath.endsWith(".parquet")) {
        return Seq(localPath.toString)
      }

      if (Files.isDirectory(localPath)) {
        val stream = Files.list(localPath)

        try {
          return stream.iterator().asScala
            .filter(entry => Files.isRegularFile(entry))
            .filter(entry => entry.getFileName.toString.endsWith(".parquet"))
            .map(_.toString)
            .toSeq
            .sorted
        } finally {
          stream.close()
        }
      }
    }

    val hadoopPath = new HadoopPath(datasetPath)
    val fs = hadoopPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

    require(fs.exists(hadoopPath), s"Dataset path not found: $datasetPath")

    if (fs.isFile(hadoopPath)) Seq(hadoopPath.toString)
    else listParquetFiles(fs, hadoopPath)
  }

  private def listParquetFiles(fs: FileSystem, directory: HadoopPath): Seq[String] = {
    val files = scala.collection.mutable.ArrayBuffer.empty[String]
    val iterator = fs.listFiles(directory, false)

    while (iterator.hasNext) {
      val status = iterator.next()
      if (status.isFile && status.getPath.getName.endsWith(".parquet")) {
        files += status.getPath.toString
      }
    }

    files.sorted.toSeq
  }

  private def hasUriScheme(path: String): Boolean =
    path.matches("^[a-zA-Z][a-zA-Z0-9+.-]*://.*")

  private def rowDoubleOrNaN(row: Row, index: Int): Double =
    if (row.isNullAt(index)) Double.NaN else row.getDouble(index)

  private def castOrNull(df: DataFrame, columnName: String, dataType: DataType): Column =
    if (df.columns.contains(columnName)) col(columnName).cast(dataType)
    else lit(null).cast(dataType)

  private case class ColumnSpec(name: String, dataType: DataType)
  private case class ColumnStats(columnName: String, mean: Double, median: Double, mode: Double)
}
