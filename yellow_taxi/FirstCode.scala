import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, col, count, date_format, lit, stddev, sum, when}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, TimestampType}

object FirstCode {
  private val DefaultHdfsUri = "hdfs://nyu-dataproc-m"
  private val DefaultUser = System.getProperty("user.name")
  private val DefaultInputPath = "/user/aes10130_nyu_edu/hw7/data"
  private val DefaultOutputPath = s"$DefaultHdfsUri/user/$DefaultUser/hw7_clean"
  private val TimestampPattern = "yyyy-MM-dd HH:mm:ss"
  private val DefaultPreviewRows = 0

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
  private val FrequentItemSupport = 1e-4
  private val QuantileRelativeError = 0.01
  private val MaxModeCandidatesPerColumn = 32

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("FirstCode")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val inputPath = if (args.nonEmpty) args(0) else DefaultInputPath
    val outputPath = if (args.length > 1) args(1) else DefaultOutputPath
    val previewRows = parsePreviewRows(args)

    val taxiDf = readRequiredColumns(spark, inputPath)
    val summary = computeDatasetSummary(taxiDf)
    val enrichedDf = addDerivedColumns(taxiDf)

    println(s"Input path: $inputPath")
    println(s"Output path: $outputPath")
    println(s"Records loaded: ${summary.recordCount}")
    println()
    println("Summary statistics:")
    summary.columnStats.foreach(printColumnStats)
    println(s"total_amount standard deviation = ${summary.totalAmountStdDev}")

    if (previewRows > 0) {
      println()
      println(s"Preview with normalized datetimes and trip_time_length ($previewRows rows):")
      enrichedDf.show(previewRows, truncate = false)
    }

    enrichedDf.write
      .mode("overwrite")
      .parquet(outputPath)

    spark.stop()
  }

  private def readRequiredColumns(spark: SparkSession, inputPath: String): DataFrame = {
    val readPaths = resolveReadPaths(spark, inputPath)
    require(readPaths.nonEmpty, s"No parquet files found at $inputPath")

    selectRequiredColumns(spark.read.parquet(readPaths: _*))
  }

  private def selectRequiredColumns(df: DataFrame): DataFrame =
    df.select(RequiredColumns.map(spec => castOrNull(df, spec.name, spec.dataType).as(spec.name)): _*)

  private def computeDatasetSummary(df: DataFrame): DatasetSummary = {
    val meanExpressions = NumericColumns.map(name => avg(col(name)).as(meanAlias(name)))
    val aggregateExpressions =
      Seq(count(lit(1)).as("record_count"), stddev(col("total_amount")).as("total_amount_stddev")) ++ meanExpressions
    val aggregateRow = df.agg(aggregateExpressions.head, aggregateExpressions.tail: _*).first()
    val medians = df.stat.approxQuantile(NumericColumns.toArray, Array(0.5), QuantileRelativeError)
    val modes = computeModes(df)

    val columnStats = NumericColumns.zipWithIndex.map { case (columnName, index) =>
      ColumnStats(
        columnName = columnName,
        mean = rowDoubleOrNaN(aggregateRow, meanAlias(columnName)),
        median = medians(index).headOption.getOrElse(Double.NaN),
        mode = modes.getOrElse(columnName, Double.NaN)
      )
    }

    DatasetSummary(
      recordCount = rowLongOrZero(aggregateRow, "record_count"),
      columnStats = columnStats,
      totalAmountStdDev = rowDoubleOrNaN(aggregateRow, "total_amount_stddev")
    )
  }

  private def computeModes(df: DataFrame): Map[String, Double] = {
    val frequentItemsRow = df.stat.freqItems(NumericColumns, FrequentItemSupport).first()

    val modeCandidates = NumericColumns.flatMap { columnName =>
      val fieldIndex = frequentItemsRow.fieldIndex(s"${columnName}_freqItems")
      frequentItemsRow.getSeq[Any](fieldIndex)
        .collect {
          case number: java.lang.Number if !java.lang.Double.isNaN(number.doubleValue()) =>
            number.doubleValue()
        }
        .distinct
        .take(MaxModeCandidatesPerColumn)
        .zipWithIndex
        .map { case (candidateValue, index) =>
          ModeCandidate(columnName, candidateValue, s"${columnName}_mode_candidate_$index")
        }
    }

    if (modeCandidates.isEmpty) {
      return Map.empty
    }

    val countExpressions = modeCandidates.map { candidate =>
      sum(when(col(candidate.columnName) === lit(candidate.candidateValue), 1L).otherwise(0L))
        .cast(LongType)
        .as(candidate.alias)
    }
    val countRow = df.agg(countExpressions.head, countExpressions.tail: _*).first()

    modeCandidates
      .groupBy(_.columnName)
      .flatMap { case (columnName, candidates) =>
        candidates
          .map(candidate => candidate -> rowLongOrZero(countRow, candidate.alias))
          .filter { case (_, candidateCount) => candidateCount > 0L }
          .sortBy { case (candidate, candidateCount) => (-candidateCount, candidate.candidateValue) }
          .headOption
          .map { case (candidate, _) => columnName -> candidate.candidateValue }
      }
      .toMap
  }

  private def addDerivedColumns(df: DataFrame): DataFrame =
    df.na.drop(Seq("tpep_pickup_datetime", "tpep_dropoff_datetime"))
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

  private def resolveReadPaths(spark: SparkSession, datasetPath: String): Seq[String] = {
    if (!hasUriScheme(datasetPath)) {
      val localPath = Paths.get(datasetPath)

      if (Files.isRegularFile(localPath) && datasetPath.endsWith(".parquet")) {
        return Seq(localPath.toString)
      }

      if (Files.isDirectory(localPath)) {
        return listLocalParquetFiles(localPath)
      }
    }

    val hadoopPath = new HadoopPath(datasetPath)
    val fs = hadoopPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

    require(fs.exists(hadoopPath), s"Dataset path not found: $datasetPath")

    if (fs.isFile(hadoopPath)) Seq(hadoopPath.toString)
    else Seq(hadoopPath.toString)
  }

  private def listLocalParquetFiles(directory: java.nio.file.Path): Seq[String] = {
    val stream = Files.list(directory)

    try {
      stream.iterator().asScala
        .filter(entry => Files.isRegularFile(entry))
        .filter(entry => entry.getFileName.toString.endsWith(".parquet"))
        .map(_.toString)
        .toSeq
        .sorted
    } finally {
      stream.close()
    }
  }

  private def hasUriScheme(path: String): Boolean =
    path.matches("^[a-zA-Z][a-zA-Z0-9+.-]*://.*")

  private def meanAlias(columnName: String): String =
    s"${columnName}_mean"

  private def rowDoubleOrNaN(row: Row, fieldName: String): Double = {
    val index = row.fieldIndex(fieldName)
    if (row.isNullAt(index)) Double.NaN else row.getDouble(index)
  }

  private def rowLongOrZero(row: Row, fieldName: String): Long = {
    val index = row.fieldIndex(fieldName)
    if (row.isNullAt(index)) 0L else row.getLong(index)
  }

  private def parsePreviewRows(args: Array[String]): Int =
    if (args.length > 2) Try(args(2).toInt).toOption.filter(_ >= 0).getOrElse(DefaultPreviewRows)
    else DefaultPreviewRows

  private def castOrNull(df: DataFrame, columnName: String, dataType: DataType): Column =
    if (df.columns.contains(columnName)) col(columnName).cast(dataType)
    else lit(null).cast(dataType)

  private case class ModeCandidate(columnName: String, candidateValue: Double, alias: String)
  private case class ColumnSpec(name: String, dataType: DataType)
  private case class ColumnStats(columnName: String, mean: Double, median: Double, mode: Double)
  private case class DatasetSummary(recordCount: Long, columnStats: Seq[ColumnStats], totalAmountStdDev: Double)
}
