import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, TimestampType}

object CountRecs {
  private val DefaultHdfsUri = "hdfs://nyu-dataproc-m"
  private val DefaultUser = System.getProperty("user.name")
  private val DefaultInputPath = s"$DefaultHdfsUri/user/$DefaultUser/hw6/data/"
  private val MaxNonCategoricalDistinctValues = 10

  private val DistinctColumns = Seq(
    DistinctColumn("tpep_pickup_datetime", TimestampType, isCategorical = false),
    DistinctColumn("tpep_dropoff_datetime", TimestampType, isCategorical = false),
    DistinctColumn("trip_distance", DoubleType, isCategorical = false),
    DistinctColumn("PULocationID", LongType, isCategorical = true),
    DistinctColumn("DOLocationID", LongType, isCategorical = true)
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CountYellowTaxiRecords")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val datasetPath = if (args.nonEmpty) args(0) else DefaultInputPath
    val parquetPaths = resolveParquetPaths(spark, datasetPath)

    require(parquetPaths.nonEmpty, s"No parquet files found at $datasetPath")

    println(s"Dataset path: $datasetPath")

    val totalRecords = countRecordsWithMap(spark, parquetPaths)
    println(s"Total number of records using map(): $totalRecords")

    println()
    println("Distinct values for requested columns:")
    DistinctColumns.foreach(spec => showDistinctValues(spark, parquetPaths, spec))

    spark.stop()
  }

  private def countRecordsWithMap(spark: SparkSession, parquetPaths: Seq[String]): Long =
    parquetPaths.foldLeft(0L) { (runningTotal, path) =>
      val fileCount = spark.read.parquet(path).rdd
        .map(_ => ("record_count", 1L))
        .reduceByKey(_ + _)
        .values
        .collect()
        .headOption
        .getOrElse(0L)

      runningTotal + fileCount
    }

  private def showDistinctValues(spark: SparkSession, parquetPaths: Seq[String], spec: DistinctColumn): Unit = {
    val distinctDf = parquetPaths
      .map(path => readColumn(spark, path, spec))
      .reduce(_ union _)
      .distinct()
      .orderBy(col(spec.name))

    val values =
      if (spec.isCategorical) distinctDf.collect()
      else distinctDf.limit(MaxNonCategoricalDistinctValues + 1).collect()

    val displayedValues =
      if (spec.isCategorical) values
      else values.take(MaxNonCategoricalDistinctValues)

    println()
    println(s"Column: ${spec.name}")
    displayedValues.foreach(row => println(s"  ${row.get(0)}"))

    if (!spec.isCategorical && values.length > MaxNonCategoricalDistinctValues) {
      println(s"  ... showing at most $MaxNonCategoricalDistinctValues distinct values")
    }
  }

  private def readColumn(spark: SparkSession, path: String, spec: DistinctColumn): DataFrame =
    spark.read.parquet(path)
      .select(col(spec.name).cast(spec.dataType).as(spec.name))
      .where(col(spec.name).isNotNull)

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

  private case class DistinctColumn(name: String, dataType: DataType, isCategorical: Boolean)
}
