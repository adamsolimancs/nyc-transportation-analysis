import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, TimestampType}

object Clean {
  private val DefaultHdfsUri = "hdfs://nyu-dataproc-m"
  private val DefaultUser = System.getProperty("user.name")
  private val DefaultInputPath = s"$DefaultHdfsUri/user/$DefaultUser/yellow_taxi_input"
  private val DefaultOutputPath = s"$DefaultHdfsUri/user/$DefaultUser/yellow_taxi_cleaned"

  private val KeptColumns = Seq(
    ColumnSpec("tpep_pickup_datetime", TimestampType),
    ColumnSpec("tpep_dropoff_datetime", TimestampType),
    ColumnSpec("trip_distance", DoubleType),
    ColumnSpec("PULocationID", LongType),
    ColumnSpec("DOLocationID", LongType)
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CleanYellowTaxiData")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val inputPath = if (args.nonEmpty) args(0) else DefaultInputPath
    val outputPath = if (args.length > 1) args(1) else DefaultOutputPath
    val parquetPaths = resolveParquetPaths(spark, inputPath)

    require(parquetPaths.nonEmpty, s"No parquet files found at $inputPath")

    val cleanedDf = parquetPaths
      .map(path => selectKeptColumns(spark.read.parquet(path)))
      .reduce(_ union _)

    println(s"Input path: $inputPath")
    println(s"Output path: $outputPath")
    println(s"Records after cleaning: ${cleanedDf.count()}")

    cleanedDf.write
      .mode("overwrite")
      .parquet(outputPath)

    spark.stop()
  }

  private def selectKeptColumns(df: DataFrame): DataFrame =
    df.select(KeptColumns.map(spec => castOrNull(df, spec.name, spec.dataType).as(spec.name)): _*)

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

  private def castOrNull(df: DataFrame, columnName: String, dataType: DataType): Column =
    if (df.columns.contains(columnName)) col(columnName).cast(dataType)
    else lit(null).cast(dataType)

  private case class ColumnSpec(name: String, dataType: DataType)
}
