import scala.io.Source
import scala.util.Try

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, date_format, hour, lit}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StringType, TimestampType}

object taxi_ingest {
  private val DefaultHdfsUri = "hdfs://nyu-dataproc-m"
  private val DefaultUser = System.getProperty("user.name")
  private val DefaultInputPath = "/user/aes10130_nyu_edu/final_project/yellow_taxi_raw"
  private val DefaultOutputPath = s"$DefaultHdfsUri/user/$DefaultUser/final_project/processed_taxi"
  private val DefaultLookupPath = s"$DefaultHdfsUri/user/$DefaultUser/final_project/taxi_zone_lookup.csv"
  private val DefaultPreviewRows = 0

  private val SourceColumns = Seq(
    ColumnSpec("tpep_pickup_datetime", TimestampType),
    ColumnSpec("tpep_dropoff_datetime", TimestampType),
    ColumnSpec("trip_distance", DoubleType),
    ColumnSpec("PULocationID", LongType)
  )

  private val OutputColumns = Seq(
    // use taxi_zone_lookup.csv longitude/latitude for the representative taxi-zone point
    ColumnSpec("start_longitude", DoubleType),
    ColumnSpec("start_latitude", DoubleType),
    // time_hours should be an int, 0-23
    ColumnSpec("time_hours", IntegerType),
    // date format: 2019-01-01
    ColumnSpec("date", StringType),
    // tpep_dropoff_datetime - tpep_pickup_datetime, in seconds (/ 1000)
    ColumnSpec("trip_duration", LongType),
    // keep in miles
    ColumnSpec("distance", DoubleType),
    // 1 for all
    ColumnSpec("is_taxi", IntegerType)
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("TaxiIngest")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val inputPath = if (args.nonEmpty) args(0) else DefaultInputPath
    val outputPath = if (args.length > 1) args(1) else DefaultOutputPath
    val lookupPath = if (args.length > 2) args(2) else DefaultLookupPath
    val previewRows = parsePreviewRows(args)

    val rawDf = readSourceColumns(spark, inputPath)
    val lookupDf = buildZoneLookup(spark, lookupPath)
    val processedDf = projectOutput(rawDf, lookupDf)

    println(s"Input path: $inputPath")
    println(s"Lookup path: $lookupPath")
    println(s"Output path: $outputPath")
    println(s"Records processed: ${processedDf.count()}")

    if (previewRows > 0) {
      println()
      println(s"Preview ($previewRows rows):")
      processedDf.show(previewRows, truncate = false)
    }

    processedDf
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(outputPath)

    spark.stop()
  }

  private def readSourceColumns(spark: SparkSession, inputPath: String): DataFrame =
    selectColumns(spark.read.parquet(inputPath), SourceColumns)

  private def buildZoneLookup(spark: SparkSession, lookupPath: String): DataFrame = {
    import spark.implicits._

    val lookupSource =
      if (hasUriScheme(lookupPath)) {
        spark.read
          .option("header", "true")
          .csv(lookupPath)
      } else {
        val source = Source.fromFile(lookupPath)

        try {
          val lines = source.getLines()
          val header = if (lines.hasNext) parseCsvLine(lines.next()) else Nil

          lines
            .flatMap { line =>
              val rowValues = parseCsvLine(line)
              if (header.nonEmpty && rowValues.length == header.length) {
                val rowMap = header.zip(rowValues).toMap
                Some(
                  LookupCsvRow(
                    locationID = rowMap.getOrElse("LocationID", ""),
                    longitude = rowMap.getOrElse("longitude", ""),
                    latitude = rowMap.getOrElse("latitude", ""),
                    x = rowMap.getOrElse("x", ""),
                    y = rowMap.getOrElse("y", "")
                  )
                )
              } else {
                None
              }
            }
            .toSeq
            .toDF()
        } finally {
          source.close()
        }
      }

    lookupSource.select(
      coalesce(optionalStringColumn(lookupSource, "LocationID"), optionalStringColumn(lookupSource, "locationID"))
        .cast(LongType)
        .as("PULocationID"),
      coalesce(optionalStringColumn(lookupSource, "longitude"), optionalStringColumn(lookupSource, "x"))
        .cast(DoubleType)
        .as("lookup_longitude"),
      coalesce(optionalStringColumn(lookupSource, "latitude"), optionalStringColumn(lookupSource, "y"))
        .cast(DoubleType)
        .as("lookup_latitude")
    )
  }

  private def projectOutput(rawDf: DataFrame, lookupDf: DataFrame): DataFrame = {
    val derivedDf = rawDf
      .join(lookupDf, Seq("PULocationID"), "left")
      .withColumn("start_longitude", col("lookup_longitude"))
      .withColumn("start_latitude", col("lookup_latitude"))
      .withColumn("time_hours", hour(col("tpep_pickup_datetime")))
      .withColumn("date", date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd"))
      .withColumn(
        "trip_duration",
        col("tpep_dropoff_datetime").cast(LongType) - col("tpep_pickup_datetime").cast(LongType)
      )
      .withColumn("distance", col("trip_distance"))
      .withColumn("is_taxi", lit(1))

    selectColumns(derivedDf, OutputColumns)
  }

  private def selectColumns(df: DataFrame, specs: Seq[ColumnSpec]): DataFrame =
    df.select(specs.map(spec => castOrNull(df, spec.name, spec.dataType).as(spec.name)): _*)

  private def castOrNull(df: DataFrame, columnName: String, dataType: DataType): Column =
    if (df.columns.contains(columnName)) col(columnName).cast(dataType)
    else lit(null).cast(dataType)

  private def optionalStringColumn(df: DataFrame, columnName: String): Column =
    if (df.columns.contains(columnName)) col(columnName)
    else lit(null).cast(StringType)

  private def parsePreviewRows(args: Array[String]): Int =
    if (args.length > 3) Try(args(3).toInt).toOption.filter(_ >= 0).getOrElse(DefaultPreviewRows)
    else DefaultPreviewRows

  private def hasUriScheme(path: String): Boolean =
    path.matches("^[a-zA-Z][a-zA-Z0-9+.-]*://.*")

  private def parseCsvLine(line: String): List[String] = {
    val fields = scala.collection.mutable.ListBuffer.empty[String]
    val current = new StringBuilder
    var inQuotes = false
    var index = 0

    while (index < line.length) {
      val char = line.charAt(index)

      if (char == '"') {
        val nextIsEscapedQuote = index + 1 < line.length && line.charAt(index + 1) == '"'
        if (inQuotes && nextIsEscapedQuote) {
          current.append('"')
          index += 1
        } else {
          inQuotes = !inQuotes
        }
      } else if (char == ',' && !inQuotes) {
        fields += current.toString()
        current.clear()
      } else {
        current.append(char)
      }

      index += 1
    }

    fields += current.toString()
    fields.toList
  }

  private case class LookupCsvRow(locationID: String, longitude: String, latitude: String, x: String, y: String)
  private case class ColumnSpec(name: String, dataType: DataType)
}
  
