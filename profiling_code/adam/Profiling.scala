import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc}

object Profiling {
  private val DefaultPath = "hdfs:///user/aes10130_nyu_edu/final_project/merged_data"
  private val TempViewName = "merged_data_profile_view"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MergedDataProfiling")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val inputPath = if (args.nonEmpty) args(0) else DefaultPath
    val df = spark.read.parquet(inputPath).cache()
    df.createOrReplaceTempView(TempViewName)

    println(s"Input path: $inputPath")
    println(s"Total rows: ${df.count()}")
    println(s"Total columns: ${df.columns.length}")
    println()

    println("Schema:")
    df.printSchema()
    println()

    println("Sample rows:")
    df.show(10, truncate = false)
    println()

    println("Null counts by column:")
    val nullCountQuery = df.columns
      .map(name => s"sum(case when ${quoted(name)} is null then 1 else 0 end) as ${quoted(name)}")
      .mkString(", ")
    spark.sql(s"select $nullCountQuery from $TempViewName").show(false)
    println()

    val numericColumns = Seq(
      "trip_duration",
      "distance",
      "start_latitude",
      "start_longitude",
      "time_hours",
      "temp_f",
      "precipitation_mm",
      "rain_mm",
      "cloudcover_pct",
      "windspeed_kmh",
      "wind_direction_deg"
    ).filter(df.columns.contains)

    if (numericColumns.nonEmpty) {
      println("Summary statistics for numeric columns:")
      val numericColumnQuery = numericColumns.map(quoted).mkString(", ")
      spark.sql(s"select $numericColumnQuery from $TempViewName").describe().show(false)
      println()
    }

    if (df.columns.contains("is_taxi")) {
      println("Counts by transport type:")
      df.groupBy("is_taxi").count().orderBy("is_taxi").show(false)
      println()
    }

    if (df.columns.contains("time_hours")) {
      println("Trip counts by the hour of the day:")
      df.groupBy("time_hours").count().orderBy("time_hours").show(24, false)
      println()
    }

    if (df.columns.contains("date")) {
      println("Top 10 dates by row count:")
      df.groupBy("date").count().orderBy(desc("count"), col("date")).show(10, false)
      println()
    }

    val aggregateColumns = Seq("trip_duration", "distance", "temp_f").filter(df.columns.contains)
    if (aggregateColumns.nonEmpty) {
      println("Basic min / max / average checks:")

      val aggregateQuery = aggregateColumns.flatMap { name =>
        Seq(
          s"min(${quoted(name)}) as ${quoted(s"${name}_min")}",
          s"max(${quoted(name)}) as ${quoted(s"${name}_max")}",
          s"round(avg(${quoted(name)}), 2) as ${quoted(s"${name}_avg")}"
        )
      }.mkString(", ")

      spark.sql(s"select $aggregateQuery from $TempViewName").show(false)
      println()
    }

    df.unpersist()
    spark.stop()
  }

  private def quoted(name: String): String =
    s"`${name.replace("`", "``")}`"
}
