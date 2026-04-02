import org.apache.spark.sql.SparkSession

object CountRecs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("EDA")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // TODO: change dataset path when downloaded.
    val datasetPath =
      if (args.nonEmpty) args(0) else "yellow_taxi/mock_yellow_taxi.csv"

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(datasetPath)

    println(s"Dataset path: $datasetPath")
    println()

    val totalRecords = df.count()
    println(s"Total number of records using count(): $totalRecords")

    val recordCountFromMap = df.rdd
      .map(_ => ("record_count", 1L))
      .reduceByKey(_ + _)
      .collect()

    recordCountFromMap.foreach { case (key, value) =>
      println(s"Total number of records using map(): $key -> $value")
    }

    println()
    println("Distinct values in each column:")

    df.columns.foreach { columnName =>
      println(s"\nColumn: $columnName")

      val distinctValues = df
        .select(columnName)
        .distinct()
        .collect()
        .map(row => Option(row.get(0)).map(_.toString).getOrElse("null"))

      distinctValues.foreach(value => println(s"  $value"))
      println(s"Distinct count for $columnName: ${distinctValues.length}")
    }

    spark.stop()
  }
}
