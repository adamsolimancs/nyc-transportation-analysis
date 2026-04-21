import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Processer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CitiBikeProcessing")
      .getOrCreate()

    import spark.implicits._

    val rawData = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("hdfs:/user/mc9967_nyu_edu/final_project_data/*.csv")

    val cleanRawData = rawData.na.drop()

    val processedDF = cleanRawData
      .withColumnRenamed("start station longitude", "start_longitude")
      .withColumnRenamed("start station latitude", "start_latitude")
      
      .withColumn("trip_duration", col("tripduration").cast("double"))
      
      .withColumn("time_hours", hour(to_timestamp(col("starttime"))))
      .withColumn("date", to_date(col("starttime")))
      .withColumn("is_taxi", lit(0))
      .withColumn("distance", {

        val R = 6371.0
        val lat1 = radians(col("start_latitude"))
        val lat2 = radians(col("end station latitude"))
        val lon1 = radians(col("start_longitude"))
        val lon2 = radians(col("end station longitude"))
        
        val dLat = lat2 - lat1
        val dLon = lon2 - lon1
        
        val a = sin(dLat / 2) * sin(dLat / 2) +
                cos(lat1) * cos(lat2) *
                sin(dLon / 2) * sin(dLon / 2)
        
        val c = lit(2) * atan2(sqrt(a), sqrt(lit(1.0) - a))
        lit(R) * c
      })

    val finalDropDF = processedDF.select(
      "start_longitude",
      "start_latitude",
      "time_hours",
      "date",
      "trip_duration",
      "distance",
      "is_taxi"
    )

    val finalCleanDF = finalDropDF.na.drop()

    finalCleanDF.show(10)

    finalCleanDF.write.mode("overwrite")
      .parquet("hdfs:/user/mc9967_nyu_edu/output/processed_citibike_data")

    spark.stop()
  }
}