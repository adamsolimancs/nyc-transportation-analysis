import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Merger {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FinalMergeNYC")
      .getOrCreate()

    import spark.implicits._

    val citibikeDF = spark.read.parquet("/user/mc9967_nyu_edu/output/processed_citibike_data")
    
    val taxiDF = spark.read.parquet("/user/aes10130_nyu_edu/final_project/processed_taxi")
    val weatherDF = spark.read.parquet("/user/aes10130_nyu_edu/final_project/processed_weather")

    val transportDF = citibikeDF.unionByName(taxiDF)

    val mergedDF = transportDF.join(weatherDF, Seq("date", "time_hours"), "inner")

    val finalCleanDF = mergedDF.na.drop().dropDuplicates()

    finalCleanDF.write.mode("overwrite")
      .parquet("/user/mc9967_nyu_edu/final_project/merged_data")
      
    spark.stop()
  }
}