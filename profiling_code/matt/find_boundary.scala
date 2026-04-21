import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("CoordinateProfiling").getOrCreate()
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs:///user/mc9967_nyu_edu/final_project_data/merged_data")

val coordStats = df.select(
    min("start_latitude").alias("min_lat"),
    max("start_latitude").alias("max_lat"),
    min("start_longitude").alias("min_lon"),
    max("start_longitude").alias("max_lon")
)

coordStats.show()
