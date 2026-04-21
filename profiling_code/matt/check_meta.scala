import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("MetaCheck").getOrCreate()
val df = spark.read.parquet("hdfs:///user/mc9967_nyu_edu/final_project_data/merged_data")

df.count()

val nullCounts = df.select(df.columns.map(c => count(when(col(c).isNull, c)).alias(c)): _*)
nullCounts.show()

val coordStats = df.select(
    min("start_latitude").alias("min_lat"),
    max("start_latitude").alias("max_lat"),
    min("start_longitude").alias("min_lon"),
    max("start_longitude").alias("max_lon")
)
coordStats.show()

df.describe().show()
df.printSchema()
