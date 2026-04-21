import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("MetaCheck").getOrCreate()
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs:///user/mc9967_nyu_edu/final_project/*.csv")

println(s"Total Records: ${df.count()}")

val nullCounts = df.select(df.columns.map(c => count(when(col(c).isNull, c)).alias(c)): _*)
nullCounts.show()

df.describe().show()
