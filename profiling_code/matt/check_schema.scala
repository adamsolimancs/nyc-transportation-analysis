import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("SchemaCheck").getOrCreate()

val dataPath = "hdfs:///user/mc9967_nyu_edu/final_project_data/merged_data"

val df = spark.read.parquet(dataPath)

df.printSchema()

df.show(5)
