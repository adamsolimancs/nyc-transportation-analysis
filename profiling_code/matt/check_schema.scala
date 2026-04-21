import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("SchemaCheck").getOrCreate()

val dataPath = "hdfs:///user/mc9967_nyu_edu/final_project/*.csv"

val df = spark.read.option("header", "true").option("inferSchema", "true").csv(dataPath)

df.printSchema()

df.show(5)
