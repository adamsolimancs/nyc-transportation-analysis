import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("MergeAll").enableHiveSupport().getOrCreate()
import spark.implicits._

val taxi = spark.read.parquet("hdfs:///user/aes10130_nyu_edu/final_project/processed_taxi")
val citibike = spark.read.parquet("hdfs:///user/aes10130_nyu_edu/final_project/processed_citibike_data")
val weather = spark.read.parquet("hdfs:///user/aj3556_nyu_edu/final_project/processed_weather")

println(s"Taxi records: ${taxi.count()}")
println(s"Citibike records: ${citibike.count()}")
println(s"Weather records: ${weather.count()}")

// check schemas before joining
taxi.printSchema()
citibike.printSchema()
weather.printSchema()

val trips = taxi.union(citibike) // join taxi + citibike
println(s"Combined trips records: ${trips.count()}")

// join on date and time_hours
val merged_data = trips.join(weather, Seq("date", "time_hours"), "left")
println(s"Merged records: ${merged_data.count()}")

merged_data.show(20, false) // show dataset

merged_data.coalesce(1).write.option("header", "true").mode("overwrite").parquet("hdfs:///user/aes10130_nyu_edu/final_project/merged_data")

spark.sql("DROP TABLE IF EXISTS aj3556_nyu_edu.merged_cleaned_2019")

merged_data.coalesce(1).write.option("header", "true").mode("overwrite").parquet("hdfs:///user/aj3556_nyu_edu/hw8_dir")
merged_data.write.mode("overwrite").saveAsTable("aj3556_nyu_edu.merged_cleaned_2019")

merged_data.coalesce(1).write.option("header", "true").mode("overwrite").parquet("hdfs:///user/aes10130_nyu_edu/final_project/merged_data")