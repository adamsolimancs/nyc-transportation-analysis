import org.apache.spark.sql.functions.lit

val df = spark.read.option("header", "true").option("inferSchema", "true").csv("citibike.csv")
val dfDropped = df.drop("start_lat", "start_lng", "end_lat", "end_lng")
val dfCleaned = dfDropped.na.drop(Seq("ride_id", "start_station_name", "end_station_name"))
val dfFinal = dfCleaned.withColumn("data_year", lit(2024))

dfFinal.show()
println(dfFinal.count())
dfFinal.write.mode("overwrite").option("header", "true").csv("citibike_cleaned")