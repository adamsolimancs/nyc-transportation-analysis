val df = spark.read.option("header", "true").option("inferSchema", "true").csv("citibike.csv")
df.count()

val countRDD = df.rdd.map(row => ("total_records", 1)).reduceByKey(_ + _)
countRDD.collect().foreach(println)

val cols = df.columns
cols.foreach { c =>
  df.select(c).distinct().show(3)
}