import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.Pipeline

object ModelTrainer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC_Transport_Model")
      .getOrCreate()

    val rawData = spark.read.parquet("/user/aes10130_nyu_edu/final_project/merged_data")
    
    val data = rawData.na.drop()

    val seasonIndexer = new StringIndexer()
      .setInputCol("season")
      .setOutputCol("season_indexed")
      .setHandleInvalid("skip")

    val beaufortIndexer = new StringIndexer()
      .setInputCol("beaufort_scale")
      .setOutputCol("beaufort_indexed")
      .setHandleInvalid("skip")

    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "start_longitude", "start_latitude", "distance", 
        "time_hours", "temp_f", "precipitation_mm", 
        "season_indexed", "beaufort_indexed"
      ))
      .setOutputCol("features")
      .setHandleInvalid("skip")

    val rf = new RandomForestClassifier()
      .setLabelCol("is_taxi")
      .setFeaturesCol("features")
      .setNumTrees(100) 

    val pipeline = new Pipeline()
      .setStages(Array(seasonIndexer, beaufortIndexer, assembler, rf))

    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2), seed = 12345)

    println("Starting training")
    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testData)
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("is_taxi")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Accuracy: ${accuracy * 100}%")

    model.write.overwrite().save("/user/mc9967_nyu_edu/final_project/transport_model")

    spark.stop()
  }
}