import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.Pipeline

object ModelTrainer2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC_Transport_Model_Balanced")
      .getOrCreate()

    import spark.implicits._

    val rawData = spark.read.parquet("/user/aes10130_nyu_edu/final_project/merged_data")
    val cleanData = rawData.na.drop()

    val taxis = cleanData.filter($"is_taxi" === 1)
    val bikes = cleanData.filter($"is_taxi" === 0)

    val taxiCount = taxis.count().toDouble
    val bikeCount = bikes.count().toDouble
    val fraction = bikeCount / taxiCount

    val balancedTaxis = taxis.sample(withReplacement = false, fraction, seed = 12345)
    val data = bikes.union(balancedTaxis)

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
      .setNumTrees(40)
      .setMaxDepth(10)
      .setMaxBins(64)

    val pipeline = new Pipeline()
      .setStages(Array(seasonIndexer, beaufortIndexer, assembler, rf))

    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2), seed = 12345)

    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testData)

    val accEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("is_taxi")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val f1Evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("is_taxi")
      .setPredictionCol("prediction")
      .setMetricName("f1")

    val accuracy = accEvaluator.evaluate(predictions)
    val f1Score = f1Evaluator.evaluate(predictions)

    println(s"Accuracy: ${accuracy * 100}%")
    println(s"F1 Score: $f1Score")

    println("Confusion Matrix:")
    predictions.groupBy("is_taxi", "prediction").count().show()

    model.write.overwrite().save("/user/mc9967_nyu_edu/final_project/transport_model_balanced")

    spark.stop()
  }
}