# Analysis of NYC Transportation Data

## Input data: 

https://data.cityofnewyork.us/dataset/Citi-Bike-System-Data/vsnr-94wk/about_data

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

https://open-meteo.com/en/docs

## HDFS File Locations: 

hdfs dfs -ls -d /user/aes10130_nyu_edu/final_project

## Describe your directories and files, step by step: 

- `data_ingest/` contains the ingestion jobs used to prepare source datasets before merging.
- `etl_code/` contains cleaning and merge jobs for the combined transportation + weather dataset.
- `ana_code/` contains analytic (model training and visualization) code used after cleaning.
- `profiling_code/` contains record counts, schema checks, and other quick validation scripts.
- `screenshots/` stores project screenshots by team member for different steps of the pipeline.
- `weather/`, `bike/`, and `yellow_taxi/` contain earlier dataset-specific cleaning and profiling code from previous hw's.

## How to build the code: 

This repo does not have a single root build file. Most jobs are standalone Spark Scala programs.

To build one Spark job manually:

1. Make a directory for compiled `.class` files.
2. Compile the Scala file with the Spark jars on the classpath.
3. Package the compiled classes into a jar.

Example for `etl_code/adam/Clean_Merged.scala`:

```bash
mkdir -p etl_code/adam/clean_merged_class_files

scalac \
  -classpath "$SPARK_HOME/jars/*" \
  -d etl_code/adam/clean_merged_class_files \
  etl_code/adam/Clean_Merged.scala

jar cf etl_code/adam/Clean_Merged_classes.jar \
  -C etl_code/adam/clean_merged_class_files .
```

Example for `data_ingest/adam/taxi_ingest.scala`:

```bash
mkdir -p data_ingest/adam/ingest_class_files

scalac \
  -classpath "$SPARK_HOME/jars/*" \
  -d data_ingest/adam/ingest_class_files \
  data_ingest/adam/taxi_ingest.scala

jar cf data_ingest/adam/taxi_ingest_classes.jar \
  -C data_ingest/adam/ingest_class_files .
```

## How to run the code: 

To run one Spark job manually after building the jar:

```bash
spark-submit \
  --class Clean_Merged \
  etl_code/adam/Clean_Merged_classes.jar \
  hdfs:///user/aes10130_nyu_edu/final_project/merged_data \
  hdfs:///user/aes10130_nyu_edu/final_project/merged_data \
  hdfs:///user/aes10130_nyu_edu/final_project/merged_data_tmp
```

```bash
spark-submit \
  --class taxi_ingest \
  data_ingest/adam/taxi_ingest_classes.jar \
  /user/aes10130_nyu_edu/final_project/yellow_taxi_raw \
  hdfs:///user/aes10130_nyu_edu/final_project/processed_taxi \
  hdfs:///user/aes10130_nyu_edu/final_project/taxi_zone_lookup.csv \
  0
```


## Where to find results of a run: 

- Taxi ingest output defaults to `hdfs:///user/aes10130_nyu_edu/final_project/processed_taxi`.
- Bike ingest output defaults to `hdfs:///user/aes10130_nyu_edu/final_project/processed_citibike_data`.
- Weather ingest output defaults to `hdfs:///user/aes10130_nyu_edu/final_project/processed_weather`.
- The merged cleaned dataset is written to `hdfs:///user/aes10130_nyu_edu/final_project/merged_data`.
- Intermediate and analysis outputs are generally written to user-specific HDFS folders referenced inside each Scala file.

## Where you can find the input data that you used: 
- Taxi input data can be found at: `hdfs:///user/aes10130_nyu_edu/final_project/yellow_taxi_raw`.
- Bike input data can be found at: 
- Weather input data can be found at:

## How to build/recreate our model: 
- 

## How to run our model: 
- Download the transport_model_local folder and model_viz.ipynb, which are located in the ana_code directory
- Open model_viz.ipynb and ensure that the model_path variable points to your local transport_model_local path
- Run the notebook and select two points on the map to receive a prediction
- To test different conditions, locate the handle_click function and modify time_hours, temp_f, season, beaufort_scale, and precipitation_mm in the test_row variable 

Alternatively, to run the model without a visualization, 
- Download the transport_model_local folder located in the ana_code directory
- For Spark, load and run the model using the following code:
```bash
import org.apache.spark.ml.PipelineModel
val model = PipelineModel.load("transport_model_local")
val predictions = model.transform(inputDF)
predictions.select("prediction").show()
```
- For Python, load and run the model using the following code:
- ```bash
from pyspark.ml import PipelineModel
model = PipelineModel.load("transport_model_local")
predictions = model.transform(inputDF)
predictions.select("prediction").show()
```
