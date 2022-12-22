from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Window

spark = SparkSession.builder.appName("session").getOrCreate()

df = (
    spark.read.parquet("input_data")
    .select("entity_id", "item_id", "month_id", "signal_count")
)

df = df.repartition(F.col("entity_id"))

windowSpecAgg = Window.partitionBy("entity_id")
result = (
    df
    .withColumn("row_number_oldest", 
                F.row_number().over(windowSpecAgg.orderBy("month_id", "item_id")))
    .withColumn("oldest_item_id", 
                F.when(F.col("row_number_oldest") == 1, F.col("item_id")).otherwise(None))
    .withColumn("row_number_newest", 
                F.row_number().over(windowSpecAgg.orderBy(F.desc("month_id"), "item_id")))
    .withColumn("newest_item_id", 
                F.when(F.col("row_number_newest") == 1, F.col("item_id")).otherwise(None))
    .groupBy("entity_id").agg(F.max("oldest_item_id").alias("oldest_item_id"),
                             F.max("newest_item_id").alias("newest_item_id"),
                             F.sum("signal_count").alias("total_signals"))
)
result.repartition(1).write.mode("overwrite").parquet("output_data/result")