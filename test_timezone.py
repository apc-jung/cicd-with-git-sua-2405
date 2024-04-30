# Databricks notebook source
from pyspark.sql.functions import col, from_utc_timestamp, unix_timestamp
from pyspark.sql.types import TimestampType



# COMMAND ----------

# サンプルデータフレームの作成
data = [("2024-02-21 12:00:00",), ("2024-02-22 12:00:00",)]
schema = ["timestamp"]
df = spark.createDataFrame(data, schema=schema)
df = df.select(col("timestamp").cast(TimestampType()).alias("timestamp"))
df.display()

# COMMAND ----------

# タイムゾーンをUTCからJST（日本時間）に変更
df_jst = df.withColumn("timestamp_jst", from_utc_timestamp("timestamp", "Asia/Tokyo"))
df_jst.display()

# COMMAND ----------

df_jst.withColumn("unixtime", unix_timestamp("timestamp")).withColumn("unixtime_jst", unix_timestamp("timestamp_jst")).display()

# COMMAND ----------

# Delta Tableとして保存
delta_path = "/tmp/delta_table"
df_jst.write.format("delta").save(delta_path)

# COMMAND ----------

# Delta Tableから読み込み
df_loaded = spark.read.format("delta").load(delta_path)

# 結果の表示
df_loaded.display()
