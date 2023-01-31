# Databricks notebook source
df = spark.read.parquet("/mnt/stagingdata/silverLayer/deltastagingLayer/customerData/part-00000-3622be5f-f9ec-404e-9cf5-05e3d416c852-c000.snappy.parquet")

# COMMAND ----------

dfO = spark.read.parquet("/mnt/stagingdata/silverLayer/deltastagingLayer/orderData/part-00000-2adaa123-8ca6-4b38-b283-4c7fcf0a6ff0-c000.snappy.parquet")


# COMMAND ----------

df.show(5)
dfO.show(5)

# COMMAND ----------

dfJoin = df.join(dfO, df.sr_no==dfO.sr_no,"inner").drop("sr_no")

# COMMAND ----------

dfJoin.display()

# COMMAND ----------

dfJoin.write.mode("overwrite").format("delta").option("mergeSchema", "true").save("/mnt/stagingdata/goldLayer/cleanData1")

# COMMAND ----------

# MAGIC %fs ls /mnt/stagingdata/goldLayer/
