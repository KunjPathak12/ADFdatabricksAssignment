# Databricks notebook source
# DBTITLE 1,mounting the blobStorage
dbutils.fs.mount(
  source = "wasbs://stagingdata@kunjlearningazure.blob.core.windows.net",
  mount_point = "/mnt/stagingdata",
  extra_configs = {"fs.azure.account.key.kunjlearningazure.blob.core.windows.net":"dunQxSeBrh6vYxOygar9SmA7JyYF15ipC6JWTYKN5Yeps8k5ORBkXecMyARmut1JAkmwWp89AnSe+AStFHPQFA=="})

# COMMAND ----------

# DBTITLE 1,creating the dataframe
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = StructType([StructField('datasets', ArrayType(StructType([StructField('customerId', StringType(), True), StructField('orderDate', StringType(), True), StructField('orderDetails', ArrayType(StructType([StructField('productId', StringType(), True), StructField('quantity', LongType(), True), StructField('sequence', LongType(), True), StructField('totalPrice', StructType([StructField('gross', LongType(), True), StructField('net', LongType(), True), StructField('tax', LongType(), True)]), True)]), True), True), StructField('orderId', StringType(), True), StructField('shipmentDetails', StructType([StructField('city', StringType(), True), StructField('country', StringType(), True), StructField('postalCode', StringType(), True), StructField('state', StringType(), True), StructField('street', StringType(), True)]), True)]), True), True), StructField('filename', StringType(), True)])

dfInit = spark.read.option("inferSchema", "true").option("multiline", "true").option("mode", "PERMISSIVE").schema(schema).json("/mnt/stagingdata/bronzeLayer/data1.json")
dfInit.printSchema()
dfInit.show(3)

# COMMAND ----------

# DBTITLE 1,Exploding the first orderDetails Column
df = dfInit.withColumn("orderDetail", explode(dfInit.datasets))
df.show(1)

# COMMAND ----------

# DBTITLE 1,Getting data for each remaining columns from the exploded column
df = df.withColumn("customerID", col("orderDetail").getItem("customerId"))\
     .withColumn("orderDate", col("orderDetail").getItem("orderDate")).withColumn("orderID", col("orderDetail").getItem("orderId")).drop("datasets")
df.show(1)
df.select(df.orderDetail).show(truncate = False)


# COMMAND ----------

# DBTITLE 1,Getting data for each remaining columns from the exploded column
df = df.withColumn("shipmentDetailsTrunc", col("orderDetail").getItem("shipmentDetails"))
df.show(1)

df = df.withColumn("country", df.shipmentDetailsTrunc.getItem("country"))\
    .withColumn("postalCode", df.shipmentDetailsTrunc.getItem("postalCode"))\
    .withColumn("state", df.shipmentDetailsTrunc.getItem("state"))\
    .withColumn("street", df.shipmentDetailsTrunc.getItem("street"))\
    .drop("shipmentDetailsTrunc")
df.show()

# COMMAND ----------

# DBTITLE 1,Getting data for each remaining columns from the exploded column and exploded the remaining array
df = df.withColumn("productDetails", df.orderDetail.getItem("orderDetails"))
df = df.withColumn("productId", df.productDetails.getItem("productId"))\
    .withColumn("quantity", df.productDetails.getItem("quantity"))\
    .withColumn("sequence", df.productDetails.getItem("sequence"))\
    .withColumn("price", df.productDetails.getItem("totalPrice"))\
    .drop("productDetails","orderDetail")
df = df.withColumn("productId",explode("productId"))
df.show(1)

# COMMAND ----------

# DBTITLE 1,getting data through exploding the remaining arrays in the schema
df = df.withColumn("quantity",explode("quantity"))\
    .withColumn("sequence",explode("sequence"))
df.show(5)

# COMMAND ----------

df = df.withColumn("grossP", df.price.getItem("gross"))\
    .withColumn("netP", df.price.getItem("net"))\
    .withColumn("taxP", df.price.getItem("tax"))
df.show(1)

# COMMAND ----------

df = df.withColumn("grossP", explode("grossP"))\
    .withColumn("netP", explode("netP"))\
    .withColumn("taxP", explode("taxP"))\
    .drop("price")
# df.show(5)

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

# DBTITLE 1,Giving the serial number by increasing function for not null value used as primary key.
df = df.withColumn("sr_no", row_number().over(Window.orderBy(monotonically_increasing_id())))

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

# DBTITLE 1,Merge column functions
from delta import *
def save_to_delta_with_overwrite(resultDf, path, db_name, table_name, mergeCol):
    base_path = path + f"{db_name}/{table_name}"
    if not DeltaTable.isDeltaTable(spark, f"{base_path}"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        resultDf.write.mode("overwrite") \
        .format("delta") \
        .option("path", base_path) \
        .save()
    else:
        deltaTable = DeltaTable.forPath(spark, f"{base_path}")
        matchKeys = " AND ".join("old." + col + " = new." + col for col in mergeCol)
        deltaTable.alias("old") \
        .merge(resultDf.alias("new"), matchKeys) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

# COMMAND ----------

# DBTITLE 1,display the splitted data
newDF = df.select("sr_no","orderDate","orderID","quantity","sequence","grossP","netP","taxP")
newDF.limit(5).display()

# COMMAND ----------

df = df.drop("orderDate","orderID","quantity","sequence","grossP","netP","taxP")
df.limit(5).display()

# COMMAND ----------

# DBTITLE 1,saving it to silver layer
df.write.mode("overwrite").format("delta").option("mergeSchema", "true").save("dbfs:/mnt/stagingdata/silverLayer/delta")
newDF.write.mode("overwrite").format("delta").option("mergeSchema", "true").save("dbfs:/mnt/stagingdata/silverLayer/delta")

# COMMAND ----------

df.display()

# COMMAND ----------

# DBTITLE 1,calling the merge function
save_to_delta_with_overwrite(df, "/mnt/stagingdata/silverLayer/delta", "staging", "customerData", ["sr_no"])
save_to_delta_with_overwrite(newDF, "/mnt/stagingdata/silverLayer/delta", "staging", "orderData", ["sr_no"])

# COMMAND ----------

# MAGIC %fs ls /mnt/stagingdata/silverLayer/delta
