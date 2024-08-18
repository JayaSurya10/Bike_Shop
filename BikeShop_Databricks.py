# Databricks notebook source
# configs = {
 #  "fs.adl.oauth2.access.token.provider.type": "CustomAccessTokenProvider",
 #  "fs.adl.oauth2.access.token.custom.provider": spark.conf.get("spark.databricks.passthrough.adls.tokenProviderClassName")
 #}

#dbutils.fs.mount(
 #  source = "adl://bronze@bikestorestorageaccount.blob.core.windows.net/",
  # mount_point = "/mnt/bronze",
   #extra_configs = configs)

# COMMAND ----------

#dbutils.fs.mounts()


# COMMAND ----------

dbutils.fs.unmount("/mnt/bronze")

# COMMAND ----------

dbutils.fs.mount(
source = "wasbs://bronze@bikestorestorageaccount.blob.core.windows.net/",
mount_point = "/mnt/bronze",
extra_configs = {"fs.azure.account.key.bikestorestorageaccount.blob.core.windows.net": "cHB2CMV2MPMOIOqpWklm07YQgQJznnX1jEutSmDxX7aVVYr886Q4MEhXUSiQ7GL4jjupogi1Gq/u+AStr7sWvA=="}
)


# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

display(dbutils.fs.ls("/mnt/bronze"))

# COMMAND ----------

df = spark.read.csv("/mnt/bronze/production/brands/brands.txt",header = True)
df.show()


# COMMAND ----------

from pyspark.sql.types import *


input_format = '/mnt/bronze/production/products/'
df = spark.read.csv(input_format,header=True)
display(df)

# COMMAND ----------

input_path = '/mnt/bronze/production/staffs/'
df = spark.read.format('csv').options(header=True).load(input_path)
df.show()

# COMMAND ----------

table_name = []
for i in dbutils.fs.ls('mnt/bronze/production/'):
    table_name.append(i.name.split('/')[0])



# COMMAND ----------

table_name


# COMMAND ----------

from pyspark.sql.functions import col, round

for i in table_name:
    path = f'/mnt/bronze/production/{i}/{i}.txt'
    df = spark.read.format('csv').options(header=True).load(path)
    column = df.columns
    for col_name in column:
        if 'Price' in col_name or 'price' in col_name:
            df = df.withColumn(col_name, round(col(col_name)))
    output_path = f'/mnt/sliver1/production/{i}/'
    df.write.format('delta').mode('overwrite').save(output_path)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

for i in table_name:
    path = f'/mnt/sliver1/production/{i}'
    df = spark.read.format('delta').options(header=True).load(path)
    
    for col_name in df.columns:
        if 'Phone' in col_name or 'phone' in col_name:
            letter_to_remove = r"['(',')',' ']"
            df = df.withColumn(col_name, regexp_replace(col_name,letter_to_remove, ""))
    output_path = f'/mnt/gold1/production/{i}/{i}'
    df.write.format('delta').mode('overwrite').save(output_path) 
    df.show()

# COMMAND ----------

dbutils.fs.ls('/mnt/gold1/production/')

# COMMAND ----------

path = '/mnt/gold1/production/orders/orders/'
df = spark.read.format('delta').options(header=True).load(path)
df.show()

# COMMAND ----------

dbutils.fs.unmount("/mnt/gold1")

# COMMAND ----------

dbutils.fs.mount(
source = "wasbs://diamond2@bikestorestorageaccount.blob.core.windows.net/",
mount_point = "/mnt/diamond2",
extra_configs = {"fs.azure.account.key.bikestorestorageaccount.blob.core.windows.net": "cHB2CMV2MPMOIOqpWklm07YQgQJznnX1jEutSmDxX7aVVYr886Q4MEhXUSiQ7GL4jjupogi1Gq/u+AStr7sWvA=="}
)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format, col
from pyspark.sql.types import TimestampType

for i in table_name:
    path = f'/mnt/gold1/production/{i}/{i}'
    df = spark.read.format('delta').options(header=True).load(path)
    column = df.columns

    for col_name in df.columns:
        if 'date' in col_name:
            df = df.withColumn(col_name, date_format(from_utc_timestamp(col(col_name).cast(TimestampType()),"UTC"), "yyyy-MM-dd"))
    output_path = f'/mnt/diamond2/production/{i}/{i}'
    df.write.format('delta').mode('overwrite').save(output_path) 
    df.show()

# COMMAND ----------

dbutils.fs.ls('/mnt/diamond2/production/')
