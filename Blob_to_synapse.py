# Databricks notebook source
dbutils.fs.mount(
source = "wasbs://sliver@bikeshopsynapseaz.blob.core.windows.net/",
mount_point = "/mnt/sliver",
extra_configs = {"fs.azure.account.key.bikeshopsynapseaz.blob.core.windows.net": 
"XrsSqSxYfwGdFoDoEqYG+hpW+FZ1Ffef3stP6TTzUcihJGCe8eg6TIssFobj2CT0r0DuZoHmSgkr+ASt+NMArA=="}
)

# COMMAND ----------

table_name = []
for i in dbutils.fs.ls('/mnt/diamond2/production/'):
    table_name.append(i.name.split('/')[0])



# COMMAND ----------

for i in table_name:
    path = f'/mnt/diamond2/production/{i}/{i}/'
    df = spark.read.format('delta').options(header=True).load(path)
    column = df.columns
    for col_name in df.columns:
        output_path = f'/mnt/sliver/production/{i}/{i}'
        df.write.format('delta').mode('overwrite').save(output_path) 
    df.show()

# COMMAND ----------


