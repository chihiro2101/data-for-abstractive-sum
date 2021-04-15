# import pandas as pd 
# # df = pd.read_parquet('ver_long.parquet', engine='pyarrow')
# df = pd.read_parquet('ver_long.parquet')
# samples = df.head(2)
# print(samples)
# import pdb; pdb.set_trace()
# print("DONE!!")


from pyspark.sql import SparkSession

appName = "Scala Parquet Example"
master = "local"

spark = SparkSession.builder.appName(appName).master(master).getOrCreate()
df = spark.read.parquet("ver_long.parquet")
samples = df.head(2)
print(samples)
import pdb; pdb.set_trace()
print("DONE!!")