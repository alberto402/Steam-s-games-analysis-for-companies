from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
import string
spark = SparkSession.builder.master("local").appName("uno").config("spark.some.config.option", "some-value").getOrCreate()

df_gen_lin = spark.read.format("csv").option("header", "true").load("/ruta/lin_genre_count.csv")

df_gen_mac = spark.read.format("csv").option("header", "true").load("/ruta/mac_genre_count.csv")

df_gen_win = spark.read.format("csv").option("header", "true").load("/ruta/win_genre_count.csv")

df_gen_win=df_gen_win.withColumn('row_index', func.monotonically_increasing_id())
df_gen_mac=df_gen_mac.withColumn('row_index', func.monotonically_increasing_id())
df_gen_lin=df_gen_lin.withColumn('row_index', func.monotonically_increasing_id())

df_gen = df_gen_win.join(df_gen_mac["row_index","Mac"], on=["row_index"]).drop("row_index")
df_gen = df_gen.join(df_gen_lin["Genre","Linux"], on=["Genre"]).drop("row_index")

df_gen.repartition(1).write.option("header", "true").csv("genre_so.csv", sep = ',')
