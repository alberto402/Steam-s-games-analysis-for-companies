from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
import string
spark = SparkSession.builder.master("local").appName("uno").config("spark.some.config.option", "some-value").getOrCreate()

df_cat_lin = spark.read.format("csv").option("header", "true").load("/Users/raqblanc/Desktop/csv/Categoría/lin_categ_count.csv")

df_cat_mac = spark.read.format("csv").option("header", "true").load("/Users/raqblanc/Desktop/csv/Categoría/mac_categ_count.csv")

df_cat_win = spark.read.format("csv").option("header", "true").load("/Users/raqblanc/Desktop/csv/Categoría/win_categ_count.csv")

df_cat_win=df_cat_win.withColumn('row_index', func.monotonically_increasing_id())
df_cat_mac=df_cat_mac.withColumn('row_index', func.monotonically_increasing_id())
df_cat_lin=df_cat_lin.withColumn('row_index', func.monotonically_increasing_id())

df_cat = df_cat_win.join(df_cat_mac["row_index","Mac"], on=["row_index"]).drop("row_index")
df_cat = df_cat.join(df_cat_lin["Category","Linux"], on=["Category"]).drop("row_index")

df_cat.repartition(1).write.option("header", "true").csv("category_so.csv", sep = ',')
