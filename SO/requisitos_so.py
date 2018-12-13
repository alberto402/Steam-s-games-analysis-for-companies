
#!/usr/bin/python
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import string

spark = SparkSession.builder.master("local").appName("uno").config("spark.some.config.option", "some-value").getOrCreate()

df = spark.read.format("csv").option("header", "true").load("/Users/raqblanc/Desktop/games-features.csv")
df = df.select(df.QueryName, df.GenreIsNonGame, df.GenreIsIndie, df.GenreIsAction, df.GenreIsAdventure, df.GenreIsCasual, df.GenreIsStrategy, df.GenreIsRPG, df.GenreIsSimulation, df.GenreIsEarlyAccess, df.GenreIsFreeToPlay, df.GenreIsSports, df.GenreIsRacing, df.GenreIsMassivelyMultiplayer, df.CategorySinglePlayer, df.CategoryMultiplayer, df.CategoryCoop, df.CategoryMMO, df.CategoryInAppPurchase, df.CategoryIncludeSrcSDK, df.CategoryIncludeLevelEditor, df.CategoryVRSupport, df.PCReqsHaveMin, df.LinuxReqsHaveMin, df.MacReqsHaveMin, df.PlatformWindows, df.PlatformLinux, df.PlatformMac)


req_mac = df.where((col("MacReqsHaveMin") == "True") & (col("PlatformMac")== "True"))
count_mac = req_mac.count()

req_lin = df.where((col("LinuxReqsHaveMin") == "True") & (col("PlatformLinux")== "True"))
count_lin = req_lin.count()

req_win = df.where((col("PCReqsHaveMin") == "True") & (col("PlatformWindows")== "True"))
count_win = req_win.count()

df_count_cat = sc.parallelize([[count_win, count_mac, count_lin]])\
.toDF(['Windows','Mac', 'Linux'])

df_count.repartition(1).write.option("header", "true").csv("count_req.csv", sep = ',')
