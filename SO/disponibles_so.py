#!/usr/bin/python
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import string

spark = SparkSession.builder.master("local").appName("uno").config("spark.some.config.option", "some-value").getOrCreate()

df = spark.read.format("csv").option("header", "true").load("/Users/raqblanc/Desktop/games-features.csv")
df = df.select(df.QueryName, df.GenreIsNonGame, df.GenreIsIndie, df.GenreIsAction, df.GenreIsAdventure, df.GenreIsCasual, df.GenreIsStrategy, df.GenreIsRPG, df.GenreIsSimulation, df.GenreIsEarlyAccess, df.GenreIsFreeToPlay, df.GenreIsSports, df.GenreIsRacing, df.GenreIsMassivelyMultiplayer, df.CategorySinglePlayer, df.CategoryMultiplayer, df.CategoryCoop, df.CategoryMMO, df.CategoryInAppPurchase, df.CategoryIncludeSrcSDK, df.CategoryIncludeLevelEditor, df.CategoryVRSupport, df.PCReqsHaveMin, df.LinuxReqsHaveMin, df.MacReqsHaveMin, df.PlatformWindows, df.PlatformLinux, df.PlatformMac)


fun_win = df.where(col("PlatformWindows")== "True")
count_win = fun_win.count()


fun_lin = df.where(col("PlatformLinux")== "True")
count_lin = fun_lin.count()


fun_mac = df.where(col("PlatformMac")== "True")
count_mac = fun_mac.count()


df_count_gen = sc.parallelize([[count_win, count_mac, count_lin]])\
.toDF(['Windows','Mac', 'Linux'])

df_count.repartition(1).write.option("header", "true").csv("count_fun.csv", sep = ',')


#Generar archivos para map-reduce (genero)
gen_win = fun_win.select("GenreIsNonGame", "GenreIsIndie", "GenreIsAction", "GenreIsAdventure", "GenreIsCasual", "GenreIsStrategy", "GenreIsRPG", "GenreIsSimulation", "GenreIsEarlyAccess", "GenreIsFreeToPlay", "GenreIsSports", "GenreIsRacing", "GenreIsMassivelyMultiplayer")

gen_lin = fun_lin.select("GenreIsNonGame", "GenreIsIndie", "GenreIsAction", "GenreIsAdventure", "GenreIsCasual", "GenreIsStrategy", "GenreIsRPG", "GenreIsSimulation", "GenreIsEarlyAccess", "GenreIsFreeToPlay", "GenreIsSports", "GenreIsRacing", "GenreIsMassivelyMultiplayer")

gen_mac = fun_mac.select("GenreIsNonGame", "GenreIsIndie", "GenreIsAction", "GenreIsAdventure", "GenreIsCasual", "GenreIsStrategy", "GenreIsRPG", "GenreIsSimulation", "GenreIsEarlyAccess", "GenreIsFreeToPlay", "GenreIsSports", "GenreIsRacing", "GenreIsMassivelyMultiplayer")

gen_win.repartition(1).write.option("header", "true").csv("win_genre.csv", sep = ',')
gen_lin.repartition(1).write.option("header", "true").csv("lin_genre.csv", sep = ',')
gen_mac.repartition(1).write.option("header", "true").csv("mac_genre.csv", sep = ',')

#Generar archivos para map-reduce (categorias)
cat_win = fun_win.select("CategorySinglePlayer", "CategoryMultiplayer", "CategoryCoop", "CategoryMMO", "CategoryInAppPurchase", "CategoryIncludeSrcSDK", "CategoryIncludeLevelEditor", "CategoryVRSupport")

cat_lin = fun_lin.select("CategorySinglePlayer", "CategoryMultiplayer", "CategoryCoop", "CategoryMMO", "CategoryInAppPurchase", "CategoryIncludeSrcSDK", "CategoryIncludeLevelEditor", "CategoryVRSupport")

cat_mac = fun_mac.select("CategorySinglePlayer", "CategoryMultiplayer", "CategoryCoop", "CategoryMMO", "CategoryInAppPurchase", "CategoryIncludeSrcSDK", "CategoryIncludeLevelEditor", "CategoryVRSupport")

cat_win.repartition(1).write.option("header", "true").csv("win_categ.csv", sep = ',')
cat_lin.repartition(1).write.option("header", "true").csv("lin_categ.csv", sep = ',')
cat_mac.repartition(1).write.option("header", "true").csv("mac_categ.csv", sep = ',')
