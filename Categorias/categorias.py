from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import string


spark = SparkSession.builder.master("local").appName("uno").config("spark.some.config.option","some-value").getOrCreate()

df=spark.read.format("csv").option("header","true").load("games-features.csv")

accion=df.select(df.QueryName,df.GenreIsAction,df.Metacritic, df.PriceFinal)
accion=accion.na.drop()

indie=df.select(df.QueryName,df.GenreIsIndie,df.Metacritic, df.PriceFinal)
indie=indie.na.drop()

aventura=df.select(df.QueryName,df.GenreIsAdventure,df.Metacritic, df.PriceFinal)
aventura=aventura.na.drop()

casual=df.select(df.QueryName,df.GenreIsCasual,df.Metacritic, df.PriceFinal)
casual=casual.na.drop()

estrategia=df.select(df.QueryName,df.GenreIsStrategy,df.Metacritic, df.PriceFinal)
estrategia=estrategia.na.drop()

simuladores=df.select(df.QueryName,df.GenreIsSimulation,df.Metacritic, df.PriceFinal)
simuladores=simuladores.na.drop()

prontoAcceso=df.select(df.QueryName,df.GenreIsEarlyAccess,df.Metacritic, df.PriceFinal)
prontoAcceso=prontoAcceso.na.drop()

gratis=df.select(df.QueryName,df.GenreIsFreeToPlay,df.Metacritic, df.PriceFinal)
gratis=gratis.na.drop()

deportes=df.select(df.QueryName,df.GenreIsSports,df.Metacritic, df.PriceFinal)
deportes=deportes.na.drop()

carreras=df.select(df.QueryName,df.GenreIsRacing,df.Metacritic, df.PriceFinal)
carreras=carreras.na.drop()

mmo=df.select(df.QueryName,df.GenreIsMassivelyMultiplayer,df.Metacritic, df.PriceFinal)
mmo=mmo.na.drop()

accion=accion.filter(accion.GenreIsAction == "True").sort("Metacritic",ascending=False)
accion.repartition(1).write.csv("accion.csv",sep=',')

indie=indie.filter(indie.GenreIsIndie == "True").sort("Metacritic",ascending=False)
indie.repartition(1).write.csv("indie.csv",sep=',')

aventura=aventura.filter(aventura.GenreIsAdventure == "True").sort("Metacritic",ascending=False)
aventura.repartition(1).write.csv("aventura.csv",sep=',')

casual=casual.filter(casual.GenreIsCasual == "True").sort("Metacritic",ascending=False)
casual.repartition(1).write.csv("casual.csv",sep=',')

estrategia=estrategia.filter(estrategia.GenreIsStrategy == "True").sort("Metacritic",ascending=False)
estrategia.repartition(1).write.csv("estrategia.csv",sep=',')

simuladores=simuladores.filter(simuladores.GenreIsSimulation == "True").sort("Metacritic",ascending=False)
simuladores.repartition(1).write.csv("simuladores.csv",sep=',')

prontoAcceso=prontoAcceso.filter(prontoAcceso.GenreIsEarlyAccess == "True").sort("Metacritic",ascending=False)
prontoAcceso.repartition(1).write.csv("prontoAcceso.csv",sep=',')

gratis=gratis.filter(gratis.GenreIsFreeToPlay == "True").sort("Metacritic",ascending=False)
gratis.repartition(1).write.csv("gratis.csv",sep=',')

deportes=deportes.filter(deportes.GenreIsSports == "True").sort("Metacritic",ascending=False)
deportes.repartition(1).write.csv("deportes.csv",sep=',')

carreras=carreras.filter(carreras.GenreIsRacing == "True").sort("Metacritic",ascending=False)
carreras.repartition(1).write.csv("carreras.csv",sep=',')

mmo=mmo.filter(mmo.GenreIsMassivelyMultiplayer == "True").sort("Metacritic",ascending=False)
mmo.repartition(1).write.csv("mmo.csv",sep=',')
