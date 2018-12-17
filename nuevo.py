
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import string
import sys
import pyspark.sql.functions as func

spark=SparkSession.builder.master("local").appName("uno").config("spark.some.config.option","some-value").getOrCreate()

sc=SparkContext.getOrCreate()


df=spark.read.format("csv").option("header","true").load("/home/cybd/Documents/proyecto/gamess.csv")


plat ='PlatformWindows'
cat = 'CategoryCoop'
gen= 'GenreIsRacing'

df =df.select(df.PlatformWindows,df.CategoryCoop,df.GenreIsRacing, df.PriceFinal.cast("float"))

#caracteristicas para filtrar


df =df.filter(col(plat).isin(['True']))

df =df.filter(col(cat).isin(['True']) )

df =df.filter(col(gen).isin(['True']) )

# los gratis no


df=df.where(~(col("PriceFinal")==0.0 ) )







media =df.agg({'PriceFinal': 'mean'})

maximo =df.agg({"PriceFinal": "max"})

minimo =df.agg({"PriceFinal": "min"})

numero =df.count()





ff =media.union(maximo).union(minimo)


ff.withColumnRenamed("avg(PriceFinal)", "Datos")










junto =plat+cat+gen
ff.coalesce(1).write.option("header","true").csv("/home/cybd/Documents/proyecto/"+ junto +".txt")








#angPercent = spark.createDataFrame([(numero)])

#angPercent.show()





