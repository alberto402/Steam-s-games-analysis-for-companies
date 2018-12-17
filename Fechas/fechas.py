from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import string


spark = SparkSession.builder.master("local").appName("uno").config("spark.some.config.option","some-value").getOrCreate()

df=spark.read.format("csv").option("header","true").load("games-features.csv")

accion=df.select(df.QueryName,df.GenreIsAction,df.SteamSpyPlayersEstimate)
accion=accion.na.drop()

indie=df.select(df.QueryName,df.GenreIsIndie,df.SteamSpyPlayersEstimate)
indie=indie.na.drop()

aventura=df.select(df.QueryName,df.GenreIsAdventure,df.SteamSpyPlayersEstimate)
aventura=aventura.na.drop()

casual=df.select(df.QueryName,df.GenreIsCasual,df.SteamSpyPlayersEstimate)
casual=casual.na.drop()

estrategia=df.select(df.QueryName,df.GenreIsStrategy,df.SteamSpyPlayersEstimate)
estrategia=estrategia.na.drop()

simuladores=df.select(df.QueryName,df.GenreIsSimulation,df.SteamSpyPlayersEstimate)
simuladores=simuladores.na.drop()

prontoAcceso=df.select(df.QueryName,df.GenreIsEarlyAccess,df.SteamSpyPlayersEstimate)
prontoAcceso=prontoAcceso.na.drop()

gratis=df.select(df.QueryName,df.GenreIsFreeToPlay,df.SteamSpyPlayersEstimate)
gratis=gratis.na.drop()

deportes=df.select(df.QueryName,df.GenreIsSports,df.SteamSpyPlayersEstimate)
deportes=deportes.na.drop()

carreras=df.select(df.QueryName,df.GenreIsRacing,df.SteamSpyPlayersEstimate)
carreras=carreras.na.drop()

mmo=df.select(df.QueryName,df.GenreIsMassivelyMultiplayer,df.SteamSpyPlayersEstimate)
mmo=mmo.na.drop()

accion=accion.filter(accion.GenreIsAction == "True").groupBy("SteamSpyPlayersEstimate").agg(avg("SteamSpyPlayersEstimate"), max("SteamSpyPlayersEstimate"))
accion.repartition(1).write.csv("jugadores/accionJugadores.csv",sep=',')

indie=indie.filter(indie.GenreIsIndie == "True").groupBy("SteamSpyPlayersEstimate").agg(avg("SteamSpyPlayersEstimate"), max("SteamSpyPlayersEstimate"))
indie.repartition(1).write.csv("jugadores/indieJugadores.csv",sep=',')

aventura=aventura.filter(aventura.GenreIsAdventure == "True").groupBy("SteamSpyPlayersEstimate").agg(avg("SteamSpyPlayersEstimate"), max("SteamSpyPlayersEstimate"))
aventura.repartition(1).write.csv("jugadores/aventuraJugadores.csv",sep=',')

casual=casual.filter(casual.GenreIsCasual == "True").groupBy("SteamSpyPlayersEstimate").agg(avg("SteamSpyPlayersEstimate"), max("SteamSpyPlayersEstimate"))
casual.repartition(1).write.csv("jugadores/casualJugadores.csv",sep=',')

estrategia=estrategia.filter(estrategia.GenreIsStrategy == "True").groupBy("SteamSpyPlayersEstimate").agg(avg("SteamSpyPlayersEstimate"), max("SteamSpyPlayersEstimate"))
estrategia.repartition(1).write.csv("jugadores/estrategiaJugadores.csv",sep=',')

simuladores=simuladores.filter(simuladores.GenreIsSimulation == "True").groupBy("SteamSpyPlayersEstimate").agg(avg("SteamSpyPlayersEstimate"), max("SteamSpyPlayersEstimate"))
simuladores.repartition(1).write.csv("jugadores/simuladoresJugadores.csv",sep=',')

prontoAcceso=prontoAcceso.filter(prontoAcceso.GenreIsEarlyAccess == "True").groupBy("SteamSpyPlayersEstimate").agg(avg("SteamSpyPlayersEstimate"), max("SteamSpyPlayersEstimate"))
prontoAcceso.repartition(1).write.csv("jugadores/prontoAccesoJugadores.csv",sep=',')

gratis=gratis.filter(gratis.GenreIsFreeToPlay == "True").groupBy("SteamSpyPlayersEstimate").agg(avg("SteamSpyPlayersEstimate"), max("SteamSpyPlayersEstimate"))
gratis.repartition(1).write.csv("jugadores/gratisJugadores.csv",sep=',')

deportes=deportes.filter(deportes.GenreIsSports == "True").groupBy("SteamSpyPlayersEstimate").agg(avg("SteamSpyPlayersEstimate"), max("SteamSpyPlayersEstimate"))
deportes.repartition(1).write.csv("jugadores/deportesJugadores.csv",sep=',')

carreras=carreras.filter(carreras.GenreIsRacing == "True").groupBy("SteamSpyPlayersEstimate").agg(avg("SteamSpyPlayersEstimate"), max("SteamSpyPlayersEstimate"))
carreras.repartition(1).write.csv("jugadores/carrerasJugadores.csv",sep=',')

mmo=mmo.filter(mmo.GenreIsMassivelyMultiplayer == "True").groupBy("SteamSpyPlayersEstimate").agg(avg("SteamSpyPlayersEstimate"), max("SteamSpyPlayersEstimate"))
mmo.repartition(1).write.csv("jugadores/mmoJugadores.csv",sep=',')
