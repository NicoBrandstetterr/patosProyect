import json
from pathlib import Path
import random
import os
# import pyproj
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import col, trim, regexp_replace, lit, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pysparkscript.py <filein> <fileout>", file=sys.stderr)
        sys.exit(-1)

    filein = sys.argv[1]
    fileout = sys.argv[2]
    # Dirección donde se ubican los archivos que se cargarán
    path_data=filein+'/'

    # Nombre que tendrá el caso
    namedata=fileout+'/PLPPYSPARK'

    print("---------------------------------- Iniciando Carga de archivos-----------------------------------\n")



    spark = SparkSession.builder.getOrCreate()
    print("--------Cargando Archivo plpbar-------------\n")

    # Cargar el archivo csv en un DataFrame de Spark
    path_data= '/uhadoop2023/energyviewer/data_test'
    plpbar = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(path_data+'plpbar.csv')

    # Cambiar los nombres de las columnas
    plpbar = plpbar.toDF("Hidro","time","TipoEtapa","id","BarName","CMgBar","DemBarP","DemBarE","PerBarP","PerBarE","BarRetP","BarRetE")

    # Eliminar los espacios en las columnas 'BarName' y 'Hidro'
    plpbar = plpbar.withColumn('BarName', regexp_replace('BarName', ' ', ''))
    plpbar = plpbar.withColumn('Hidro', regexp_replace('Hidro', ' ', ''))
    plpbar = plpbar.withColumn("id", col("id").cast("int"))
    plpbar = plpbar.withColumn("time", col("time").cast("int"))
    plpbar.drop('TipoEtapa', 'PerBarE', 'PerBarP', 'BarRetE')
    
    print("--------Cargando Archivo de ubicaciones Ubibar-------------\n")
    barras = spark.read.options(delimiter=';').format('csv').option('header', 'true').option('inferSchema', 'true').load(f'{path_data}/ubibar.csv')
    barras = barras.withColumn('LATITUD', regexp_replace('LATITUD', ',', '.').cast('float'))
    barras = barras.withColumn('LONGITUD', regexp_replace('LONGITUD', ',', '.').cast('float'))
    barras = barras.toDF('id',"BarName","latitud","longitud")
    barras = barras.withColumn('BarName',regexp_replace('BarName', " ", ""))


    print("--------Cargando Archivo plpcen-------------\n")

    # Cargar el archivo csv en un DataFrame de Spark
    plpcen = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(path_data+'plpcen.csv')

    # Cambiar los nombres de las columnas
    plpcen = plpcen.toDF("Hidro","time","TipoEtapa","id","CenName","tipo","bus_id","BarName","CenQgen",
                         "CenPgen","CenEgen","CenInyP","CenInyE","CenRen","CenCVar","CenCostOp","CenPMax")

    # Eliminar los espacios en las columnas 'CenName' y 'Hidro'
    plpcen = plpcen.withColumn('CenName', regexp_replace('CenName', ' ', ''))
    plpcen = plpcen.withColumn('Hidro', regexp_replace('Hidro', ' ', ''))
    plpcen = plpcen.withColumn("time", col("time").cast("int"))

    # Descartar algunas columnas
    plpcen = plpcen.drop("CenEgen","CenInyP","CenInyE","CenRen","CenCostOp","CenPMax")

    centrales = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(path_data+'centrales.csv')

    # Cargando el archivo plplin.csv en un DataFrame de Spark
    plplin = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(path_data+'plplin.csv')

    # Cambiando los nombres de las columnas
    plplin = plplin.toDF("Hidro","time","TipoEtapa","id","LinName","bus_a","bus_b","LinFluP","LinFluE","capacity","LinUso","LinPerP","LinPerE","LinPer2P","LinPer2E","LinITP","LinITE")
    plplin = plplin.withColumn("time", col("time").cast("int"))

    # Removiendo los espacios en las columnas 'LinName' y 'Hidro'
    plplin = plplin.withColumn('LinName', regexp_replace('LinName', ' ', ''))
    plplin = plplin.withColumn('Hidro', regexp_replace('Hidro', ' ', ''))

    lineas = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(path_data+'lineas.csv')


    #pasarlo a tablas de sql
    plpbar.createOrReplaceTempView("plpbar")
    barras.createOrReplaceTempView("barras")
    query = """SELECT barras.id AS bar_id, plpbar.Hidro AS hydro, plpbar.time AS time, plpbar.CMgBar AS marginal_cost, plpbar.DemBarE, plpbar.DemBarP, plpbar.BarRetP
    FROM barras, plpbar 
    WHERE barras.BarName = plpbar.BarName
    ORDER BY hydro ASC, bar_id ASC, time ASC
    """
    bar_dynamic = spark.sql(query)

    plpcen.createOrReplaceTempView("plpcen")
    centrales.createOrReplaceTempView("centrales")
    query = """SELECT centrales.id AS cen_id, plpcen.Hidro AS hydro, plpcen.time AS time, plpcen.CenPgen AS CenPgen, plpcen.CenCVar, plpcen.CenQgen
    FROM centrales, plpcen 
    WHERE centrales.cen_name = plpcen.CenName
    ORDER BY hydro ASC, cen_id ASC, time ASC
    """
    cen_dynamic = spark.sql(query)

    plplin.createOrReplaceTempView("plplin")
    lineas.createOrReplaceTempView("lineas")
    query = """SELECT lineas.id AS lin_id, plplin.Hidro AS hydro, plplin.time AS time, plplin.LinFluP AS LinFluP, plplin.capacity
    FROM lineas, plplin 
    WHERE lineas.LinName = plplin.LinName
    ORDER BY hydro ASC, lin_id ASC, time ASC
    """
    lin_dynamic = spark.sql(query)