import json
from pathlib import Path
import random
import os
# import pyproj
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import col, trim, regexp_replace, lit, when,udf
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
    plpbar = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(path_data+'plpbar.csv')

    # Cambiar los nombres de las columnas
    plpbar = plpbar.toDF("Hidro","time","TipoEtapa","id","BarName","CMgBar","DemBarP","DemBarE","PerBarP","PerBarE","BarRetP","BarRetE")

    # Eliminar los espacios en las columnas 'BarName' y 'Hidro'
    plpbar = plpbar.withColumn('BarName', regexp_replace('BarName', ' ', ''))
    plpbar = plpbar.withColumn('Hidro', regexp_replace('Hidro', ' ', ''))
    plpbar = plpbar.withColumn("id", col("id").cast("int"))
    plpbar = plpbar.withColumn("time", col("time").cast("int"))
    # Seleccionar las columnas 'id' y 'BarName'
    indexbus = plpbar.select("id", "BarName").dropDuplicates()

    print("--------Cargando Archivo de ubicaciones Ubibar-------------\n")
    ubibar = spark.read.options(delimiter=';').format('csv').option('header', 'true').option('inferSchema', 'true').load(f'{path_data}/ubibar.csv')
    ubibar = ubibar.drop('ID')
    ubibar = ubibar.withColumn('LATITUD', regexp_replace('LATITUD', ',', '.').cast('float'))
    ubibar = ubibar.withColumn('LONGITUD', regexp_replace('LONGITUD', ',', '.').cast('float'))
    ubibar = ubibar.toDF("BarName","latitud","longitud")
    ubibar = ubibar.withColumn('BarName',regexp_replace('BarName', " ", ""))




    print("--------Cargando Archivo plpcen-------------\n")

    plpcen = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(path_data+'plpcen.csv')

    plpcen = plpcen.toDF("Hidro","time","TipoEtapa","id","CenName","tipo","bus_id","BarName","CenQgen","CenPgen","CenEgen","CenInyP","CenInyE","CenRen","CenCVar","CenCostOp","CenPMax")

    # Eliminar los espacios en las columnas 'CenName' y 'Hidro'
    plpcen = plpcen.withColumn('CenName', regexp_replace('CenName', ' ', ''))
    plpcen = plpcen.withColumn('Hidro', regexp_replace('Hidro', ' ', ''))
    plpcen = plpcen.withColumn("time", col("time").cast("int"))

    # Descartar algunas columnas
    plpcen = plpcen.drop("CenEgen","CenInyP","CenInyE","CenRen","CenCostOp","CenPMax")

    # Crear una columna constante 'tipo'
    plpcen = plpcen.withColumn("tipo", lit("otros"))

    # Crear un DataFrame que contiene las columnas únicas 'id', 'CenName', 'tipo', 'bus_id'
    indexcen = plpcen.select("id", "CenName", "tipo", "bus_id").dropDuplicates()

    print("--------Cargando Archivo centralsinfo-------------\n")
    centralsinfo = spark.read.options(delimiter=';').format('csv').option('header', 'true').option('inferSchema', 'true').load(f'{path_data}/centralesinfo.csv')

    # Cambiar los nombres de las columnas
    centralsinfo = centralsinfo.toDF('id','CenName','type','CVar','effinciency','bus_id','serie_hidro_gen','serie_hidro_ver','min_power','max_power',"VembIn","VembFin","VembMin","VembMax","cotaMínima")

    # Eliminar los espacios en la columna 'CenName'
    centralsinfo = centralsinfo.withColumn('CenName', regexp_replace('CenName', " ", ""))

    # Cambiar las comas a puntos en algunas columnas
    cols = ['min_power', 'max_power', 'effinciency', 'CVar', 'VembIn', 'VembFin', 'VembMin', 'VembMax', 'cotaMínima']
    for c in cols:
        centralsinfo = centralsinfo.withColumn(c, regexp_replace(c, ',', '.').cast('float'))


    print("--------Cargando Archivo plplin-------------\n")

    # Cargando el archivo plplin.csv en un DataFrame de Spark
    plplin = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(path_data+'plplin.csv')

    # Cambiando los nombres de las columnas
    plplin = plplin.toDF("Hidro","time","TipoEtapa","id","LinName","bus_a","bus_b","LinFluP","LinFluE","capacity","LinUso","LinPerP","LinPerE","LinPer2P","LinPer2E","LinITP","LinITE")
    plplin = plplin.withColumn("time", col("time").cast("int"))

    # Removiendo los espacios en las columnas 'LinName' y 'Hidro'
    plplin = plplin.withColumn('LinName', regexp_replace('LinName', ' ', ''))
    plplin = plplin.withColumn('Hidro', regexp_replace('Hidro', ' ', ''))

    # Seleccionando las columnas 'id', 'LinName', 'bus_a', 'bus_b'
    indexlin = plplin.select('id', 'LinName', 'bus_a', 'bus_b').dropDuplicates()

    print("--------Cargando Archivo linesinfo-------------\n")

    # Cargando el archivo linesinfo.csv en un DataFrame de Spark
    linesinfo = spark.read.options(delimiter=';').format('csv').option('header', 'true').option('inferSchema', 'true').load(path_data+'linesinfo.csv')

    # Cambiando los nombres de las columnas
    linesinfo = linesinfo.toDF("id","LinName","bus_a","bus_b","max_flow_a_b","max_flow_b_a","voltage","r","x","segments")

    # Removiendo los espacios en la columna 'LinName'
    linesinfo = linesinfo.withColumn('LinName', regexp_replace('LinName', " ", ""))

    # Reemplazando comas por puntos y convirtiendo a tipo float
    cols = ['max_flow_a_b', 'max_flow_b_a', 'r', 'x']
    for c in cols:
        linesinfo = linesinfo.withColumn(c, regexp_replace(c, ',', '.').cast('float'))

    # Realizando merge de indexlin y linesinfo basándose en 'LinName'
    linesfinal = indexlin.join(linesinfo.withColumnRenamed('id', 'linesinfo_id'), 'LinName', 'inner').drop('bus_a', 'bus_b')
    linesfinal = linesfinal.withColumn("id", col("linesinfo_id").cast(IntegerType())).drop('linesinfo_id')


    
    # Creación de las rutas de directorios
    electricTopology = f"{namedata}/Topology/Electric"
    hydricTopology = f"{namedata}/Topology/Hydric"

    # Crear los directorios si no existen
    os.makedirs(electricTopology, exist_ok=True)
    os.makedirs(hydricTopology, exist_ok=True)

    # Obtener los valores únicos de la columna 'Hidro' del DataFrame 'plpbar'
    hidrolist = plpbar.select("Hidro").distinct().rdd.flatMap(lambda x: x).collect()

    busscenariolist = []
    centralscenariolist = []
    linescenariolist = []


    for hidronum in range(len(hidrolist)):
        # Crear los directorios
        busscenario = f"{namedata}/Scenarios/{hidronum+1}/Bus"
        centralscenario = f"{namedata}/Scenarios/{hidronum+1}/Centrals"
        linescenario = f"{namedata}/Scenarios/{hidronum+1}/Lines"
       

        os.makedirs(busscenario, exist_ok=True)
        busscenariolist.append(busscenario)

        os.makedirs(centralscenario, exist_ok=True)
        centralscenariolist.append(centralscenario)

        os.makedirs(linescenario, exist_ok=True)
        linescenariolist.append(linescenario)

    hydrofile = [x for x in range(1,len(hidrolist)+1)]

    with open( namedata+'/Scenarios/hydrologies.json', 'w') as f:
        json.dump(hydrofile, f)

    # Número de horas de bloques temporales del proyecto
    time=plpbar.agg({"time": "max"}).collect()[0][0]

    # Número de barras
    nbus=indexbus.count()
    lbus=[row['id'] for row in indexbus.collect()]

    # Número de generadores
    ngen=indexcen.count()

    # Número de lineas
    nlin=indexlin.count()

    # Número de Reservoirs


    print("Creando Archivos Bus en Scenario \n")


    def busscenariofunction(dfbusauxlist, pathbus):
        for x in range(nbus): 
            idbus = lbus[x]
            dfbusaux = dfbusauxlist[x]
            aux = dfbusaux.withColumn('id', lit(idbus))\
                        .withColumn('name', lit(indexbus.filter(indexbus.id == idbus).select('BarName').first()[0]))\
                        .withColumn('marginal_cost', dfbusaux['CMgBar'])\
                        .withColumn('value', dfbusaux['CMgBar'])\
                        .withColumn('DemBarE', dfbusaux['DemBarE'])\
                        .withColumn('DemBarP', dfbusaux['DemBarP'])\
                        .withColumn('BarRetP', dfbusaux['BarRetP'])
        
            aux.write.json(pathbus +  f"/bus_{idbus}.json")

    for hidronum, hidroname in enumerate(hidrolist):
        dfbussauxx = plpbar.filter(col("Hidro") == hidroname)
        dfbuslist = [dfbussauxx.filter(dfbussauxx.id == idaux) for idaux in lbus]
        print(f"{((hidronum+1)/len(hidrolist))*100}% Completado")
        busscenariofunction(dfbuslist, busscenariolist[hidronum])


        print("Creando Archivos Central en Scenario \n")

    def centralscenariofunction(dfcenauxlist, cenpath):
        for x in range(ngen):
            idaux = indexcen.filter(indexcen['index'] == x).select('id').first()[0]
            dfcen = dfcenauxlist[x]
            bus_id = indexcen.filter(indexcen['index'] == x).select('bus_id').first()[0]
            if bus_id == 0 or bus_id is None:
                continue

            time = dfcen.select('time').rdd.flatMap(lambda x: x).collect()
            CenPgen = dfcen.select('CenPgen').rdd.flatMap(lambda x: x).collect() if dfcen.count() > 0 else [0]*len(time)
            CenCVar = dfcen.select('CenCVar').rdd.flatMap(lambda x: x).collect() if dfcen.count() > 0 else [0]*len(time)
            CenQgen = dfcen.select('CenQgen').rdd.flatMap(lambda x: x).collect() if dfcen.count() > 0 else [0]*len(time)

            aux_df = dfcen.withColumn('id', lit(idaux))\
                        .withColumn('time', lit(time))\
                        .withColumn('bus_id', lit(int(bus_id)))\
                        .withColumn('name', lit(indexcen.filter(indexcen.id == idaux).select('CenName').first()[0]))\
                        .withColumn('CenPgen', lit(CenPgen))\
                        .withColumn('value', lit(CenPgen))\
                        .withColumn('CenCVar', lit(CenCVar))\
                        .withColumn('CenQgen', lit(CenQgen))

            aux_df.write.json(cenpath + f"/central_{idaux}.json")
            
    for hidronum, hidroname in enumerate(hidrolist):
        dfcensauxx = plpcen.filter(col("Hidro") == hidroname)
        dfcenlist = [dfcensauxx.filter(dfcensauxx.id == idaux) for idaux in [indexcen.filter(indexcen['index'] == x).select('id').first()[0] for x in range(ngen)]]
        print(f"{((hidronum + 1) / len(hidrolist)) * 100}% Completado")
        centralscenariofunction(dfcenlist, centralscenariolist[hidronum])


    print("Creando Archivos Lineas en Scenario \n")
    def linescenariofunction(dflinelist, linpath):
        for x in range(nlin):
            idaux = linesfinal.filter(linesfinal.index == x).select('id').first()[0]
            dflinea = dflinelist[x]
            aux_df = dflinea.withColumn('id', lit(idaux))\
                            .withColumn('time',dflinea('time'))\
                            .withColumn('name', lit(linesfinal.filter(linesfinal.id == idaux).select('LinName').first()[0]))\
                            .withColumn('bus_a', lit(linesfinal.filter(linesfinal.id == idaux).select('bus_a').first()[0]))\
                            .withColumn('bus_b', lit(linesfinal.filter(linesfinal.id == idaux).select('bus_b').first()[0]))\
                            .withColumn('flow', dflinea['LinFluP'])\
                            .withColumn('value', dflinea['LinFluP'])\
                            .withColumn('capacity', dflinea['capacity'])
            
            aux_df.write.json(linpath + f"/line_{idaux}.json")

    for hidronum, hidroname in enumerate(hidrolist):
        dflinesaux = plplin.filter(col("Hidro") == hidroname)
        dflinelist = [dflinesaux.filter(dflinesaux.id == idaux) for idaux in range(nlin)]
        print(f"{((hidronum + 1) / len(hidrolist)) * 100}% Completado")
        linescenariofunction(dflinelist, linescenariolist[hidronum])

spark.stop()