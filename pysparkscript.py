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

    # Cargar el archivo csv en un DataFrame de Spark
    plpcen = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(path_data+'plpcen.csv')

    # Cambiar los nombres de las columnas
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

    # Cargar el archivo 'hydric_adicional.csv'
    hydric_adicional = spark.read.options(delimiter=';').format('csv').option('header', 'true').option('inferSchema', 'true').load(f'{path_data}/hydric_adicional.csv')

    # Cargar el archivo 'centralestype.csv'
    tiposcentrales = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(path_data+'centralestype.csv')
    tiposcentrales = tiposcentrales.withColumnRenamed("cen_name", "CenName")

    # Unir indexcen y tiposcentrales en base a 'CenName'
    typecentrals = indexcen.join(tiposcentrales, 'CenName', 'inner')

    # Reemplazar los valores en la columna 'tipo' en plpcen basándose en typecentrals
    # Asumiendo que 'id' y 'CenName' son únicos en typecentrals
    typecentrals = typecentrals.withColumnRenamed('tipo', 'new_tipo')

    # Unir plpcen y typecentrals en base a 'id' y 'CenName'
    plpcen = plpcen.join(typecentrals.select('id', 'CenName', 'new_tipo'), on=['id', 'CenName'], how='left')

    # Crear una nueva columna 'tipo' que toma el valor de 'new_tipo' si este no es nulo, y si no, toma el valor de la antigua columna 'tipo'
    plpcen = plpcen.withColumn('tipo', when(col('new_tipo').isNull(), col('tipo')).otherwise(col('new_tipo')))

    # Finalmente, puedes descartar la columna 'new_tipo'
    plpcen = plpcen.drop('new_tipo')


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


    print("--------Cargando Archivo plpemb-------------\n")

    # Cargando el archivo plpemb.csv en un DataFrame de Spark
    reservoirs = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(path_data+'plpemb.csv')

    # Cambiando los nombres de las columnas
    reservoirs = reservoirs.withColumnRenamed('Bloque', 'time') \
        .withColumnRenamed('EmbNum', 'id') \
        .withColumnRenamed('EmbNom', 'EmbName')

    # Removiendo los espacios en las columnas 'EmbName' y 'Hidro'
    reservoirs = reservoirs.withColumn('EmbName', regexp_replace('EmbName', ' ', ''))
    reservoirs = reservoirs.withColumn('Hidro', regexp_replace('Hidro', ' ', ''))
    reservoirs = reservoirs.withColumn("time", col("time").cast("int"))

    # Seleccionando las columnas 'id', 'EmbName' y eliminando duplicados
    indexres = reservoirs.select('id', 'EmbName').dropDuplicates()

    # Filtrando el DataFrame centralsinfo según el tipo
    junctionsinfo = centralsinfo.filter(centralsinfo['type'].isin(["E", 'S', 'R']))
    reservoirsinfo = centralsinfo.filter(centralsinfo['type'].isin(["E"]))

    # Renombrando la columna 'CenName' a 'EmbName'
    reservoirsinfo = reservoirsinfo.withColumnRenamed('CenName', 'EmbName')

    # Asegurando que 'id' es de tipo Integer
    reservoirsinfo = reservoirsinfo.withColumn("id", col("id").cast(IntegerType()))
    indexres = indexres.withColumn("id", col("id").cast(IntegerType()))

    # Uniendo 'reservoirsinfo' con 'indexres' en base a 'EmbName' y actualizando 'id' en 'reservoirsinfo' con 'id' de 'indexres'
    reservoirsinfo = reservoirsinfo.join(indexres.withColumnRenamed('id', 'indexres_id'), 'EmbName', 'left')
    reservoirsinfo = reservoirsinfo.withColumn("id", col("indexres_id").cast(IntegerType())).drop('indexres_id')
    print("--------Cargando Archivo indhor-------------\n")

    # Cargando el archivo indhor.csv en un DataFrame de Spark
    indhor = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').option('encoding', 'ISO-8859-1').load(path_data+'indhor.csv')

    # Creación de las rutas de directorios
    electricTopology = Path(namedata) / 'Topology' / 'Electric'
    hydricTopology = Path(namedata) / 'Topology' / 'Hydric'

    # Crear los directorios si no existen
    electricTopology.mkdir(parents=True, exist_ok=True)
    hydricTopology.mkdir(parents=True, exist_ok=True)

    # Obtener los valores únicos de la columna 'Hidro' del DataFrame 'plpbar'
    hidrolist = plpbar.select("Hidro").distinct().rdd.flatMap(lambda x: x).collect()

    busscenariolist=[]
    centralscenariolist=[]
    linescenariolist=[]
    reservoirscenariolist=[]

    for hidronum in range(len(hidrolist)):
        # Crear los directorios
        busscenario = Path(namedata) / f'Scenarios/{hidronum+1}/Bus'
        centralscenario = Path(namedata) / f'Scenarios/{hidronum+1}/Centrals'
        linescenario = Path(namedata) / f'Scenarios/{hidronum+1}/Lines'
        reservoirscenario = Path(namedata) / f'Scenarios/{hidronum+1}/Reservoirs'

        busscenario.mkdir(parents=True, exist_ok=True)
        busscenariolist.append(busscenario)

        centralscenario.mkdir(parents=True, exist_ok=True)
        centralscenariolist.append(centralscenario)

        linescenario.mkdir(parents=True, exist_ok=True)
        linescenariolist.append(linescenario)

        reservoirscenario.mkdir(parents=True, exist_ok=True)
        reservoirscenariolist.append(reservoirscenario)

    marginal_cost_path = Path(namedata) / 'Scenarios/Marginal_cost_percentil'
    line_flow_percentil_path = Path(namedata) / 'Scenarios/Flow_Line_percentil'
    generation_sistem_path = Path(namedata) / 'Scenarios/Generation_system'

    marginal_cost_path.mkdir(parents=True, exist_ok=True)
    line_flow_percentil_path.mkdir(parents=True, exist_ok=True)
    generation_sistem_path.mkdir(parents=True, exist_ok=True)





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
    nres = reservoirs.select("EmbName").distinct().count()

    print(time)
    print("tipo time", type(time))
    print(nbus)
    print(type(nbus))

    print("Creando Archivos Bus en Scenario \n")

    # Definir la estructura del DataFrame
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("time", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("marginal_cost", FloatType(), True),
        StructField("value", FloatType(), True),
        StructField("DemBarE", FloatType(), True),
        StructField("DemBarP", FloatType(), True),
        StructField("BarRetP", FloatType(), True)
    ])

    def busscenariofunction(dfbusauxlist, pathbus):
        for x in range(nbus): # Para cada barra
            bus_sc_1filas_aux=[]
            for y in range(1,time+1): # Para cada bloque de tiempo, se agrega un estado de la barra x
                aux=[]
                
                idbus=lbus[x]
                aux.append(idbus)
                aux.append(y)
                aux.append(indexbus.filter(indexbus.id == idbus).select('BarName').first()[0])
                aux.append(dfbusauxlist[x].filter(col('time') == y).select('CMgBar').first()[0])
                aux.append(aux[-1])
                aux.append(dfbusauxlist[x].filter(col('time') == y).select('DemBarE').first()[0])
                aux.append(dfbusauxlist[x].filter(col('time') == y).select('DemBarP').first()[0])
                aux.append(dfbusauxlist[x].filter(col('time') == y).select('BarRetP').first()[0])
                bus_sc_1filas_aux.append(aux)
            bus_sc_1_aux=spark.createDataFrame(bus_sc_1filas_aux, schema)
            bus_sc_1_aux.write.json(pathbus+f"/bus_{idbus}.json")

    for hidronum, hidroname in enumerate(hidrolist):
        dfbussauxx=plpbar.filter(col("Hidro")==hidroname)
        dfbuslist=[]
        for x in lbus:
            idaux=x
            dfbuslist.append(dfbussauxx.filter(dfbussauxx.id==idaux))
        print(f"{((hidronum+1)/len(hidrolist))*100}% Completado")
        busscenariofunction(dfbuslist,busscenariolist[hidronum])



    print("Creando Archivos Central en Scenario \n")

    def centralscenariofunction(dfcenauxlist,cenpath):
        for x in range(ngen):  # Para cada generador (central)
            if indexcen.where(col('id') == x).first()['bus_id'] is None:  # No existe la barra 0, por lo que no se consideran dichos generadores
                pass
            else:
                central_sc_1filas_aux = []
                for y in range(1, time + 1):  # Para cada bloque de tiempo, se agrega un estado del generador x
                    aux = []
                    aux.append(indexcen.where(col('id') == x).first()['id'])
                    aux.append(y)
                    aux.append(int(indexcen.where(col('id') == x).first()['bus_id']))
                    aux.append(indexcen.where(col('id') == x).first()['CenName'])
                    row_aux = dfcenauxlist.where((col('id') == x) & (col('time') == y)).first()
                    if row_aux is None:
                        for i in range(4):
                            aux.append(0)
                    else:
                        aux.append(row_aux['CenPgen'])
                        aux.append(row_aux['CenPgen'])
                        aux.append(row_aux['CenCVar'])
                        aux.append(row_aux['CenQgen'])
                    central_sc_1filas_aux.append(aux)
                central_sc_1_aux = spark.createDataFrame(central_sc_1filas_aux, ['id', 'time', 'bus_id', 'name', 'CenPgen', 'value', 'CenCVar', 'CenQgen'])
                central_sc_1_aux.write.json(cenpath + f"/central_{indexcen.where(col('id') == x).first()['id']}.json")

    for hidronum, hidroname in enumerate(hidrolist):
        dfcensauxx = plpcen.filter(col('Hidro') == hidroname)
        dfcenlist = dfcensauxx
        print(f"{((hidronum + 1) / len(hidrolist)) * 100}% Completado")
        centralscenariofunction(dfcenlist, centralscenariolist[hidronum])


    print("Creando Archivos Lineas en Scenario \n")
    def linescenariofunction(dflinelist, linpath):
        for x in range(nlin):  # Para cada linea
            line_sc_1filas_aux = []
            for y in range(1, time + 1):  # Para cada bloque de tiempo, se agrega un estado de la linea x
                aux = []
                idaux = linesfinal.where(col('id') == x).first()['id']
                bus_a_id = linesfinal.where(col('id') == x).first()['bus_a']
                bus_b_id = linesfinal.where(col('id') == x).first()['bus_b']
                name = f"{indexbus.where(col('id') == (bus_a_id - 1)).first()['BarName']}->{indexbus.where(col('id') == (bus_b_id - 1)).first()['BarName']}"
                aux.append(idaux)
                aux.append(y)
                aux.append(name)
                aux.append(bus_a_id)
                aux.append(bus_b_id)
                row_aux = dflinelist.where((col('id') == x) & (col('time') == y)).first()
                if row_aux is None:
                    for i in range(2):
                        aux.append(0)
                else:
                    aux.append(row_aux['LinFluP'])
                    aux.append(row_aux['LinFluP'])
                    aux.append(row_aux['capacity'])
                line_sc_1filas_aux.append(Row(id=aux[0], time=aux[1], name=aux[2], bus_a=aux[3], bus_b=aux[4], flow=aux[5], value=aux[6], capacity=aux[7]))

            line_sc_1_aux = spark.createDataFrame(line_sc_1filas_aux)
            line_sc_1_aux.write.json(linpath + f"/line_{idaux}.json")

    for hidronum, hidroname in enumerate(hidrolist):
        dflinesaux = plplin.filter(col('Hidro') == hidroname)
        dflinelist = dflinesaux
        print(f"{((hidronum + 1) / len(hidrolist)) * 100}% Completado")
        linescenariofunction(dflinelist, linescenariolist[hidronum])


    def linescenariofunction(dflinelist, linpath):
        for x in range(nlin):  # Para cada linea
            line_sc_1filas_aux = []
            for y in range(1, time + 1):  # Para cada bloque de tiempo, se agrega un estado de la linea x
                aux = []
                idaux = linesfinal.where(col('id') == x).first()['id']
                bus_a_id = linesfinal.where(col('id') == x).first()['bus_a']
                bus_b_id = linesfinal.where(col('id') == x).first()['bus_b']
                name = f"{indexbus.where(col('id') == (bus_a_id - 1)).first()['BarName']}->{indexbus.where(col('id') == (bus_b_id - 1)).first()['BarName']}"
                aux.append(idaux)
                aux.append(y)
                aux.append(name)
                aux.append(bus_a_id)
                aux.append(bus_b_id)
                row_aux = dflinelist.where((col('id') == x) & (col('time') == y)).first()
                if row_aux is None:
                    for i in range(2):
                        aux.append(0)
                else:
                    aux.append(row_aux['LinFluP'])
                    aux.append(row_aux['LinFluP'])
                    aux.append(row_aux['capacity'])
                line_sc_1filas_aux.append(Row(id=aux[0], time=aux[1], name=aux[2], bus_a=aux[3], bus_b=aux[4], flow=aux[5], value=aux[6], capacity=aux[7]))

            line_sc_1_aux = spark.createDataFrame(line_sc_1filas_aux)
            line_sc_1_aux.write.json(linpath + f"/line_{idaux}.json")

    for hidronum, hidroname in enumerate(hidrolist):
        dflinesaux = plplin.filter(col('Hidro') == hidroname)
        dflinelist = dflinesaux
        print(f"{((hidronum + 1) / len(hidrolist)) * 100}% Completado")
        linescenariofunction(dflinelist, linescenariolist[hidronum])




    print("Creando Archivos Reservoirs en Scenario \n")
    def resscenariofunction(dfreslist, respath):
        for x in range(nres):
            res_sc_filas_aux = []
            idaux = indexres.where(col('id') == x).first()['id']
            name = indexres.where(col('id') == x).first()['EmbName']
            junction_id = junctionsinfo.filter(col('CenName') == name).first()['id']
            for y in range(1, time + 1):  # Para cada bloque de tiempo, se agrega un estado del embalse x
                aux = []
                aux.append(y)
                aux.append(idaux)
                aux.append(junction_id)
                aux.append(name)
                row_aux = dfreslist.where((col('id') == x) & (col('time') == y)).first()
                if row_aux is None:
                    for i in range(2):
                        aux.append(0)
                else:
                    aux.append((row_aux['EmbFac'] * row_aux['EmbVfin']) / 1000000)
                    aux.append(aux[-1])
                res_sc_filas_aux.append(Row(time=aux[0], id=aux[1], junction_id=aux[2], name=aux[3], level=aux[4], value=aux[5]))
                
            res_sc_1_aux = spark.createDataFrame(res_sc_filas_aux)
            res_sc_1_aux.write.json(respath + f"/reservoir_{idaux}.json")

    for hidronum, hidroname in enumerate(hidrolist):
        dfresaux = reservoirs.filter(col('Hidro') == hidroname)
        dfreslist = dfresaux
        print(f"{((hidronum+1) / len(hidrolist)) * 100}% Completado")
        resscenariofunction(dfreslist, reservoirscenariolist[hidronum])

spark.stop