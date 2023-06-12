from pathlib import Path
import pandas as pd
import numpy as np
import random
import pyproj
import shutil
import json
import os


# Ubicación de la carpeta con los archivos

path_corr=False
path_data="data_test/"

print("en hora buena\n")

namedata = "PLPTEST"

print("---------------------------------- Iniciando Carga de archivos-----------------------------------\n")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim,regexp_replace
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

spark = SparkSession.builder.getOrCreate()

# Cargar el archivo csv en un DataFrame de Spark
plpbarsk = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(path_data+'plpbar.csv')

# Cambiar los nombres de las columnas
plpbarsk = plpbarsk.toDF("Hidro","time","TipoEtapa","id","BarName","CMgBar","DemBarP","DemBarE","PerBarP","PerBarE","BarRetP","BarRetE")

# Eliminar los espacios en las columnas 'BarName' y 'Hidro'
plpbarsk = plpbarsk.withColumn('BarName', regexp_replace('BarName', ' ', ''))
plpbarsk = plpbarsk.withColumn('Hidro', regexp_replace('Hidro', ' ', ''))
plpbarsk = plpbarsk.withColumn("id", col("id").cast("int"))
plpbarsk = plpbarsk.withColumn("time", col("time").cast("int"))
# Seleccionar las columnas 'id' y 'BarName'
indexbussk = plpbarsk.select("id", "BarName").dropDuplicates()

ubibarsk = spark.read.options(delimiter=';').format('csv').option('header', 'true').option('inferSchema', 'true').load(f'{path_data}/ubibar.csv')
ubibarsk = ubibarsk.drop('ID')
ubibarsk = ubibarsk.withColumn('LATITUD', regexp_replace('LATITUD', ',', '.').cast('float'))
ubibarsk = ubibarsk.withColumn('LONGITUD', regexp_replace('LONGITUD', ',', '.').cast('float'))
ubibarsk = ubibarsk.toDF("BarName","latitud","longitud")
ubibarsk = ubibarsk.withColumn('BarName',regexp_replace('BarName', " ", ""))





# Nombre data
electricTopology=namedata+'/Topology/Electric'
hydricTopology=namedata+'/Topology/Hydric'

os.makedirs(electricTopology,exist_ok=True)
os.makedirs(hydricTopology,exist_ok=True)

# pyspark
hidrolist=[row.Hidro for row in plpbarsk.select("Hidro").distinct().collect()]
busscenariolist=[]
centralscenariolist=[]
linescenariolist=[]
reservoirscenariolist=[]
for hidronum in range(len(hidrolist)):
	# Creamos los directorios
	busscenario= namedata+f'/Scenarios/{hidronum+1}/Bus'
	centralscenario=namedata+f'/Scenarios/{hidronum+1}/Centrals'
	linescenario=namedata+f'/Scenarios/{hidronum+1}/Lines'
	reservoirscenario=namedata+f'/Scenarios/{hidronum+1}/Reservoirs'

	os.makedirs(busscenario,exist_ok=True)
	busscenariolist.append(busscenario)

	os.makedirs(centralscenario,exist_ok=True)
	centralscenariolist.append(centralscenario)

	os.makedirs(linescenario,exist_ok=True)
	linescenariolist.append(linescenario)

	os.makedirs(reservoirscenario,exist_ok=True)
	reservoirscenariolist.append(reservoirscenario)

marginal_cost_path=namedata+f'/Scenarios/Marginal_cost_percentil'
line_flow_percentil_path=namedata+f'/Scenarios/Flow_Line_percentil'
generation_sistem_path=namedata+f'/Scenarios/Generation_system'
os.makedirs(marginal_cost_path,exist_ok=True)
os.makedirs(line_flow_percentil_path,exist_ok=True)
os.makedirs(generation_sistem_path,exist_ok=True)
hydrofile = [x for x in range(1,len(hidrolist)+1)]

with open( namedata+'/Scenarios/hydrologies.json', 'w') as f:
  json.dump(hydrofile, f)

nbus = indexbussk.count()
lbus = [row.id for row in indexbussk.collect()]
time = plpbarsk.select("time").agg({"time": "max"}).collect()[0][0]


def busscenariofunction(dfbusauxlist: list, pathbus: str):
	for x in range(nbus): # Para cada barra

		bus_sc_1filas_aux=[]
		for y in range(1,time+1): # Para cada bloque de tiempo, se agrega un estado de la barra x
			aux=[]
			idbus = indexbussk.filter(col('id') == lbus[x]).select('id').collect()[0][0]
			aux.append(idbus)
			aux.append(y)
			aux.append(indexbussk.filter(col('id') == lbus[x]).select('BarName').collect()[0][0])
			aux.append(dfbusauxlist[x].filter(col('CMgBar')).select('CMgBar').collect()[y - 1][0])
			aux.append(aux[-1])
			aux.append(dfbusauxlist[x].filter(col('DemBarE')).select('DemBarE').collect()[y - 1][0])
			aux.append(dfbusauxlist[x].filter(col('DemBarP')).select('DemBarP').collect()[y-1][0])
			aux.append(dfbusauxlist[x].filter(col('DemRetP')).select('DemRetP').collect()[y-1][0])

			bus_sc_1filas_aux.append(aux)
		bus_sc_1_aux = spark.createDataFrame(bus_sc_1filas_aux, ['id', 'time', 'name', 'marginal_cost', 'value', 'DemBarE', 'DemBarP', 'BarRetP'])
		bus_sc_1_aux.write.json(f"{pathbus}/bus_{idbus}.json")

for hidronum,hidroname in enumerate(hidrolist):
	
	dfbussauxx=plpbarsk.filter(col('Hidro') == hidroname)
	dfbuslist=[]
	for x in lbus:
		idaux=x
		dfbuslist.append(dfbussauxx.filter(col('id') == idaux))
	print(f"{((hidronum+1)/len(hidrolist))*100}% Completado")
	busscenariofunction(dfbuslist,busscenariolist[hidronum])