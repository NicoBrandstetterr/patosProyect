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

