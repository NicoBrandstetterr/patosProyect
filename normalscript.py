import pandas as pd
import numpy as np
import json
from pathlib import Path
import random
import os
import pyproj
from IPython.display import display

if __name__ == "__main__":
	# Dirección donde se ubican los archivos que se cargarán
	path_data='data_test/'

	# Nombre que tendrá el caso
	namedata='PLPNORMAL'

	print("---------------------------------- Iniciando Carga de archivos-----------------------------------\n")


	print("---------------------------------- Iniciando Carga de archivos-----------------------------------\n")

	print("Este proceso puede demorar unos minutos dependiendo del tamaño de los archivos\n")

	print("--------Cargando Archivo plpbar-------------\n")
	plpbar=pd.read_csv(path_data+'plpbar.csv')
	plpbar.columns=["Hidro","time","TipoEtapa","id","BarName","CMgBar","DemBarP","DemBarE","PerBarP","PerBarE","BarRetP","BarRetE"]
	plpbar['BarName']=plpbar['BarName'].str.replace(" ","")
	plpbar["Hidro"] = plpbar["Hidro"].str.replace(" ", "")

	indexbus=plpbar[['id','BarName']].drop_duplicates(keep="first").reset_index(drop=True)

	print("--------Archivo plpbar cargado-------------\n")


	print("--------Cargando Archivo de ubicaciones Ubibar-------------\n")
	ubibar=pd.read_csv(path_data+'ubibar.csv',sep=';')
	ubibar=ubibar.drop('ID',axis=1)
	ubibar['LATITUD']=ubibar['LATITUD'].apply(lambda x:x.replace(',','.')).apply(float)
	ubibar['LONGITUD']=ubibar['LONGITUD'].apply(lambda x:x.replace(',','.')).apply(float)
	ubibar.columns=["BarName","latitud","longitud"]
	ubibar['BarName']=ubibar['BarName'].str.replace(" ","")
	print("--------Archivo de ubicaciones Ubibar Cargado-------------\n")

	print("--------Cargando Archivo plpcen-------------\n")
	plpcen=pd.read_csv(path_data+'plpcen.csv')
	plpcen.columns=["Hidro","time","TipoEtapa","id","CenName","tipo","bus_id","BarName","CenQgen","CenPgen","CenEgen","CenInyP","CenInyE","CenRen","CenCVar","CenCostOp","CenPMax"]
	plpcen['CenName']=plpcen["CenName"].str.replace(" ","")
	plpcen=plpcen.drop(["CenEgen","CenInyP","CenInyE","CenRen","CenCostOp","CenPMax"],axis=1)
	plpcen["Hidro"] = plpcen["Hidro"].str.replace(" ", "")
	plpcen['tipo']="otros"

	indexcen=plpcen[['id','CenName','tipo','bus_id']].drop_duplicates(keep="first").reset_index(drop=True)

	print("--------Archivo plpcen cargado-------------\n")


	print("--------Cargando Archivo centralesinfo-------------\n")
	centralsinfo=pd.read_csv(path_data+'centralesinfo.csv',sep=';')
	centralsinfo.columns=['id','CenName','type','CVar','effinciency','bus_id','serie_hidro_gen','serie_hidro_ver','min_power','max_power',"VembIn","VembFin","VembMin","VembMax","cotaMínima"]

	cols = ['min_power', 'max_power', 'effinciency', 'CVar', 'VembIn', 'VembFin', 'VembMin', 'VembMax', 'cotaMínima']
	centralsinfo['CenName'] = centralsinfo["CenName"].str.replace(" ", "")
	for col in cols:
		centralsinfo[col] = centralsinfo[col].replace(",", ".", regex=True)
	
		
	print("--------Archivo centralesinfo cargado-------------\n")



	print("--------Cargando Archivo plplin-------------\n")
	plplin=pd.read_csv(path_data+'plplin.csv')
	# Cambiando los nombres de las columnas
	plplin.columns=["Hidro","time","TipoEtapa","id","LinName","bus_a","bus_b","LinFluP","LinFluE","capacity","LinUso","LinPerP","LinPerE","LinPer2P","LinPer2E","LinITP","LinITE"]
	plplin['LinName']=plplin['LinName'].str.replace(" ","")
	plplin["Hidro"] = plplin["Hidro"].str.replace(" ", "")

	indexlin=plplin[['id','LinName',"bus_a","bus_b"]].drop_duplicates(keep="first").reset_index(drop=True)

	print("--------Archivo plplin Cargado-------------\n")



	print("--------Cargando Archivo linesinfo-------------\n")
	linesinfo=pd.read_csv(path_data+'linesinfo.csv',sep=';')
	linesinfo.columns=["id","LinName","bus_a","bus_b","max_flow_a_b","max_flow_b_a","voltage","r","x","segments"]
	linesinfo['LinName']=linesinfo['LinName'].str.replace(" ","")
	linesinfo['max_flow_a_b']=(linesinfo["max_flow_a_b"].apply(str)).apply(lambda x:x.replace(',','.')).apply(float)
	linesinfo['max_flow_b_a']=(linesinfo['max_flow_b_a'].apply(str)).apply(lambda x:x.replace(',','.')).apply(float)
	linesinfo['r']=(linesinfo['r'].apply(str)).apply(lambda x:x.replace(',','.')).apply(float)
	linesinfo['x']=(linesinfo['x'].apply(str)).apply(lambda x:x.replace(',','.')).apply(float)

	linesfinal=indexlin.drop(['id','bus_a','bus_b'],axis=1).merge(linesinfo,on='LinName')
	linesfinal['id']=(linesfinal['id']).apply(int)

	print("--------Archivo linesinfo Cargado-------------\n")



	# Creando directorios

	electricTopology=namedata+'/Topology/Electric'
	hydricTopology=namedata+'/Topology/Hydric'

	os.makedirs(electricTopology,exist_ok=True)
	os.makedirs(hydricTopology,exist_ok=True)


	hidrolist=plpbar['Hidro'].unique()
	busscenariolist=[]
	centralscenariolist=[]
	linescenariolist=[]
	for hidronum in range(len(hidrolist)):
		# Creamos los directorios
		busscenario= namedata+f'/Scenarios/{hidronum+1}/Bus'
		centralscenario=namedata+f'/Scenarios/{hidronum+1}/Centrals'
		linescenario=namedata+f'/Scenarios/{hidronum+1}/Lines'

		os.makedirs(busscenario,exist_ok=True)
		busscenariolist.append(busscenario)

		os.makedirs(centralscenario,exist_ok=True)
		centralscenariolist.append(centralscenario)

		os.makedirs(linescenario,exist_ok=True)
		linescenariolist.append(linescenario)


	
	hydrofile = [x for x in range(1,len(hidrolist)+1)]

	with open( namedata+'/Scenarios/hydrologies.json', 'w') as f:
		json.dump(hydrofile, f)


	# Variables indicadoras de cantidades

	# Número de horas de bloques temporales del proyecto
	time=plplin['time'].max()

	# Número de barras
	nbus=len(indexbus['id'])
	lbus=list(indexbus['id'])

	# Número de generadores
	ngen=len(indexcen['id'])

	# Número de lineas
	nlin=len(indexlin['id'])

	print("Creando Archivos Bus en Scenario \n")
	# Bus contiene:
	'''
			(*) id <int>: identificador de la barra 
			(*) time <int>: instante de registro
			(*) name <str>: nombre de la barra
			marginal_cost <float>: costo marginal, genera el gráfico de costo
						[USD/MWh]
			DemBarE <float>: construye el gráfico de demanda de Energía [MWh]
			DemBarP <float>: construye el gráfico de demanda de Potencia [MW]
			Value <float>: mismo valor que marginal_cost [MWh]
	'''

	def busscenariofunction(dfbusauxlist,pathbus):
		for x in range(nbus): # Para cada barra

			bus_sc_1filas_aux=[]
			for y in range(1,time+1): # Para cada bloque de tiempo, se agrega un estado de la barra x
				aux=[]
				
				idbus=indexbus['id'][x]
				aux.append(idbus)
				aux.append(y)
				aux.append(indexbus['BarName'][x])
				aux.append(dfbusauxlist[x]['CMgBar'][y-1])
				aux.append(aux[-1])
				aux.append(dfbusauxlist[x]['DemBarE'][y-1])
				aux.append(dfbusauxlist[x]['DemBarP'][y-1])
				aux.append(dfbusauxlist[x]['BarRetP'][y-1])
				bus_sc_1filas_aux.append(aux)
			bus_sc_1_aux=pd.DataFrame(bus_sc_1filas_aux,columns=['id','time','name','marginal_cost','value','DemBarE','DemBarP','BarRetP'])
			bus_sc_1_aux.to_json(pathbus+f"/bus_{idbus}.json",orient='records')

	for hidronum,hidroname in enumerate(hidrolist):
		
		dfbussauxx=plpbar.query(f"(Hidro=='{hidroname}')").reset_index()
		dfbuslist=[]
		for x in lbus:
			idaux=x
			dfbuslist.append(dfbussauxx[dfbussauxx.id==idaux].reset_index(drop=True))
		print(f"{((hidronum+1)/len(hidrolist))*100}% Completado")
		busscenariofunction(dfbuslist,busscenariolist[hidronum])

	print("Archivos Bus en Scenario creados\n")

	print("Creando Archivos Central en Scenario \n")
	# Centrals contiene:
	'''
			(*) id <int>: identificador del generador
			(*) time <int>: instante de registro
			(*) bus_id <int>: identificador de la barra a la que se conecta
			(*) name <str>: nombre del generador
			CenPgen <float>: energía generada en el instante time [MW]
			value <float>: mismo valor que CenPgen [MW]
			(?) CenCVar <unknown>: parámetro no identificado
			(?) CenQgen <unknown>: parámetro no identificado
			
	'''
	def centralscenariofunction(dfcenauxlist,cenpath):
		
		for x in range(ngen): # Para cada generador (central)
			if indexcen['bus_id'][x]==0 or np.isnan(indexcen['bus_id'][x]): # No existe la barra 0, por lo que no se consideran dichos generadores
				pass

			else:
				central_sc_1filas_aux=[]
				for y in range(1,time+1): # Para cada bloque de tiempo, se agrega un estado del generador x
					aux=[]
					aux.append(indexcen['id'][x])
					aux.append(y)
					aux.append(int(indexcen['bus_id'][x]))
					aux.append(indexcen['CenName'][x])
					if len(dfcenauxlist[x])==0:
						for i in range(4):
							aux.append(0)
					else:
						aux.append(dfcenauxlist[x]['CenPgen'][y-1])
						aux.append(aux[-1])
						aux.append(dfcenauxlist[x]['CenCVar'][y-1])
						aux.append(dfcenauxlist[x]['CenQgen'][y-1])
					central_sc_1filas_aux.append(aux)
				central_sc_1_aux=pd.DataFrame(central_sc_1filas_aux,columns=['id','time','bus_id','name','CenPgen','value','CenCVar','CenQgen'])
				central_sc_1_aux.to_json(cenpath+f"/central_{indexcen['id'][x]}.json",orient='records')

	for hidronum,hidroname in enumerate(hidrolist):
		
		dfcensauxx=plpcen.query(f"(Hidro=='{hidroname}')").reset_index()
		dfcenlist=[]
		for x in range(ngen):
				idaux=indexcen['id'][x]
				dfcenlist.append(dfcensauxx[dfcensauxx.id==idaux].reset_index(drop=True))
		print(f"{((hidronum+1)/len(hidrolist))*100}% Completado")
		centralscenariofunction(dfcenlist,centralscenariolist[hidronum])

	print("Archivos Central en Scenario creados \n")

	print("Creando Archivos Lineas en Scenario \n")
	'''

			(*) id <int>: identificador de la linea 
			(*) time <int>: instante de registro
			(*) bus_a <int>: identificador de la barra de origen
			(*) bus_b <int>: identificador de la barra de destino
			flow <float>: flujo en el instante time [MW]
			value <float>: mismo valor que flow [MW]
			
	'''
	# if not Path('linesscenariolist.pickle').is_file():
	def linescenariofunction(dflinelist,linpath):
		for x in range(nlin): # Para cada linea
			line_sc_1filas_aux=[]
			for y in range(1,time+1): # Para cada bloque de tiempo, se agrega un estado de la linea x
				aux=[]
				idaux=linesfinal['id'][x]
				bus_a_id = linesfinal['bus_a'][x]
				bus_b_id = linesfinal['bus_b'][x]
				name = f"{indexbus.BarName[bus_a_id - 1]}->{indexbus.BarName[bus_b_id - 1]}"
				aux.append(idaux)
				aux.append(y)
				aux.append(name)
				aux.append(bus_a_id)
				aux.append(bus_b_id)
				aux.append(dflinelist[x]['LinFluP'][y-1])
				aux.append(aux[-1])
				aux.append(dflinelist[x]['capacity'][y-1])

				
				line_sc_1filas_aux.append(aux)
			line_sc_1_aux=pd.DataFrame(line_sc_1filas_aux,columns=['id','time',"name",'bus_a','bus_b','flow','value','capacity'])
			line_sc_1_aux.to_json(linpath+f"/line_{idaux}.json",orient='records')

	for hidronum,hidroname in enumerate(hidrolist):
		
		dflinesaux=plplin.query(f"(Hidro=='{hidroname}')").reset_index()
		dflinelist=[]
		for x in range(nlin):
			idaux=linesfinal['id'][x]
			dflinelist.append(dflinesaux[dflinesaux.id==idaux].reset_index(drop=True))
		print(f"{((hidronum+1)/len(hidrolist))*100}% Completado")
		linescenariofunction(dflinelist,linescenariolist[hidronum])
	print("Archivos Line en Scenario creados \n")
