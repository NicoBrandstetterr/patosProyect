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
path_data=None

while not path_corr:
    path_data=input("Ingrese la ruta de la carpeta en donde se ubican los archivos CSV: \n")
    aux=input(f"Se ha ingresado la siguiente ruta: '{path_data}', de ser correcta, escriba 'y', si no, escriba 'n' y vuelva a agregar el path \n")
    if aux == "y":
        path_corr=True

print("en hora buena\n")

namedata=input("A continuación, escriba el nombre de la carpeta en donde se encontrarán los archivos Json que se crearán\n")

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

hydric_adicional = pd.read_csv(path_data+'hydric_adicional.csv',sep=";")

tiposcentrales=pd.read_csv(path_data+'centralestype.csv', encoding="latin-1").rename(columns={'cen_name':'CenName'})
typecentrals=indexcen.merge(tiposcentrales,on='CenName')

for x in range(len(indexcen['id'])):
    tipo=typecentrals[typecentrals['CenName']==indexcen['CenName'][x]]['cen_type'].values
    
    if len(tipo)>0:
        plpcen.loc[plpcen['id'] == indexcen['id'][x], 'tipo'] = tipo[0]
	
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

print("--------Cargando Archivo Reservoirs-------------\n")

reservoirs = pd.read_csv(path_data+'plpemb.csv')
reservoirs.rename(columns={'Bloque': 'time', 'EmbNum': 'id', 'EmbNom': 'EmbName'}, inplace=True)
reservoirs['EmbName']=reservoirs['EmbName'].str.replace(" ","")
reservoirs['Hidro']=reservoirs['Hidro'].str.replace(" ","")

indexres = reservoirs[['id','EmbName']].drop_duplicates(keep="first").reset_index(drop=True)

junctionsinfo=centralsinfo[centralsinfo['type'].isin(["E",'S','R'])].reset_index(drop=True)
reservoirsinfo=centralsinfo[centralsinfo['type'].isin(["E"])].reset_index(drop=True)
reservoirsinfo.rename(columns={'CenName':'EmbName'}, inplace=True)

for i, emb_name in enumerate(reservoirsinfo['EmbName']):
    if emb_name in indexres['EmbName'].values:
        idx = indexres.index[indexres['EmbName'] == emb_name][0]
        reservoirsinfo.at[i, 'id'] = indexres.at[idx, 'id']

print("--------Archivo Reservoirs Cargado-------------\n")


print("--------Cargando Archivo indhor-------------\n")
indhor = pd.read_csv(path_data+'indhor.csv',encoding='latin-1')
print("--------Archivo indhor Cargado-------------\n")

# Creando directorios

electricTopology=namedata+'/Topology/Electric'
hydricTopology=namedata+'/Topology/Hydric'

os.makedirs(electricTopology,exist_ok=True)
os.makedirs(hydricTopology,exist_ok=True)


hidrolist=plpbar['Hidro'].unique()
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

# Número de Reservoirs
nres = len(reservoirs['EmbName'].unique())

# Función generadora de latitudes y longitudes

def aleatory_direction():
    latitud=-random.uniform(10, 85)
    longitud=-random.uniform(10, 85)
    return latitud,longitud

def LatLon_To_XY(Lat,Lon):
  B = pyproj.Transformer.from_crs(4326,20049) #WGS84->EPSG:20049 (Chile 2021/UTM zone 19S)
  UTMx, UTMy = B.transform(Lat,Lon)
  return UTMx, UTMy

def XY_To_LatLon(x,y):
  B = pyproj.Transformer.from_crs(20049,4326)
  Lat, Lon = B.transform(x,y)
  return Lat, Lon

def valorXY(LatP, LonP, scale):
  A = LatLon_To_XY(LatP, LonP)
  X,Y = A[0]*scale, A[1]*scale
  return Y,X


print("A continuación se iniciará el proceso de creación de la carpeta Scenarios, este proceso en total puede llegar a tomar más de 1 hora 30 minutos\n")

print("Creando archivos de Bloques a Fechas")
indhor2=indhor.drop('Hora',axis=1).groupby(['Año','Mes'])
indhorlist=[]
for x in indhor2:
    indhorlist.append([str(x[1]['Bloque'].min()),str(x[1]['Bloque'].max()),str(x[0])])
with open( namedata+'/Scenarios/indhor.json', 'w') as f:
  json.dump(indhorlist, f)


print("Creando archivos generación por Sistema por Hidrología")

typegenlist=typecentrals.cen_type.unique()
for i,hydro in enumerate(hidrolist):
    print(hydro+" lista")
    dic_type_gen={}
    auxdf = plpcen[plpcen['Hidro']==hydro]
    auxdf=auxdf.groupby(['tipo','time'])['CenPgen'].sum().reset_index().groupby('tipo')
    for group in auxdf:
        tipo = group[0]
        df_tipo = group[1]
        dic_type_gen[tipo] = [row for row in df_tipo[['time', 'CenPgen']].to_dict(orient='records')]

    
    with open(generation_sistem_path+f'/generation_system_{i+1}.json', 'w') as f:
        json.dump(dic_type_gen, f)
	
print("Creando archivos para Grafico Percentiles Costo Marginal")
def percentilCM():
    datos_bar = plpbar[['Hidro', 'time','id', 'BarName', 'CMgBar']]
    lista_bar = datos_bar.BarName.unique()

    i=1
    for barra in lista_bar:
        print(f'Procesando datos de {barra} [{i}/{len(lista_bar)}]')
        data_barraTx = datos_bar.loc[(datos_bar.BarName == barra)]
        idbar=data_barraTx['id'].unique()[0]
        data_barraTx = data_barraTx[~(data_barraTx['Hidro'] == 'MEDIA')]
        Promedio = data_barraTx[['time','CMgBar']]
        xy =Promedio.groupby(['time']).mean()
        
        data_barraTx = data_barraTx.groupby(['time']).agg(perc0=('CMgBar', lambda x: x.quantile(0.0)),
                                                                perc20=(
                                                                    'CMgBar', lambda x: x.quantile(0.2)),
                                                                perc80=(
                                                                    'CMgBar', lambda x: x.quantile(0.8)),
                                                                perc100=('CMgBar', lambda x: x.quantile(1)))

        data_barraTx['promedio'] = xy
        data_barraTx = data_barraTx.assign(name=barra)
        data_barraTx = data_barraTx.assign(id=idbar)
        data_barraTx.reset_index(inplace=True)
        data_barraTx=data_barraTx[['id','time','name','perc0','perc20','perc80','perc100','promedio']]
        data_barraTx.to_json(marginal_cost_path+f"/bus_{idbar}.json",orient='records')
        i=i+1

percentilCM()


print("Creando archivos para Grafico Percentiles flujos de lineas de transmisión")
def percentilFL():
    datos_lineas=plplin[['id','Hidro', 'time', 'LinName', 'LinFluP', 'capacity']]
    lista_lineas = datos_lineas.LinName.unique()
    n_lineas = len(lista_lineas)
    i=1
    for linea in lista_lineas:
        print(f'Procesando datos de {linea} [{i}/{n_lineas}]')
        data_lineaTx = datos_lineas.loc[(datos_lineas.LinName == linea)]
        idlin=data_lineaTx['id'].unique()[0]
        data_lineaTx = data_lineaTx[~(data_lineaTx['Hidro'] == 'MEDIA')]
        fluMax = data_lineaTx[['time','capacity']]
        xy =-fluMax.groupby(['time']).max()
        data_lineaTx = data_lineaTx.groupby(['time']).agg(perc0=('LinFluP', lambda x: x.quantile(0.0)),
                                                                perc20=(
                                                                    'LinFluP', lambda x: x.quantile(0.2)),
                                                                perc80=(
                                                                    'LinFluP', lambda x: x.quantile(0.8)),
                                                                perc100=('LinFluP', lambda x: x.quantile(1)))

        data_lineaTx['Min'] = xy
        data_lineaTx['Max'] = -xy
        i = i+1
        data_lineaTx.reset_index(inplace=True)
        data_lineaTx = data_lineaTx.assign(id=idlin)
        data_lineaTx = data_lineaTx.assign(LinName = linea)
        data_lineaTx.to_json(line_flow_percentil_path+f"/line_{idlin}.json",orient='records')

percentilFL()


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

print("Creando Archivos Reservoirs en Scenario \n")

# Resevoirs contiene:
'''
		(*) time <int>: instante de registro
		(*) id <int>: identificador del embalse
		(*) junction_id <int>: identificador del canal al que se conecta
		(*) name <str>: nombre del embalse
		level <float>: nivel en el instante time
		value <float>: mismo valor que level
'''

def resscenariofunction(dfreslist,respath):
	for x in range(nres):
		res_sc_filas_aux=[]
		idaux=indexres['id'][x]
		name=indexres['EmbName'][x]
		junction_id = junctionsinfo[junctionsinfo['CenName']==name]['id'].values[0]
		for y in range(1,time+1): # Para cada bloque de tiempo, se agrega un estado del embalse x
			aux=[]
			aux.append(y)
			aux.append(idaux)
			aux.append(junction_id)
			aux.append(name)
			aux.append((dfreslist[x]['EmbFac'][y-1]*dfreslist[x]['EmbVfin'][y-1])/1000000)
			aux.append(aux[-1])
			res_sc_filas_aux.append(aux)

		res_sc_1_aux=pd.DataFrame(res_sc_filas_aux,columns=['time','id','junction_id','name','level','value'])
		res_sc_1_aux.to_json(respath+f"/reservoir_{idaux}.json",orient='records')

for hidronum,hidroname in enumerate(hidrolist):
	dfresaux=reservoirs.query(f"(Hidro=='{hidroname}')").reset_index()
	dfreslist=[]
	for x in range(nres):
		idaux=indexres['id'][x]
		dfreslist.append(dfresaux[dfresaux.id==idaux].reset_index(drop=True))
	print(f"{((hidronum+1)/len(hidrolist))*100}% Completado")
	resscenariofunction(dfreslist,reservoirscenariolist[hidronum])

print("Archivos Reservoir en Scenario creados \n")



print("Ahora se procede a crear los Archivos correspondientes a la Topología y datos estáticos de los elementos")

print("Creando datos topologicos Bus")

ubibar[['latUTM','lonUTM']]=ubibar.apply(lambda row: valorXY(row['latitud'],row['longitud'],scale=0.00001),axis=1,result_type='expand')
dirdfbus=ubibar
# bus electric contiene:

'''   
		(*) id <int>: identificador de la barra
		(*) name <str>: nombre de la barra
		longitude <float>
		latitude <float>
		active <int>: indica si la barra está activa
'''
escalador = 1
auxiliar=[]
buselectricfilas_aux=[]
for x in range(nbus): # Para cada barra (bus)
	if dirdfbus['BarName'].isin([indexbus['BarName'][x]]).tolist().count(True)>0:
		# latitud=float(dirdfbus[dirdfbus['BarName']==indexbus['BarName'][x]]['latUTM'].values[0])
		# longitud=float(dirdfbus[dirdfbus['BarName']==indexbus['BarName'][x]]['lonUTM'].values[0])
		latitud=float(dirdfbus[dirdfbus['BarName']==indexbus['BarName'][x]]['latitud'].values[0])*escalador
		longitud=float(dirdfbus[dirdfbus['BarName']==indexbus['BarName'][x]]['longitud'].values[0])*escalador
	else:
		auxiliar.append(indexbus['BarName'][x])
		latitud,longitud=aleatory_direction()
		# latitud,longitud=valorXY(latitud,longitud,scale=0.00001)

	aux=[]
	aux.append(indexbus['id'][x])
	aux.append(indexbus['BarName'][x])
	aux.append(longitud)
	aux.append(latitud)
	aux.append(1)
	buselectricfilas_aux.append(aux)

buselectric=pd.DataFrame(buselectricfilas_aux,columns=['id','name','longitude','latitude','active'])

buselectric.to_json(electricTopology+"/bus.json",orient='records')

print("Creando datos topologicos Central")

# centrals electric contiene:

'''   
        (*) id <int>: identificador del generador
		(*) bus_id <int>: id de la barra conectada al generador
		(*) name <str>: nombre del generador
		active <int>: indica si el generador está activo
		capacity <float>: capacidad del generador [MW]
		min_power <float>: generación mínima [MW]
		max_power <float>: generación máxima [MW]
		type <str>: tipo de generador
		longitude <float>
		latitude <float>
		(?) effinciency <float>: Rendimiento [MWh/m3s]
		(?) flow <float>: parámetro no identificado
		(?) rmin <float>: parámetro no identificado
		(?) rmax <float>: parámetro no identificado
		(?) cvar <float>: Costo Variable
		(?) cvnc <unknown>: parámetro no identificado
		(?) cvc <unknown>: parámetro no identificado
		(?) entry_date <unknown>: parámetro no identificado

'''

centralselectricfilas_aux=[]
for x in range(ngen): # Para cada generador (central)
	if indexcen['bus_id'][x]==0 or np.isnan(indexcen['bus_id'][x]): # No existe la barra 0, por lo que no se consideran dichos generadores
		pass
	else:
		latitud,longitud=None,None
		aux=[]
		aux.append(indexcen['id'][x])
		aux.append(int(indexcen['bus_id'][x]))
		aux.append(indexcen['CenName'][x])
		aux.append(1)
		# capacidad
		aux.append(0)
		aux.append(centralsinfo[centralsinfo['CenName']==indexcen['CenName'][x]]['min_power'])
		aux.append(centralsinfo[centralsinfo['CenName']==indexcen['CenName'][x]]['max_power'])
		tipo=typecentrals[typecentrals['CenName']==indexcen['CenName'][x]]['cen_type'].values
		if len(tipo)>0:
			aux.append(tipo[0])
		else:
			aux.append(None)
		aux.append(longitud)
		aux.append(latitud)
		aux.append(centralsinfo[centralsinfo['CenName']==indexcen['CenName'][x]]['effinciency'])
		for x in range(7):
			aux.append(0)
		centralselectricfilas_aux.append(aux)

centralelectric=pd.DataFrame(centralselectricfilas_aux,columns=['id','bus_id','name','active','capacity','min_power','max_power','type','longitude','latitude','efficiency','flow','rmin','rmax','cvar',
'cvnc','cvc','entry_date'])

centralelectric.to_json(electricTopology+"/centrals.json",orient='records')


print("Creando datos topologicos Lineas")
# Lines electric tiene:
'''  
        (*) id <int>: identificador de la línea
		(*) bus_a <int>: id de la barra origen
		(*) bus_b <int>: id de la barra destino
		active <int>: indica si la línea está activa
		capacity <float>: capacidad máxima de la línea [MW]  ->
		max_flow_a_b <float>: flujo máximo en dirección
					dispuesta [MW]
		max_flow_b_a <float>: flujo máximo en dirección
					contraria [MW]
		voltage <float>: voltaje de la línea [kV]
		r <float>: resistencia de la línea [Ω]
		x <float>: reactancia de la línea [Ω]
		(? )segments <int>: parámetro no identificado
		(?) entry_date <unknown>: parámetro no identificado
		(?) exit_date <unknown>: parámetro no identificado

'''

lineselectricfilas_aux=[]
for x in range(nlin): # Para cada linea
	aux=[]
	bus_a_id = linesfinal['bus_a'][x]
	bus_b_id = linesfinal['bus_b'][x]
	name = f"{indexbus.BarName[bus_a_id - 1]}->{indexbus.BarName[bus_b_id - 1]}"
	aux.append(linesfinal['id'][x])
	aux.append(name)
	aux.append(bus_a_id)
	aux.append(bus_b_id)
	aux.append(1)
	# capacidad
	aux.append(0)
	aux.append(linesfinal['max_flow_a_b'][x])
	aux.append(linesfinal['max_flow_b_a'][x])
	aux.append(linesfinal['voltage'][x])
	aux.append(linesfinal['r'][x])
	aux.append(linesfinal['x'][x])
	aux.append(linesfinal['segments'][x])
	aux.append(None)
	aux.append(None)
	
	lineselectricfilas_aux.append(aux)

lineelectric=pd.DataFrame(lineselectricfilas_aux,columns=['id','name','bus_a','bus_b','active','capacity','max_flow_a_b','max_flow_b_a','voltage','r','x','segments','entry_date','exit_date'])

lineelectric.to_json(electricTopology+"/lines.json",orient='records')


print("Creando datos topologicos Reservoirs")

'''   
        (*) id <int>: identificador del embalse
		(*) junction_id <int>: id del embalse relacionada (mismo valor id)
		(*) name <str>: nombre del embalse
		(*) type <str>: tipo de embalse
		min_vol <float>: volumen mínimo del embalse
		max_vol <float>: volumen máximo del embalse
		start_vol <float>: volumen inicial del embalse
		end_vol <float>: volumen final del embalse
		active <bool>: indica si el embalse está activo
		(?) hyd_independant <bool>: parámetro no identificado
		(?) future_cost <unknown>: parámetro no identificado
		(?) cmin <unknown>: cota m.s.n.m mínima


'''


reshydricfilas_aux=[]
for x in range(nres): # Para cada linea
	aux=[]
	idaux=indexres['id'][x]
	name=indexres['EmbName'][x]
	junction_id = junctionsinfo[junctionsinfo['CenName']==name]['id'].values[0]
	
	aux.append(idaux)
	aux.append(junction_id)
	aux.append(name)
	aux.append(reservoirsinfo[reservoirsinfo['id']==idaux]['type'].values[0])
	aux.append(reservoirsinfo[reservoirsinfo['id']==idaux]['VembMin'].values[0])
	aux.append(reservoirsinfo[reservoirsinfo['id']==idaux]['VembMax'].values[0])
	aux.append(reservoirsinfo[reservoirsinfo['id']==idaux]['VembIn'].values[0])
	aux.append(reservoirsinfo[reservoirsinfo['id']==idaux]['VembFin'].values[0])
	aux.append(1)
	aux.append(0)
	aux.append(None)
	aux.append(reservoirsinfo[reservoirsinfo['id']==idaux]['cotaMínima'].values[0])
	reshydricfilas_aux.append(aux)

reshydric=pd.DataFrame(reshydricfilas_aux,columns=['id','junction_id','name','type','min_vol','max_vol','start_vol','end_vol','active','hyd_independant','future_cost','cmin'])

reshydric.to_json(hydricTopology+"/reservoirs.json",orient='records')

print("Creando datos topologicos Junctions")

'''
	(*) id <int>: identificador de la unión
	(*) name <str>: nombre de la unión
	longitude <float>
	latitude <float>
	active <bool>: indica si la barra está activa
	drainage <bool>: parámetro no identificado


'''

junctionhydricfilas_aux=[]
for x in range(len(junctionsinfo)): # Para cada junction
	latitud,longitud=aleatory_direction()
	aux=[]
	aux.append(junctionsinfo['id'][x])
	aux.append(junctionsinfo['CenName'][x])
	aux.append(longitud)
	aux.append(latitud)
	aux.append(1)
	aux.append(0)
	
	junctionhydricfilas_aux.append(aux)

junctionhydric=pd.DataFrame(junctionhydricfilas_aux,columns=['id','name','logitude','latitude','active','drainage'])

junctionhydric.to_json(hydricTopology+"/junctions.json",orient='records')

print("Creando datos topologicos Waterways")

'''
        (*) id <int>: identificador del canal
		(*) name <str>: nombre del canal
		(*) type <str>: tipo de waterway
		(*) junc_a_id <int>: id de la unión de origen
		(*) junc_b_id <int>: id de la unión de destino
		active <bool>: indica si el canal está activo
		(?) fmin <unknown>: parámetro no identificado
		(?) fmax <unknown>: parámetro no identificado
		(?) cvar <unknown>: parámetro no identificado 
        (?) delay <unknown>: parámetro no identificado

'''
junctionhydricfilas_aux=[]
countid=1
for x in range(len(junctionsinfo)):
    gen_id=junctionsinfo.serie_hidro_gen[x]
    ver_id=junctionsinfo.serie_hidro_ver[x]
    name_a = junctionsinfo.CenName[x]
    df_adicional = hydric_adicional[hydric_adicional['embalse'] == name_a]
    if not pd.isnull(gen_id):
        aux=[]
        aux.append(countid)
        countid+=1
        name_b = junctionsinfo[junctionsinfo['id']==gen_id].CenName.values[0]
        name = name_a+'_Gen_'+name_b
        aux.append(name)
        aux.append("generation")
        aux.append(junctionsinfo.id[x])
        aux.append(gen_id)
        aux.append(1)
        aux.append(None)
        aux.append(None)
        aux.append(None)
        aux.append(None)
        junctionhydricfilas_aux.append(aux)
    if not pd.isnull(ver_id):
        aux=[]
        aux.append(countid)
        countid+=1
        name_b = junctionsinfo[junctionsinfo['id']==ver_id].CenName.values[0]
        name = name_a+'_Vert_'+name_b
        aux.append(name)
        aux.append("spillover")
        aux.append(junctionsinfo.id[x])
        aux.append(ver_id)
        aux.append(1)
        aux.append(None)
        aux.append(None)
        aux.append(None)
        aux.append(None)
        junctionhydricfilas_aux.append(aux)
    if len(df_adicional)>0:
        for i in range(len(df_adicional)):
            tipo =df_adicional['type'].iloc[i]
            name =""
            central = df_adicional['central'].iloc[i].lower()
            id_central = centralsinfo[centralsinfo['CenName'].str.lower() == central]['id'].values[0]
            aux=[]
            aux.append(countid)
            countid+=1
            name_b = junctionsinfo[junctionsinfo['id']==id_central].CenName.values[0]
            if tipo == "filtration":
                name = name_a+'_Fil_'+name_b
            elif tipo == "extraction":
                name = name_a+'_Ext_'+name_b
            aux.append(name)
            aux.append(tipo)
            aux.append(junctionsinfo.id[x])
            aux.append(id_central)
            aux.append(1)
            aux.append(None)
            aux.append(None)
            aux.append(None)
            aux.append(None)
            junctionhydricfilas_aux.append(aux)
waterwayshydric=pd.DataFrame(junctionhydricfilas_aux,columns=["id","name","type","junc_a_id","junc_b_id","active","fmin","fmax","cvar","delay"])
waterwayshydric.to_json(hydricTopology+"/waterways.json",orient='records')

print("Archivos listos para visualizar ubicados en la ruta en donde se encuentra este archivo py")

# Se comprime el archivo de salidas .json
current_directory = os.getcwd()
folder_path = os.path.join(current_directory, namedata)
zip_path = folder_path
shutil.make_archive(zip_path, "zip", folder_path)

print(f"Se comprime el archivo de visualización {namedata}.zip")

# Ruta de destino para mover la carpeta
Destino = '/home/emonsalve/EnergyViewer/Archivos_JSON' # Modificable
ruta_destino = os.path.join(Destino, namedata)

# Mover la carpeta
shutil.move(folder_path, ruta_destino)

print(f"Se mueve la carpeta de visualización '{namedata}' hacia '{ruta_destino}'")