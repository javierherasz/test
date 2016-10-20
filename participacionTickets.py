#!/usr/bin/env python
class style:
   BOLD = '\033[1m'
   END = '\033[0m'

#Librerias BD
from pymongo import MongoClient
from bson.objectid import ObjectId

#Librerias para fechas
from datetime import date, timedelta, datetime
import time
import dateutil.parser
import pandas as pd
from pandas.tseries.offsets import BDay

#ibrerias generales
import operator
import libgeneral as general
import sys
from tqdm import tqdm
from influxdb import InfluxDBClient

# Logging
import logging
logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.INFO)


###
## VAR
###
tickets = 0.0		# Numero total de tickets
aux = []			# Lista que guarda todos los participantes
dic = {}			# Diccionario con nombre : total_apariciones

#fecha_ini = dateutil.parser.parse("2016-09-12T00:00:00Z")
#fecha_fin = dateutil.parser.parse("2016-09-13T00:00:00Z")


###
## Codigo
###

# Me conecto al mongo
db = MongoClient().betacompany

while True:
    i = str(raw_input('Introduce fecha de inicio (YYYY, MM, DD): '))
    try:
        fecha_ini = datetime.strptime(i, '%Y, %m, %d')
        break
    except ValueError:
        print "Formato incorrecto, vuelve a introducir la fecha\n"
        pass
while True:
    i = str(raw_input('Introduce fecha de fin (YYYY, MM, DD): '))
    try:
        fecha_fin = datetime.strptime(i, '%Y, %m, %d')
        break
    except ValueError:
        print "Formato incorrecto, vuelve a introducir la fecha\n"
        pass


# Forzando fechas
#fecha_ini = datetime(2016, 9, 12, 00, 00, 00, 999999)
#fecha_fin = datetime(2016, 9, 12, 23, 59, 00, 999999)



# Empieza la iteracion entre las fechas indicadas
for dt in general.dategenerator(fecha_ini, fecha_fin):

    day_ini = datetime(dt.year, dt.month, dt.day, 00, 00, 00, 999999)
    day_fin = datetime(dt.year, dt.month, dt.day, 23, 59, 00, 999999)

    filt = {"$and" :[{"initial_date": {"$gte" : day_ini, "$lte" : day_fin}}, {"type" : "Claim"}, {"customerservice" : "CS_SM2MS"}, {"status" : { "$ne" : "Canceled"}}]}
    # Para la fecha actual, procesamos todo el mongo
    for doc in tqdm(db.assurance.tt.incidence.find(filt)):
        #print general.getRestorationDate(doc)
        #print general.getGrupoDelegado(doc)
        #print general.getResponsablesAsignados(doc)
        tickets += 1
        idTicket = doc['contact_eid']

        # Guardo una lista con todos los participantes (pueden aparecer repetidos)
        aux = aux + general.getUsuariosSalen(doc)			

        # Muestro por cada ticket las personas que han intervenido (aunque sea un simple comentario)
        print "\nEn el ticket", idTicket,"participaron las siguientes personas: "
        print general.getUsuariosSalen(doc)


print "Total de tickets:" ,tickets, "\n"

# print "\n", aux, "\n"  
# print "\n", dic

for elemento in aux:
	dic[elemento] = float(aux.count(elemento))

nombres = dic.keys()
valor = dic.values()

dic_view = [ (v,k) for k,v in dic.iteritems() ]
dic_view.sort(reverse=True)
for v,k in dic_view:
   print "%s tiene un porcentaje de participacion del: %.2f" % (k,(v*100/tickets))


#for nombres, valor in dic.items():
#	print style.BOLD + nombres, style.END + ' tiene un porcentaje de participacion del', (valor*100)/tickets


