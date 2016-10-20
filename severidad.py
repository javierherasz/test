#!/usr/bin/env python

# Liberias BD
from pymongo import MongoClient
from bson.objectid import ObjectId

# Liberias para fechas
from datetime import date, timedelta, datetime
import time
import pandas as pd
from pandas.tseries.offsets import BDay

# Liberias generales
import sys
from tqdm import tqdm
from influxdb import InfluxDBClient

# Logging
import logging
logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.INFO)


###
## Perso
###

basededatos="informesint"
def printSeveridad(severidad):
    sev = "Fallo"
    if severidad is 0:
        sev = "Cosmetic"
    elif severidad is 1:
        sev = "Slight"
    elif severidad is 2:
        sev = "Minor"
    elif severidad is 3:
        sev = "Major"
    elif severidad is 4:
        sev = "Major High"
    elif severidad is 5:
        sev = "Critical"
    else:
        sev = severidad
    return sev


cosmetic=0
slight=0
minor=0
major=0
major_high=0
critical=0


###
##
###

hoy_raw = date.today()
hoy = datetime(hoy_raw.year, hoy_raw.month, hoy_raw.day, 00, 00, 00, 000000)
hace1semana_raw = date.today() -timedelta(7)
hace1semana = datetime(hace1semana_raw.year, hace1semana_raw.month, hace1semana_raw.day, 23, 59, 59, 999999)

###
## Codigo
###

# Me conecto al mongo
db = MongoClient().betacompany

# Me conecto al InfluxDB
logger.info("Inicio cliente de InfluxDB")
influx = InfluxDBClient('localhost',8086,'javi','javi','mydb')


# Limpio el MySQL
query = """DROP MEASUREMENT severidad"""
try:
    influx.query(query)
except:
    pass


filt = { "$and": [{"customerservice" : "CS_SM2MS"}, {"status" : { "$ne" : "Canceled"}}]}
for doc in db.assurance.tt.incidence.find(filt):
 
 	# Obtengo el contacto TAG
    contact = doc['contact']
    c = db.assurance.tt.contact.find_one({'_id': ObjectId(contact)})

    # Servicio TAG
    service = c['customerservice']

    # Severidad TAG
    severidad = c['severity']

    # Fecha de apertura TIME , fecha de restauracion TAG, tiempo en restaurar TAG
    fecha_apertura = "NULL"
    fecha_restaurado = "NULL"
    slaRestaurado = "NULL"
    try:
        fecha_apertura = doc['initial_date']
    except:
        for change in doc['status_change']:
            if change['code'] in "New":
                fecha_apertura = change['start']
    try:
        fecha_restaurado = doc['restoration_date']
    except:
        for change in doc['status_change']:
            if change['code'] in "Restored":
                fecha_restaurado = change['start']
    try:
        tiempo_enrestaurar = fecha_restaurado - fecha_apertura
    except:
        pass


    # Tiempo retenida TAG
    tiemporetenida = timedelta(0)
    try:
        for change in doc['status_change']:
            if change['code'] in "Delayed":
                cambio = change['end'] - change['start']
                tiemporetenida += cambio
    except:
        pass

    # Fuera sla KEY
    fuerasla = "false"
    fechalimite = fecha_apertura + BDay(6) + tiemporetenida
    try:
        if fecha_restaurado > fechalimite:
            fuerasla = "true"
    except:
        pass

##### Nuevo

    # Grupo delegado
    delegado = []
    try:
        for change in doc['history']:
            if change['code'] in "DELEGATED":
                delegado.append(change['value'])
    except:
        pass

    # Notas semanal
    notas_semanal = []
    try:
        for change in doc['history']:
            if change['code'] in "ANNOTATION_ADDED":
                if change['start'] >= hace1semana:
                    notas_semanal.append(change['value'].replace(",",''))
                    notas_semanal.append(change['value'].replace(";",''))
    except:
        notas_semanal.append("NULL")

    # Busqueda de cadena
    cadena = "WGA"
    encontrado = 0
    try:
        for change in doc['history']:
            if change['code'] in "ANNOTATION_ADDED":
                if cadena in change['value']:
                    encontrado = 1
    except:
        pass
    if encontrado is 1:
        print "%s;'%s';%s;%s" % (id, fecha_apertura, delegado, service )
        

    # slaRestaurado necesito que siempre sea un timedelta
    if isinstance(slaRestaurado, timedelta):
        slaRestaurado_timedelta = slaRestaurado
    else:
        slaRestaurado_timedelta = timedelta(0)

    if isinstance(fecha_restaurado, datetime):
        fecha_restaurado_epoch = int(fecha_restaurado.strftime('%s'))
    else:
        fecha_restaurado_epoch = 0


    if severidad is 0:
        cosmetic=cosmetic+1
    if severidad is 1:
        slight=slight+1
    if severidad is 2:
        minor=minor+1
    if severidad is 3:
        major=major+1
    if severidad is 4:
        major_high=major_high+1
    if severidad is 5:
        critical=critical+1





    # Datos para influxdb
    influxdata = {
                  "measurement": "severidad",
                  "time": int(fecha_apertura.strftime('%s')),
                  "tags": {
                           "servicio": service,                 
                          },
                  "fields": {
                  			 "Cosmetic" : cosmetic,
                             "Slight" : slight,
                             "Minor" : minor,
                             "Major" : major,
                             "High Major" : major_high,
                             "Critical" : critical,
                            }
                  }


    logger.info("Influx data: %s", influxdata)
    influx.write_points([influxdata], time_precision='s')