#!/usr/bin/env python

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

basededatos="informesint"

def dategenerator(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


restauradas=0


fecha_ini = dateutil.parser.parse("2016-07-01T00:00:00Z")
fecha_fin = dateutil.parser.parse("2016-10-01T00:00:00Z")

###
## Codigo
###

# Me conecto al mongo
db = MongoClient().betacompany

# Me conecto al InfluxDB
logger.info("Inicio cliente de InfluxDB")
influx = InfluxDBClient('localhost',8086,'javi','javi','mydb')

# Limpio el MySQL
query = """DROP MEASUREMENT restauradaspordia"""
try:
    influx.query(query)
except:
    pass


# Forzando fechas
logger.info("Forzando fechas %s y %s" % (fecha_ini, fecha_fin))
# Empieza la iteracion entre las fechas indicadas
logger.info("Exploracion entre %s y %s", fecha_ini, fecha_fin)
for dt in dategenerator(fecha_ini, fecha_fin):

    logger.info("De este dia, pillo los datos: %s/%s/%s", dt.day, dt.month, dt.year)
    day = dt +timedelta(1)

    day_ini = datetime(day.year, day.month, day.day, 00, 00, 00, 999999)
    day_fin = datetime(day.year, day.month, day.day, 23, 59, 00, 999999)
    today = time.strftime("%Y-%m-%d")
    logger.info("Fecha inicio %s", day_ini)
    logger.info("Fecha fin %s", day_fin)



    filt = {"$and" :[{"initial_date": {"$gte" : day_ini, "$lte" : day_fin}}, {"type" : "Claim"}, {"status" : "Restored"}, {"customerservice" : "CS_SM2MS"}]}
  # Para la fecha actual, procesamos todo el mongo
    logger.info("Procesamos cada registro de mongo de esta fecha...",)
    for doc in tqdm(db.assurance.tt.incidence.find(filt)):

    # Obtengo el contacto TAG
      contact = doc['contact']
      c = db.assurance.tt.contact.find_one({'_id': ObjectId(contact)})

      # Servicio TAG
      service = c['customerservice']

      # Estado TAG
      status = c['status']

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

		  
      if status == 'Restored':
          restauradas=restauradas+1
      


    # Datos para influxdb
    influxdata = {
                  "measurement": "restauradaspordia",
                  "time": int(day_ini.strftime('%s')),
                  "tags": {
                           #"servicio": service,
                          },
                  "fields": {
                             "Restauradas" : restauradas,
                            }
                  }



    logger.info("Influx data: %s", influxdata)
    influx.write_points([influxdata], time_precision='s')

    restauradas=0





