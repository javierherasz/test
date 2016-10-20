#!/usr/bin/env python

# Liberias BD
from pymongo import MongoClient
from bson.objectid import ObjectId

# Liberias para fechas
from datetime import date, timedelta, datetime
import time

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
## VAR
###

basededatos="informesint"

def dategenerator(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


# filtro de mongo
filt = {"customerservice" : "CS_SM2MS"}

###
## Codigo
###

# Me contaco al mongo
logger.info("Me conecto al mongo")
db = MongoClient().betacompany

# Inicio el cliente de influxdb
logger.info("Inicio cliente de InfluxDB")
influx = InfluxDBClient('localhost',8086,'javi','javi','mydb')

# Limpio el MySQL
query = """DROP MEASUREMENT prueba"""
try:
    influx.query(query)
except:
    pass

# Saco las fechas de inicio y fin
logger.info("Busco la ultima fecha de la BD y empiezo al dia siguiente")
day = date.today()
temp = "NULL"
query = 'select * from prueba ORDER BY time DESC LIMIT 1;'
result = influx.query(query)
for item in result.get_points():
    temp = item['time'].rsplit('T',1)[0]
try:
    fecha_inicio = datetime.strptime(temp,"%Y-%m-%d").date()+ timedelta(1)
except:
    fecha_inicio = date(2016, 1, 1)


# Forzando fechas
fecha_inicio = day - timedelta(2)
fecha_fin = day - timedelta(1)
logger.info("Forzando fechas %s y %s" % (fecha_inicio, fecha_fin))

# Empieza la iteracion entre las fechas indicadas
logger.info("Exploracion entre %s y %s", fecha_inicio, fecha_fin)
for dt in dategenerator(fecha_inicio, fecha_fin):

    logger.info("De este dia, pillo los datos: %s/%s/%s", dt.day, dt.month, dt.year)
    day = datetime(dt.year, dt.month, dt.day, 23, 59, 59, 999999)
    today = time.strftime("%Y-%m-%d")

    # Lista de servicios
    serv = []

    # Lista de tickets claim, order y query
    Claim = {}
    
    Order = {}

    Query = {}

    # Para la fecha actual, procesamos todo el mongo
    logger.info("Procesamos cada registro de mongo de esta fecha...",)
    for doc in tqdm(db.assurance.tt.incidence.find(filt)):

        # Obtengo info del ticket
        logger.debug("Obtengo la info del ticket..")
        contact = doc['contact']
        c = db.assurance.tt.contact.find_one({'_id': ObjectId(contact)})

		# ID TAG
   	 	#id = doc['contact_eid']
    	#id=int(id)

        # Almaceno el servicio
        service = c['customerservice']
        if service not in serv:
           serv.append(service)

        # Tipo de ticket
        t = c['type']

        logger.debug("contact=%s  service=%s  type=%s", contact, service, t)

        # Para ese dia, si no esta vacios
        if t == 'Claim':

          	if service not in Claim:
          		Claim[service] = []
			Claim[service].append(doc['contact_eid'])


        # Para ese dia, si no esta vacios
        if t == 'Order':

          	if service not in Order:
          		Order[service] = []
          	Order[service].append(doc['contact_eid'])
                
        if t == 'Query':
           
          	if service not in Query:
          		Query[service] = []
          	Query[service].append(doc['contact_eid'])


    logger.info("Para cada Claim, imprimo lo datos (%s prueba)", len(serv))
    for s in serv:

        claims = 0
        orders = 0
        queries = 0

        # Pimero Claim

        if s in Claim:
        	claims = len(Claim[service])
		
		if s in Order:
			orders = len(Order[service])
		if s in Query:
			queries = len(Query[service])

  
        # Datos para influxdb
        influxdata = {
                      "measurement": "prueba",
                      "time": int(dt.strftime('%s')),
                      "tags": {
                               "servicio": s,
                              },
                      "fields": {
                                 "claims": claims,
                                 "orders": orders,
                                 "queries": queries,
                                }
                      }

        logger.info("Influx data: %s", influxdata)
        influx.write_points([influxdata], time_precision='s')
