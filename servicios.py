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


###
## Codigo
###

# Me contaco al mongo
logger.info("Me conecto al mongo")
db = MongoClient().betacompany

# Inicio el cliente de influxdb
logger.info("Inicio cliente de InfluxDB")
influx = InfluxDBClient('localhost',8086,'javi','javi','mydb')

# Saco las fechas de inicio y fin
logger.info("Busco la ultima fecha de la BD y empiezo al dia siguiente")
day = date.today()
temp = "NULL"
query = 'select * from servicios ORDER BY time DESC LIMIT 1;'
result = influx.query(query)
for item in result.get_points():
    temp = item['time'].rsplit('T',1)[0]
try:
    fecha_inicio = datetime.strptime(temp,"%Y-%m-%d").date()+ timedelta(1)
except:
    fecha_inicio = date(2016, 1, 1)


# Forzando fechas
fecha_inicio = day - timedelta(7)
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

    # Lista de tickets nuevos, activos, delayed y restored
    Claim_were_new = {}
    Claim_were_active = {}
    Claim_were_delayed = {}
    Claim_were_restored = {}

    Order_were_new = {}
    Order_were_active = {}
    Order_were_delayed = {}
    Order_were_restored = {}

    Query_were_new = {}
    Query_were_active = {}
    Query_were_delayed = {}
    Query_were_restored = {}

    # Para la fecha actual, procesamos todo el mongo
    logger.info("Procesamos cada registro de mongo de esta fecha...",)
    for doc in tqdm(db.assurance.tt.incidence.find()):

        # Obtengo info del ticket
        logger.debug("Obtengo la info del ticket..")
        contact = doc['contact']
        c = db.assurance.tt.contact.find_one({'_id': ObjectId(contact)})

        # Almaceno el servicio
        service = c['customerservice']
        if service not in serv:
            serv.append(service)

        # Tipo de ticket
        t = c['type']

        logger.debug("contact=%s  service=%s  type=%s", contact, service, t)

        # Para ese dia, si no esta vacios
        if t == 'Claim':
            maxchange = None

            # Para todos los cambios, busco el ultimo estado antes del dia indicado
            for change in doc['status_change']:
                if change['start'] <= day:
                    maxchange = change

            # Para ese dia, si no esta vacios
            if maxchange is not None:

                # Para el servicio dado, le correspondo el ticket
                if maxchange['code'] in ('New'):
                    # Si es la primera vez que aparece
                    if service not in Claim_were_new:
                        Claim_were_new[service] = []
                    Claim_were_new[service].append(doc['contact_eid'])
                if maxchange['code'] in ('Active'):
                    if service not in Claim_were_active:
                        Claim_were_active[service] = []
                    Claim_were_active[service].append(doc['contact_eid'])
                if maxchange['code'] in ('Delayed'):
                    if service not in Claim_were_delayed:
                        Claim_were_delayed[service] = []
                    Claim_were_delayed[service].append(doc['contact_eid'])
                if maxchange['code'] in ('Restored'):
                    if service not in Claim_were_restored:
                        Claim_were_restored[service] = []
                    Claim_were_restored[service].append(doc['contact_eid'])

        # Para ese dia, si no esta vacios
        if t == 'Order':
            maxchange = None

            # Para todos los cambios, busco el ultimo estado antes del dia indicado
            for change in doc['status_change']:
                if change['start'] <= day:
                    maxchange = change

            # Para ese dia, si no esta vacios
            if maxchange is not None:

                # Para el servicio dado, le correspondo el ticket
                if maxchange['code'] in ('New'):
                    # Si es la primera vez que aparece
                    if service not in Order_were_new:
                        Order_were_new[service] = []
                    Order_were_new[service].append(doc['contact_eid'])
                if maxchange['code'] in ('Active'):
                    if service not in Order_were_active:
                        Order_were_active[service] = []
                    Order_were_active[service].append(doc['contact_eid'])
                if maxchange['code'] in ('Delayed'):
                    if service not in Order_were_delayed:
                        Order_were_delayed[service] = []
                    Order_were_delayed[service].append(doc['contact_eid'])
                if maxchange['code'] in ('Restored'):
                    if service not in Order_were_restored:
                        Order_were_restored[service] = []
                    Order_were_restored[service].append(doc['contact_eid'])

        if t == 'Query':
            maxchange = None

            # Para todos los cambios, busco el ultimo estado antes del dia indicado
            for change in doc['status_change']:
                if change['start'] <= day:
                    maxchange = change

            # Para ese dia, si no esta vacios
            if maxchange is not None:

                # Para el servicio dado, le correspondo el ticket
                if maxchange['code'] in ('New'):
                    # Si es la primera vez que aparece
                    if service not in Query_were_new:
                        Query_were_new[service] = []
                    Query_were_new[service].append(doc['contact_eid'])
                if maxchange['code'] in ('Active'):
                    if service not in Query_were_active:
                        Query_were_active[service] = []
                    Query_were_active[service].append(doc['contact_eid'])
                if maxchange['code'] in ('Delayed'):
                    if service not in Query_were_delayed:
                        Query_were_delayed[service] = []
                    Query_were_delayed[service].append(doc['contact_eid'])
                if maxchange['code'] in ('Restored'):
                    if service not in Query_were_restored:
                        Query_were_restored[service] = []
                    Query_were_restored[service].append(doc['contact_eid'])

    logger.info("Para cada Claim, imprimo lo datos (%s servicios)", len(serv))
    for s in serv:

        nuevas = 0
        activas = 0
        delegadas = 0
        restauradas = 0

        # Pimero Claim

        if s in Claim_were_new:
            nuevas = len(Claim_were_new[s])
        if s in Claim_were_active:
            activas = len(Claim_were_active[s])
        if s in Claim_were_delayed:
            delegadas = len(Claim_were_delayed[s])
        if s in Claim_were_restored:
            restauradas = len(Claim_were_restored[s])


        # Datos para influxdb
        influxdata = {
                      "measurement": "servicios",
                      "time": int(dt.strftime('%s')),
                      "tags": {
                               "servicio": s,
                               "tipo": "Claim"
                              },
                      "fields": {
                                 "nuevas": nuevas,
                                 "activas": activas,
                                 "restauradas": restauradas,
                                 "delegadas": delegadas,
                                }
                      }

        logger.info("Influx data: %s", influxdata)
        influx.write_points([influxdata], time_precision='s')

    logger.info("Para cada Order, imprimo lo datos (%s servicios)", len(serv))
    for s in serv:

        nuevas = 0
        activas = 0
        delegadas = 0
        restauradas = 0

        # Ahora Order

        if s in Order_were_new:
            nuevas = len(Order_were_new[s])
        if s in Order_were_active:
            activas = len(Order_were_active[s])
        if s in Order_were_delayed:
            delegadas = len(Order_were_delayed[s])
        if s in Order_were_restored:
            restauradas = len(Order_were_restored[s])

        # Datos para influxdb
        influxdata = {
                      "measurement": "servicios",
                      "time": int(dt.strftime('%s')),
                      "tags": {
                               "servicio": s,
                               "tipo": "Order"
                              },
                      "fields": {
                                 "nuevas": nuevas,
                                 "activas": activas,
                                 "restauradas": restauradas,
                                 "delegadas": delegadas,
                                }
                      }

        logger.info("Influx data: %s", influxdata)
        influx.write_points([influxdata], time_precision='s')

    logger.info("Para cada Query, imprimo lo datos (%s servicios)", len(serv))
    for s in serv:

        nuevas = 0
        activas = 0
        delegadas = 0
        restauradas = 0

        # Finalmente Query

        if s in Query_were_new:
            nuevas = len(Query_were_new[s])
        if s in Query_were_active:
            activas = len(Query_were_active[s])
        if s in Query_were_delayed:
            delegadas = len(Query_were_delayed[s])
        if s in Query_were_restored:
            restauradas = len(Query_were_restored[s])

        # Datos para influxdb
        influxdata = {
                      "measurement": "servicios",
                      "time": int(dt.strftime('%s')),
                      "tags": {
                               "servicio": s,
                               "tipo": "Query"
                              },
                      "fields": {
                                 "nuevas": nuevas,
                                 "activas": activas,
                                 "restauradas": restauradas,
                                 "delegadas": delegadas,
                                }
                      }

        logger.info("Influx data: %s", influxdata)
        influx.write_points([influxdata], time_precision='s')
