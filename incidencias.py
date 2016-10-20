# Liberias BD
from pymongo import MongoClient
import MySQLdb
from bson.objectid import ObjectId

# Mis Librerias
import libgeneral

# Liberias para fechas
from datetime import date, timedelta
from datetime import datetime
import time
import pandas as pd
from pandas.tseries.offsets import BDay

# Liberias generales
import sys

###
## VAR
###

tabla="etl_incidencias"
basededatos="informespre"

###
## Codigo
###

# Me conecto al mongo
db = MongoClient().betacompany

# Me conecto al MySQL
dbmysql = MySQLdb.connect(host="localhost", user="root", passwd="sm2madmin", db=basededatos)
cur = dbmysql.cursor()

# Limpio el MySQL
query = """truncate table %s""" % tabla
cur.execute(query)

filt = { "$and": [{"customerservice" : "CS_SM2MS"}, {"status" : "Active"}]}
for doc in db.assurance.tt.incidence.find(filt):

    # Obtengo el contacto
    contact = doc['contact']
    c = db.assurance.tt.contact.find_one({'_id': ObjectId(contact)})

    # ID
    id = doc['contact_eid']

    # Servicio
    service = c['customerservice']

    # Severidad
    severidad = c['severity']

    # Tipo
    tipo = c['type']

    # Fecha de apertura, fecha de restauracion
    fecha_apertura = "NULL"
    fecha_restaurado = "NULL"
    slaRestaurado = "NULL"
    try:
        fecha_apertura = doc['initial_date']
        fecha_restaurado = doc['restoration_date']
    except:
        for change in doc['status_change']:
            if change['code'] in "New":
                fecha_apertura = change['start']
        for change in doc['status_change']:
            if change['code'] in "Restored":
                fecha_restaurado = change['start']
    try:
        slaRestaurado = fecha_restaurado - fecha_apertura
    except:
        pass

    week = libgeneral.getWeekFromDate(fecha_apertura)
    # Tiempo retenida
    tiemporetenida = timedelta(0)
    try:
        for change in doc['status_change']:
            if change['code'] in "Delayed":
                cambio = change['end'] - change['start']
                tiemporetenida += cambio
    except:
        pass

    # Fuera sla
    fuerasla = 0
    fechalimite = fecha_apertura + BDay(6) + tiemporetenida
    try:
        if fecha_restaurado > fechalimite:
            fuerasla = 1
    except:
        pass
    
    print "Id: %s, Servicio: %s, Severidad: %s, Tipo: %s, SLA Restaurado: %s" % (id,service,libgeneral.printSeveridad(severidad),tipo,slaRestaurado )
    query = "INSERT INTO `%s` ( `id`, `servicio`, `severidad`, `tipo`, `sla_restaurado`, `fecha_apertura`,`fecha_restaurado`, `fuerasla`,`tiempo_retenida`, `week` ) VALUES ( %s, '%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', %s)" % (tabla, id, service, libgeneral.printSeveridad(severidad),tipo,slaRestaurado, fecha_apertura, fecha_restaurado, fuerasla, libgeneral.horas_minutos_segundos(tiemporetenida), week)

    # Para el mysql
    try:
        cur.execute(query)
        dbmysql.commit()
    except:
        dbmysql.rollback()
