"""
Hace una ETL en la que coge los datos del mongo y los transforma y carga en la base de datos 
MySQL "informespre" en la tabla activeByMonth. Se usa para generar una 
grafica de barras de las incidencias activas por cada semana.
"""
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

# Liberias generales
import sys
import collections

###
## VAR
###

tabla="activeByMonth"
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
query = """truncate table %s""" 
cur.execute(query, (tabla,))

"""def doQuery(query):
    # Para el mysql
    try:
        cur.execute(query)
        dbmysql.commit()
    except:
        dbmysql.rollback()
        print "rollback"   """

# filtro de mongo
filt = { "$and": [{"customerservice" : "CS_SM2MS"}, {"status" : "Active"} , {"type" : "Claim"}]}

slight, minor, mayor , critical = 0,0,0,0

auxmonth = "NULL"
#print firstWeek
activeMonth = collections.OrderedDict()
activeSlight = collections.OrderedDict()
activeMinor = collections.OrderedDict()
activeMajor = collections.OrderedDict()

def addMonth(w,s,mi,ma,m):
    activeMonth[w] = m
    activeSlight[w] = s
    activeMinor[w] = mi
    activeMajor[w] = ma

for doc in db.assurance.tt.incidence.find(filt):

	 # Obtengo el contacto
    contact = doc['contact']
    c = db.assurance.tt.contact.find_one({'_id': ObjectId(contact)})

    # ID
    id = doc['contact_eid']

    # Severidad
    severidad = c['severity']

    # Fecha de apertura
    fecha_apertura = "NULL"
    try:
        fecha_apertura = doc['initial_date']
    except:
        for change in doc['status_change']:
            if change['code'] in "New":
                fecha_apertura = change['start']

    month = libgeneral.getMonthFromDate(fecha_apertura)
    print(fecha_apertura)
    #print week

    #print "cambio! %s %s" % (auxmonth, week)
    total = slight + minor + mayor
    #query = "INSERT INTO `%s` ( `month`, `slight`, `minor`, `mayor`, `critical` , `total`) VALUES ( %s, '%s', %s, %s, %s, %s)" % (tabla, auxmonth, slight, minor, mayor, critical, total)
    query = "INSERT INTO `%s` ( `id`, `severidad`, `month` ) VALUES ( %s, '%s', %s)" % (tabla, id, libgeneral.printSeveridad(severidad),month)

    print query
    # Para el mysql
    try:
        cur.execute(query)
        dbmysql.commit()

    except:
        dbmysql.rollback()
        print"rollback"

