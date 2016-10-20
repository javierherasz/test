"""
Hace una ETL en la que coge los datos del mongo y los transforma y carga en la base de datos 
MySQL "informespre" en la tabla activeByWeek. Se usa para generar una 
grafica de barras de las incidencias activas por cada semana.
"""
# Liberias BD
from pymongo import MongoClient
import MySQLdb
from bson.objectid import ObjectId

# Mis Librerias
import libgeneral
from grafica import Grafica

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

tabla="ActiveByWeek"
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

def doQuery(query):
    # Para el mysql
    try:
        cur.execute(query)
        dbmysql.commit()
    except:
        dbmysql.rollback()
        print "rollback"

# filtro de mongo
filt = { "$and": [{"customerservice" : "CS_SM2MS"}, {"status" : "Active"}]}

slight, minor, mayor = 0,0,0

anterior = True
firstWeek = date.today().isocalendar()[1] - 8
auxweek = "NULL"
#print firstWeek
activeWeek = collections.OrderedDict()
activeSlight = collections.OrderedDict()
activeMinor = collections.OrderedDict()
activeMajor = collections.OrderedDict()

def addWeek(w,a,b,c,d):
    activeWeek[w] = d
    activeSlight[w] = a
    activeMinor[w] = b
    activeMajor[w] = c

	
for doc in db.assurance.tt.incidence.find(filt):

    # Obtengo el contacto
    contact = doc['contact']
    c = db.assurance.tt.contact.find_one({'_id': ObjectId(contact)})
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

    week = libgeneral.getWeekFromDate(fecha_apertura)
    print(fecha_apertura)
    #print week
    if week > firstWeek:
        # se anade una fila en mysql con las semanas "anteriores", solo se hace una vez
        if anterior:
            total = slight + minor + mayor
            query = "INSERT INTO `%s` ( `week`, `slight`, `minor`, `mayor`, `total`) VALUES ( %s, '%s', %s, %s, %s)" % (tabla, '"anteriores"', slight, minor, mayor, total)
            addWeek("anteriores", slight, minor, mayor, total)
            print query
            doQuery(query)
            slight, minor, mayor = 0,0,0
            anterior = False

        elif week != auxweek:
            #print "cambio! %s %s" % (auxweek, week)
            total = slight + minor + mayor
            query = "INSERT INTO `%s` ( `week`, `slight`, `minor`, `mayor`, `total`) VALUES ( %s, '%s', %s, %s, %s)" % (tabla, auxweek, slight, minor, mayor, total)
            print query
            addWeek(auxweek, slight, minor, mayor, total)
            # Para el mysql
            try:
                cur.execute(query)
                dbmysql.commit()

            except:
                dbmysql.rollback()
                print"rollback"
            slight, minor, mayor = 0,0,0

    if severidad is 1:
        slight += 1
    elif severidad is 2:
        minor += 1
    elif severidad is 3:
        mayor += 1
    auxweek = week

total = slight + minor + mayor
addWeek(auxweek, slight, minor, mayor, total)
query = "INSERT INTO `%s` ( `week`, `slight`, `minor`, `mayor`, `total`) VALUES ( %s, '%s', %s, %s, %s)" % (tabla, auxweek, slight, minor, mayor, total)
print query
# Para el mysql

try:
    cur.execute(query)
    dbmysql.commit()

except:
    dbmysql.rollback()
    print"rollback"

gr = Grafica(activeSlight, activeMinor, activeMajor)
gr.ordenar()
gr.mostrar()