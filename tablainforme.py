"""
Hace una ETL en la que coge los datos del mongo y los transforma y carga en la base de datos
MySQL "informespre" en la tabla informe. Se usa para generar una 
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

tabla="informe"
basededatos="informespre"

###
## Codigo
###

# Me conecto al mongo
db = MongoClient().betacompany

# Me conecto al MySQL
dbmysql = MySQ
db.connect(host="localhost", user="root", passwd="sm2madmin", db=basededatos)
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

	
for doc in db.assurance.tt.wo.find(filt):

    # Obtengo el contacto
    contact = doc['contact']
    c = db.assurance.tt.contact.find_one({'_id': ObjectId(contact)})
    
		# ID
    id = doc['contact_eid']
		
		#	Obs afectadas
		obafectada = c['leading_ob']		

		#	Organizacion Responsable
		oresponsable = wo['responsible']
	
		#	Asunto (Subject)
		asunto = wo['subject']		

		# Severidad
    severidad = c['severity']

		# Descripcion
		descripcion = wo['description'] 



    # se anade una fila en mysql con las obs afectadas
    query = "INSERT INTO `%s` (`id`, `ob_afectada`, `organizacion_responsable`, `asunto`, `severidad`, `descripcion`) VALUES (%s, '%s', %s, %s, %s, %s)" % (tabla ,id, obafectada, oresponsable, asunto, severidad, descripcion)
   
		#	Print Query
		print query

		# Para el mysql
    try:
 		   cur.execute(query)
       dbmysql.commit()

    except:
 		   dbmysql.rollback()
       print"rollback"
      


