import sys
import MySQLdb
from datetime import date, timedelta

# Recorre fechas desde START hasta END
def dategenerator(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)

# Obtener la ultima fecha de una BD y TABLA  dada. La tabla deve tener fecha_consulta
def bdlastday(bd, tabla):
    answer = ""
    dbmysql = MySQLdb.connect(host="localhost", user="root", passwd="admintid33", db=bd)
    cur = dbmysql.cursor()
    query = "SELECT fecha_consulta FROM %s ORDER BY fecha_consulta DESC LIMIT 1" % tabla
    cur.execute(query)
    answer = cur.fetchone()
    return answer

def printSeveridad(severidad):
    sev = "Fallo"
    if severidad is 0:
        sev = "Cosmetic"
    elif severidad is 1:
        sev = "Slight"
    elif severidad is 2:
        sev = "Minor"
    elif severidad is 3:
        sev = "Mayor"
    elif severidad is 4:
        sev = "Mayor High" 
    elif severidad is 5:
        sev = "Critical"
    else:
        sev = severidad
    return sev

def horas_minutos_segundos(td):
    # return "%s:%s" % (td.days * 24 + td.seconds//3600, (td.seconds//60)%60)
    # return td.seconds//3600, (td.seconds//60)%60
    minutes, seconds = divmod(td.seconds + td.days * 86400, 60)
    hours, minutes = divmod(minutes, 60)
    return '{:d}:{:02d}:{:02d}'.format(hours, minutes, seconds)


# si es de un anyo anteior devolvera un numero de semana negativo o 0
def getWeekFromDate(dt):
    if (dt.year < date.today().year):
        week = (dt.isocalendar()[1] - getNWeeksDifference(dt))
    elif (dt.month is 1) and (dt.isocalendar()[1] is 53):
        week = 0
    else:
        week = dt.isocalendar()[1]
    return week

def getMonthFromDate(dt):
    return dt.month


# devuelve el numero de semanas de un anyo
def getNWeeksYear(dt):
	aux_dt = dt.replace(month=12, day=31)
	n = aux_dt.isocalendar()[1]
	if n == 1:
		n = 52
	return n

#devuelve el numero de semana relativo al anyo actual 
def getNWeeksDifference(dt):
	i = date.today().year - dt.year
	if (i > 0):
		n=0
		y = date.today().year - 1
		for x in xrange(i):
			n = n + getNWeeksYear(date.today().replace(year = y))
			y-=1		
	return n


def sortOrderedDict(od):
    auxod = collections.OrderedDict()
    for k in sorted(od.keys()):
        auxod[k] = od[k]
    return auxod


# Restoration date
def getRestorationDate(doc):
    restoration = doc['restoration_date']
    return restoration

# Grupo delegado
def getGrupoDelegado(doc):
    delegado = []
    try:
        for change in doc['history']:
            if change['code'] in "DELEGATED":
                delegado.append(change['value'])
    except:
        pass 
    return delegado


# Grupo Responsables Asignados
def getResponsablesAsignados(doc):
    responsables = []
    try:
        for change in doc['history']:
            if change['code'] in "RESPONSIBLE_ASSIGNMENT":
                responsables.append(change['starting_operator'])
    except:
        pass 
    return responsables




# Lista de usuarios que aparecen en el ticket
def getUsuariosSalen(doc):
    aux = 0
    resultado = []
    for change in doc['history']:
        try:
            aux = change['starting_operator']
            resultado.append(change['starting_operator'])
        except:
            pass
    for change in doc['status_change']:
        try:
            aux = change['starting_operator']
            resultado.append(change['starting_operator'])
        except:
            pass
    for change in doc['last_event']:
        try:
            aux = change['starting_operator']
            resultado.append(change['starting_operator'])
        except:
            pass
    #eliminamos elementos repetidos creando un diccionario
    resultado = dict.fromkeys(resultado)	
    resultado = resultado.keys()
    return resultado


# Grupo delegado
def getIdTicket(doc):
    ticket = doc['contact_eid']
    return ticket


def getType(doc):
    type = doc['type']
    return type


def getStatus(doc):
    status = doc['status']
    return status


def getTipoNota(doc):
    responsables = []
    aux = []
    try:
        for change in doc['history']:
            if change['code'] in "ANNOTATION_ADDED":
                responsables.append(change['value'])
    except:
        pass 
    j=0
    for i in responsables:
        responsable = responsables[j]
        aux.append(responsable[responsable.find("[")+1:responsable.find("]")]) #responsable[1:7]
        j+=1
    return aux


def getListaPersonas(doc):
    personas = []
    try:
        for change in doc['history']:
            if change['code'] in "ANNOTATION_ADDED":
                personas.append(change['starting_operator']) 
    except:
        pass 
    return personas



def getFechaComentario(doc):
    fecha = []
    try:
        for change in doc['history']:
            if change['code'] in "ANNOTATION_ADDED":
                fecha.append(change['start'])
    except:
        pass 
    return fecha




"""
responsables = []
    try:
        for change in doc['history']:
            if change['code'] in "ANNOTATION_ADDED":
                responsables.append(change['value'])
    except:
        pass 
    j=0
    for i in responsables:
        responsable = responsables[j]
        print responsable[responsable.find("[")+1:responsable.find("]")] #responsable[1:7]
        j+=1
    return responsable
"""
