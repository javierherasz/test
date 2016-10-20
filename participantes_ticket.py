##
##codigo
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

