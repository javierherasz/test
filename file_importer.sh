#!/bin/bash

fecha=$(date +"%Y-%m-%d")
cd /home/PRE_M2M_RPR_02

# Importar el JSON de mongo (usamos la clave publica del usuario PRE_M2M_RPR_02)
scp -r -i .ssh/id_rsa sm2m@10.95.235.4:/home/repo/mongobackup/mongodb_dump_ESJC-UDOP-WS01P_${fecha} /var/tmp/


#mongo
#use betacompany
#db.dropDatabase()
#exit


# Restaurarlo en el mongo
mongorestore --drop -d betacompany /var/tmp/mongodb_dump_ESJC-UDOP-WS01P_${fecha}/betacompany/


# Limpio los datos
#rm -rf /var/tmp/mongobackup/
#rm -f mongodb_dump_ESJC-UDOP-WS01P_${fecha}.tar.gz

