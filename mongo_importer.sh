mongorestore --drop ./mongodb_dump_ESJC-UDOP-WS01P_${fecha}/betacompany/
#!/bin/bash

### uDO importer ###


fecha=$(date --date "yesterday" +"%Y-%m-%d")

# Restaurarlo en el mongo
mongorestore --drop /var/tmp/mongobackup/mongodb_dump_ESJC-UDOP-WS01P_${fecha}/betacompany/


