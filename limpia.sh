#!/bin/bash
fecha=$(date --date "yesterday" +"%Y-%m-%d")
cd /var/tmp/
rm -r mongodb_dump_ESJC-UDOP-WS01P_${fecha}
