#!/bin/bash


#Exporta los datos de mongo en un CSV
mongoexport --db betacompany --collection assurance.tt.incidence --csv --out /var/tmp/ejemplo.csv --fields _id,customerservice,history.start,history.starting_operator,history.code,history.value,last_event,priority,responsible.current.date.start,responsible.current.operator,responsible.current.group,severity,status,type

#mongoexport --db betacompany --collection assurance.tt.incidence --out prueba.json


