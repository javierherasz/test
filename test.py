import libgeneral as general
from pymongo import MongoClient
db = MongoClient().betacompany
for doc in db.assurance.tt.incidence.find({"contact_eid":90321}):
    #print general.getRestorationDate(doc)
    #print general.getGrupoDelegado(doc)
    #print general.getResponsablesAsignados(doc)
    #print general.getUsuariosSalen(doc)
    general.getTipoNota(doc)