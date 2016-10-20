"""
Contiene la clase grafica que permite generar una grafica de barras
Se usa en activeweek
"""

import matplotlib
import matplotlib.pyplot as plt
import collections
import pandas as pd
from datetime import date

class GraficaJ(object):

	def __init__(self, slight, minor, mayor, critical):
		self.slight = slight
		self.minor = minor
		self.mayor = mayor
		self.critical = critical


	def ordenar(self):
		sSlight = collections.OrderedDict()
		sMinor = collections.OrderedDict()
		sMayor = collections.OrderedDict()
		sCritical = collections.OrderedDict()

		#ordenamos los dictionary y anadimos las semanas que no tienen incidencias para que aparezcan en la grafica
		week = date.today().isocalendar()[1]
		sSlight['anteriores'] = self.slight['anteriores']
		sMinor['anteriores'] = self.minor['anteriores']
		sMayor['anteriores'] = self.mayor['anteriores']
		sCritical['anteriores'] = self.critical['anteriores']

		for x in xrange(week - 8,week):
			if x in self.slight.keys():
				sSlight[x] = self.slight[x]
				sMinor[x] = self.minor[x]
				sMayor[x] = self.mayor[x]
				sCritical[x] = self.critical[x]
			else:
				sSlight[x] = 0
				sMinor[x] = 0
				sMayor[x] = 0
				sCritical[x] = 0

		self.slight = sSlight
		self.minor = sMinor
		self.mayor = sMayor
		self.critical = sCritical

	def mostrar(self):
		matplotlib.style.use('ggplot')

		d= {'Slight' : pd.Series(self.slight), 
			'Minor' : pd.Series(self.minor), 
			'Major' : pd.Series(self.mayor),
			'Critical' : pd.Series(self.critical)
			}
		df = pd.DataFrame(d)

		#optional colors http://matplotlib.org/examples/color/colormaps_reference.html
		#other options http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.plot.html
		df.plot(kind='bar', stacked=True, rot=0, colormap='winter', alpha=0.75)
		plt.savefig('activeWeeks.png')

		#muestra para debug
		plt.show()
