import matplotlib.pyplot as plt
import numpy as np
import collections
from datetime import date

def autolabel(rects):
	# attach some text labels
	for rect in rects:
		height = rect.get_height()
		ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
			'%d' % int(height),
			ha='center', va='bottom')

def muestra(slight, minor, mayor):

	width = 0.15       # the width of the bars

	fig, ax = plt.subplots()

	#cambiamos OrderedDict para mantener los diccionarios ordenados
	sSlight = collections.OrderedDict()
	sMinor = collections.OrderedDict()
	sMayor = collections.OrderedDict()

	#ordenamos los diccionarios y anadimos las semanas que no tienen incidencias para que aparezcan en la grafica
	week = date.today().isocalendar()[1]
	sSlight['anteriores'] = slight['anteriores']
	sMinor['anteriores'] = minor['anteriores']
	sMayor['anteriores'] = mayor['anteriores']

	for x in xrange(week - 8,week):
		if x in slight.keys():
			sSlight[x] = slight[x]
			sMinor[x] = minor[x]
			sMayor[x] = mayor[x]
		else:
			sSlight[x] = 0
			sMinor[x] = 0
			sMayor[x] = 0


	 # the x locations for the groups
	ind = np.arange(len(sSlight.keys()))

	#anadimos los vectores de los valores y la posicion que ocuparan, ademas asignamos color
	rects1 = ax.bar(ind, sSlight.values(), width, color='r')
	rects2 = ax.bar(ind + width, sMinor.values(), width, color='y')
	rects3 = ax.bar(ind + width + width, sMayor.values(), width, color='g')

	# add some text for labels, title and axes ticks
	ax.set_ylabel('Semanas')
	ax.set_title('Incidencias activas por semana')
	ax.set_xticks(ind + width + width/2)
	ax.set_xticklabels(sSlight.keys())

	ax.legend((rects1[0], rects2[0], rects3[0]), ('slight', 'minor', 'major'))

	"""
	#podemos asignar
	autolabel(rects1)
	autolabel(rects2)
	autolabel(rects3)
	"""
	
	plt.savefig('activeWeeks.png')
	plt.show()