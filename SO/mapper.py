#!/usr/bin/python

import sys
import re

i = 0
headers = []
for line in sys.stdin:
	j = 0
	line = re.sub( r'^\W+|\W+$', '', line )
	line = line.split(",")
	for data in line:
		if (i != 0):
			if (data == "True"):
				print(headers[j] + "\t" + "1")
		else:
			headers.append(data)
		j+=1
	i+=1


#Salida <genero, 1> por cada true
