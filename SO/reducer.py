#!/usr/bin/python
import sys
previous = None
sum = 0
print(sys.argv[2] + ',' + sys.argv[1])
for line in sys.stdin:
    key, value = line.split( '\t' )
    if key != previous:
        if previous is not None:
            print previous + ',' + str( sum )
        previous = key
        sum = 0

    sum = sum + int( value )

print previous + ',' + str( sum )
