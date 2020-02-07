import sys
import numpy as np
from random import shuffle

if len(sys.argv) < 2:
    print("Please provide an argument to choose the distribution, e.g. 'python3 npdist.py poisson'.")
    print("Options are: 'poisson', 'binomial'")
    quit()

### build rlation
numBuild=10000000
numProbe=10000000

if sys.argv[1] == "poisson":
    ls = np.random.poisson(300, numBuild)
elif sys.argv[1] == "binomial":
    ls = np.random.binomial(2000, 0.1, numBuild)
else:
    print("Distribution '" + argv[1] + "' unknown. Quit.")
    quit()

#add 1 as lower limit for string length
ls += 1

#print(ls[0:500])

stringIndex = 0
tupleIndex = 0
stringOffset = 0
stringLength = 0

buildKeys = []

while tupleIndex < numBuild:
    stringLength = ls[stringIndex] 
    buildKeys.append(stringIndex)

    tupleIndex += 1
    stringOffset += 1
    
    if stringOffset >= stringLength:
        stringIndex += 1
        stringOffset = 0

#print(buildKeys[0:500])

shuffle(buildKeys)

#print(buildKeys[0:500])

with open("r_build.tbl","w") as f:
    for i in range(numBuild):
        f.write( str(buildKeys[i]) + "|" + str(i) + "|" + "\n")

#print ( "max string index: " + str(stringIndex) )

### probe relation

probeKeyMultiplicity = 3

maxProbeKey = int ( numProbe / probeKeyMultiplicity )

probeKeys = []
for i in range(numProbe):
    probeKeys.append(i % maxProbeKey)


shuffle(probeKeys)

with open("s_probe.tbl","w") as f:
    for i in range(numProbe):
        f.write( str(probeKeys[i]) + "|" + str(i) + "|" + "\n")

print ( "selectivity: " + str( (1 /  (numProbe / stringIndex) ) * probeKeyMultiplicity))
