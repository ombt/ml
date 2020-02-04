import sys, getopt

import itertools

max_weight = 10

unix_opts = "hw:"
gnu_opts = [ "help", "weight=" ]

try:
	args, vals = getopt.getopt(sys.argv[1:], unix_opts, gnu_opts)
except getopt.error as err:
	print(str(err))
	sys.exit(2)

for opt, optval in args:
	print ("getopt parameter: %s %s" % (opt, optval))
	if opt in ( "-h", "--help" ):
		print("usage: %s [-h] [-w max] " % (sys.argv[0]))
		sys,exit(0)
	elif opt in ( "-w", "--weight" ):
		max_weight = int(optval)

objects = [(5, 18),(2, 9), (4, 12), (6,25)]

print("Items available: ",objects)
print("***********************************")

AllCombination = [comb for k in range(0, len(objects)+1) for comb in itertools.combinations(objects, k)]

print("All combination: ")
for x in range(len(AllCombination)):
    print(AllCombination[x]),

print("***********************************")        
def ConditionControl(Subset):    
    totweight = totvalue = 0
    for weight, value in Subset:
        totweight  += weight
        totvalue += value
    return (totvalue, totweight) if totweight <= max_weight else (0, 0)
 


Subset = max(AllCombination, key=ConditionControl)
print("Subset selected: ",Subset)

value, weight = ConditionControl(Subset)
print("Total value: " ,value)
print("Total weight: ",weight)

sys.exit(0)

