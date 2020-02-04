#!/usr/bin/python3

import sys, getopt, time

from random import seed
from random import random

from matplotlib import pyplot

prob = 0.5
iseed = int(time.time()) & 1
nsteps = 1000

unix_opts = "hs:n:p:"
gnu_opts = [ "help", "seed=", "steps=", "prob=" ]

try:
	args, vals = getopt.getopt(sys.argv[1:], unix_opts, gnu_opts)
except getopt.error as err:
	print(str(err))
	sys.exit(2)

for opt, optval in args:
	print ("getopt parameter: %s %s" % (opt, optval))
	if opt in ( "-h", "--help" ):
		print("usage: %s [-h] [-s seed] [-n steps] [-p prob]" % (sys.argv[0]))
		sys,exit(0)
	elif opt in ( "-s", "--seed" ):
		iseed = int(optval)
	elif opt in ( "-n", "--steps" ):
		nsteps = int(optval)
	elif opt in ( "-p", "--prob" ):
		prob = float(optval)

seed(iseed)

RandomWalk = list()
RandomWalk.append(-1 if random() < 0.5 else 1)

for i in range(1, nsteps):
	Zn = -1 if random() < prob else 1
	Xn = RandomWalk[i-1] + Zn
	RandomWalk.append(Xn)

pyplot.plot(RandomWalk)
pyplot.show()

exit(0)
