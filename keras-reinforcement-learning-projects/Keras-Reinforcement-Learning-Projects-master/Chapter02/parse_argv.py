#!/usr/bin/python3

import sys, getopt

# count the arguments
arguments = len(sys.argv) - 1

# output argument-wise
position = 1
while (arguments >= position):
	print ("parameter %i: %s" % (position, sys.argv[position]))
	position = position + 1

for iarg, arg in enumerate(sys.argv[1:]):
	print ("enum parameter %i: %s" % (iarg, arg))

unix_opts = "ho:v"
gnu_opts = [ "help", "output=", "verbose" ]

try:
	args, vals = getopt.getopt(sys.argv[1:], unix_opts, gnu_opts)
except getopt.error as err:
	print(str(err))
	sys.exit(2)

print("args, vals", args, vals)

for opt, optval in args:
	print ("getopt parameter: %s %s" % (opt, optval))

for val in vals:
	print ("getopt value: %s" % (val))
	
exit(0)
