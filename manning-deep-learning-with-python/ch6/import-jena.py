#!/usr/bin/python3

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
import numpy as np
from matplotlib import pyplot as plt

data_dir = '/mnt/d/sandbox/ml/manning-deep-learning-with-python/ch6/jena_climate/'
fname = os.path.join(data_dir, 'jena_climate_2009_2016.csv')

f = open(fname)
data = f.read()
f.close()

lines = data.split('\n')
header = lines[0].split(',')
lines = lines[1:]

print(header)
print(len(lines))

float_data = np.zeros((len(lines), len(header) - 1))
for i, line in enumerate(lines):
	values = [float(x) for x in line.split(',')[1:]]
	float_data[i, :] = values

temp = float_data[:, 1]
plt.plot(range(len(temp)), temp)
plt.show()

exit(0)
