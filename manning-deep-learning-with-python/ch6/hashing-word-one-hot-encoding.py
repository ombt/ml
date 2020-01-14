#!/usr/bin/python3
#
import sys
import numpy as np
np.set_printoptions(threshold=sys.maxsize)
#
samples = ['The cat sat on the mat.', 
           'The dog ate my homework.']
#
dimensionality = 20
max_length = 10
#
results = np.zeros((len(samples), max_length, dimensionality))
#
for i, sample in enumerate(samples):
    print('i:', i)
    print('sample:', sample)
    for j, word in list(enumerate(sample.split()))[:max_length]:
        print('j:', j)
        print('word:', word)
        index = abs(hash(word)) % dimensionality
        print('index:', index)
        results[i, j, index] = 1.
#
print(results)
#
exit(9)
