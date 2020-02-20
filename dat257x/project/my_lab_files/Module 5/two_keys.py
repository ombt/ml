#!/usr/bin/python3

import sys
import numpy as np

class TupleTwoKeys:
    def __init__(self):
        self.data = dict()

    def insert(self, data, key1, key2=None):
        if (key2 == None):
            self.data[key1] = dict(data)
        else:
            self.data[key1][key2] = data
        return True

    def remove(self, key1, key2=None):
        if (key1 in self.data):
            if (key2 == None):
                try:
                    del self.data[key1]
                except:
                    pass
            else:
                try:
                    del self.data[key1][key2]
                except:
                    pass
        return True

    def read(self, key1, key2=None):
        if (key1 in self.data):
            if (key2 == None):
                return self.data[key1]
            elif (key2 in self.data[key1]):
                return self.data[key1][key2]
            else:
                return None
        else:
            return None

    def update(self, data, key1, key2=None):
        if (key2 == None):
            self.data[key1] = dict(data)
        else:
            self.data[key1][key2] = data
        return True

    def keys(self, key1=None):
        if (key1 == None):
            return self.data.keys()
        elif (key1 in self.data):
            return self.data[key1].keys()
        else:
            return dict()

if (__name__ == "__main__"):

    d2k = TupleTwoKeys()

    d2k.insert({ 'a': 1, 'b': 2 }, "ab")
    d2k.insert({ 'c': 3, 'd': 4 }, "cd")

    print("read ... ", d2k.read("ab"))
    print("read ... ", d2k.read("ab","a"))

    for key1 in d2k.keys():
        for key2 in d2k.keys(key1):
            print("(k1,k2)", key1, key2)
            print("({},{})={}".format(key1, key2, d2k.read(key1,key2)))

    sys,exit(0)

