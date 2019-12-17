#!/usr/bin/python3
#
# load required modules
#
import numpy as np
import cv2
#
from matplotlib import pyplot as plt
#
# load image in grey scale for now
#
img = cv2.imread('linux.jpg', 0)
#
# display image and wait for any key to exit
#
plt.imshow(img, cmap='gray', interpolation='bicubic')
plt.xticks([])
plt.yticks([])
plt.show()
#
quit(0)
