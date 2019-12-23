#!/usr/bin/python3
#
# load required modules
#
import numpy as np
import cv2
from matplotlib import pyplot as plt
#
# load image
#
img = cv2.imread('linux.jpg')
#
# convert oenCV BGR to pyplot RGB, then display both images to see the difference
#
b,g,r = cv2.split(img)
img2 = cv2.merge([r,g,b])
#
# plot images
#
plt.subplot(121)
plt.imshow(img)
#
plt.subplot(122)
plt.imshow(img2)
#
plt.show()
#
cv2.imshow('bgr imaage', img)
cv2.imshow('rgb image', img2)
cv2.waitKey(0)
cv2.destroyAllWindows()
#
quit(0)

