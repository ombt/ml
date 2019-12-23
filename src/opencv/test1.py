#!/usr/bin/python3
#
# load required modules
#
import numpy as np
import cv2
#
# load image in grey scale for now
#
img = cv2.imread('linux.jpg', 0)
#
# display image and wait for any key to exit
#
cv2.imshow('image', img)
k = cv2.waitKey(0)
if k == ord('s'):
    cv2.imwrite('grey_linux.jpg', img)
#
cv2.destroyAllWindows()
#
quit(0)
