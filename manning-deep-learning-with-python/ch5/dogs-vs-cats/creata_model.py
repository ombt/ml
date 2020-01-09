#!/usr/bin/python3
#
from keras import layers
from keras import models
from keras import optimizers
#
model = models.Sequential()
#
model.add(layers.Conv2D(32, (3, 3), 
                        activation='relu',
                        input_shape=(150, 150, 3)))
model.add(layers.MaxPooling2D((2, 2)))
#
model.add(layers.Conv2D(64, (3, 3), 
                        activation='relu'))
model.add(layers.MaxPooling2D((2, 2)))
#
model.add(layers.Conv2D(128, (3, 3), 
                        activation='relu'))
model.add(layers.MaxPooling2D((2, 2)))
#
model.add(layers.Conv2D(128, (3, 3), 
                        activation='relu'))
model.add(layers.MaxPooling2D((2, 2)))
#
model.add(layers.Flatten())
#
model.add(layers.Dense(512, 
                       activation='relu'))
model.add(layers.Dense(1, 
                       activation='sigmoid'))
#
model.summary()
#
model.compile(loss='binary_crossentropy',
               optimizer=optimizers.RMSprop(lr=1e-4),
               metrics=['acc'])
#
exit(0)
