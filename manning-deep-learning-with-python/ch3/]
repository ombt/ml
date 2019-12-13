#!/usr/bin/python3
#
#############################################################################
#
# import required modules
#
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
#
from sys import exit
#
import numpy as np
import matplotlib.pyplot as plt
#
from tensorflow import keras
from keras.datasets import imdb
#
from keras import models
from keras import layers
from keras import optimizers
from keras import losses
from keras import metrics
#
from keras.utils import to_categorical
#
# get the IMDB data ...
#
((train_data, train_labels), 
 (test_data, test_labels)) = imdb.load_data(num_words=10000)
#
print(train_data.shape)
print(train_labels.shape)
#
print(test_data.shape)
print(test_labels.shape)
#
# get range if indexes
print(max([max(sequence) for sequence in train_data]))
#
# Encoding the integer sequences into a binary matrix
#
# Creates an all-zero matrix of shape (len(sequences), dimension)
#
def vectorize_sequences(sequences, dimension=10000):
    results = np.zeros((len(sequences), dimension))
    for i, sequence in enumerate(sequences):
        results[i, sequence] = 1.
    return results
#
x_train = vectorize_sequences(train_data)
x_test = vectorize_sequences(test_data)
#
y_train = np.asarray(train_labels).astype('float32')
y_test = np.asarray(test_labels).astype('float32')
#
model = models.Sequential()
#
model.add(layers.Dense(16, activation='relu', input_shape=(10000,)))
model.add(layers.Dense(16, activation='relu'))
model.add(layers.Dense(1, activation='sigmoid'))
#
# compiling using binary crossentopy since it is a two-state output
#
# model.compile(optimizer='rmsprop',
#               loss='binary_crossentropy',
#               metrics=['accuracy'])
#
# other ways to comoile and offering more control
#
# Configuring the optimizer
#
# model.compile(optimizer=optimizers.RMSprop(lr=0.001),
#               loss='binary_crossentropy',
#               metrics=['accuracy'])
#
# Using custom losses and metrics
#
# model.compile(optimizer=optimizers.RMSprop(lr=0.001),
#               loss=losses.binary_crossentropy,
#               metrics=[metrics.binary_accuracy])
#
# Setting aside a validation set
#
x_val = x_train[:10000]
partial_x_train = x_train[10000:]
#
y_val = y_train[:10000]
partial_y_train = y_train[10000:]
#
# Training your model
model.compile(optimizer='rmsprop',
              loss='binary_crossentropy',
              metrics=['accuracy'])
#
history = model.fit(partial_x_train,
                    partial_y_train,
                    epochs=20,
                    batch_size=512,
                    validation_data=(x_val, y_val))
#
# get history of fit and plot results
#
history_dict = history.history
#
acc             = history.history['acc']
loss_values     = history_dict['loss']
val_loss_values = history_dict['val_loss']
epochs          = range(1, len(acc) + 1)
#
plt.plot(epochs, 
         loss_values, 
         'bo', 
         label='Training loss')
plt.plot(epochs, 
         val_loss_values, 
         'b', 
         label='Validation loss')
plt.title('Training and validation loss')
plt.xlabel('Epochs')
plt.ylabel('Loss')
plt.legend()
#
plt.show()
#
exit(0)
