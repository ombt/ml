#!/usr/bin/python3
#
#############################################################################
#
# import required modules
#
import warnings
#
warnings.simplefilter(action='ignore', category=FutureWarning)
#
from sys import exit
import numpy as np
#
from tensorflow import keras
from keras.datasets import imdb
#
from keras import models
from keras import layers
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
exit(0)
#
# create the NN model
#
network = models.Sequential()
#
network.add(layers.Dense(512, activation='relu', input_shape=(28*28,)))
network.add(layers.Dense(10, activation='softmax'))
#
# compile model
#
network.compile(optimizer='rmsprop',
                loss='categorical_crossentropy',
                metrics=['accuracy'])
#
# get training and testing data, convert to numpy arrays, etc.
#
train_images = train_images.reshape((60000, 28*28))
train_images = train_images.astype('float32')/255
#
test_images = test_images.reshape((10000, 28*28))
test_images = test_images.astype('float32')/255
#
train_labels = to_categorical(train_labels)
test_labels = to_categorical(test_labels)
#
# fit the data
#
network.fit(train_images, train_labels, epochs=5, batch_size=128)
#
# evaluate the performance of the model against the test data.
#
test_loss, test_acc = network.evaluate(test_images, test_labels)
print('test_acc:', test_acc)
#
# all done
#
exit(0)
