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
from keras.datasets import reuters
#
from keras import models
from keras import layers
from keras import optimizers
from keras import losses
from keras import metrics
#
from keras.utils import to_categorical
from keras.utils.np_utils import to_categorical
#
# get the IMDB data ...
#
((train_data, train_labels), 
 (test_data, test_labels)) = reuters.load_data(num_words=10000)
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
# one-hot-encoding of labels
#
# def to_one_hot(labels, dimension=46):
#     results = np.zeros((len(labels), dimension))
#     for i, label in enumerate(labels):
#         results[i, label] = 1.
#     return results
#
# one_hot_train_labels = to_one_hot(train_labels)
# one_hot_test_labels = to_one_hot(test_labels)
#
# use keras util to perform encodings
#
one_hot_train_labels = to_categorical(train_labels)
one_hot_test_labels = to_categorical(test_labels)
#
# Model definition - have more inputs than we have expected outputs
#
model = models.Sequential()
#
model.add(layers.Dense(64, 
                       activation='relu', 
                       input_shape=(10000,)))
model.add(layers.Dense(64, 
                       activation='relu'))
model.add(layers.Dense(46, 
                       activation='softmax'))
#
# Compiling the model
#
model.compile(optimizer='rmsprop',
           loss='categorical_crossentropy',
           metrics=['accuracy'])
#
# Setting aside a validation set
#
x_val           = x_train[:1000]
partial_x_train = x_train[1000:]
#
y_val           = one_hot_train_labels[:1000]
partial_y_train = one_hot_train_labels[1000:]
#
# Training the model
#
history = model.fit(partial_x_train,
                    partial_y_train,
                    epochs=20,
                    batch_size=512,
                    validation_data=(x_val, y_val))
#
# plot training and validation loss
#
loss     = history.history['loss']
val_loss = history.history['val_loss']
#
epochs = range(1, len(loss) + 1)
#
plt.plot(epochs, 
         loss, 
         'bo', 
          label='Training loss')
plt.plot(epochs, 
         val_loss, 
         'b', 
         label='Validation loss')
#
plt.title('Training and validation loss')
plt.xlabel('Epochs')
plt.ylabel('Loss')
plt.legend()
#
plt.show()
#
# Clears the figure
#
plt.clf()
#
# plot training and validation accuracy
#
acc = history.history['accuracy']
val_acc = history.history['val_accuracy']
#
plt.plot(epochs, acc, 'bo', label='Training acc')
plt.plot(epochs, val_acc, 'b', label='Validation acc')
#
plt.title('Training and validation accuracy')
plt.xlabel('Epochs')
plt.ylabel('Loss')
plt.legend()
#
plt.show()
#
exit(0)
