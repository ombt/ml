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
 (test_data, test_labels)) = imdb.load_data()
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
# decode in of the reviews
#
word_index = imdb.get_word_index()
#
reverse_word_index = dict(
    [(value, key) for (key, value) in word_index.items()])
decoded_review = ' '.join(
    [reverse_word_index.get(i - 3, '?') for i in train_data[0]])#
#
print(decoded_review)
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
