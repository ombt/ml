#!/usr/bin/python3

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import matplotlib.pyplot as plt

from keras.datasets import imdb
from keras import preprocessing
from keras.models import Sequential
from keras.layers import Flatten, Dense, Embedding

max_features = 10000
maxlen = 20
(x_train, y_train), (x_test, y_test) = imdb.load_data(num_words=max_features)

x_train = preprocessing.sequence.pad_sequences(x_train, maxlen=maxlen)
x_test  = preprocessing.sequence.pad_sequences(x_test, maxlen=maxlen)

model = Sequential()

model.add(Embedding(10000, 8, input_length=maxlen))
model.add(Flatten())
model.add(Dense(1, activation='sigmoid'))

model.compile(optimizer='rmsprop', loss='binary_crossentropy', metrics=['accuracy'])

model.summary()

history = model.fit(x_train, y_train,
                    epochs=10,
                    batch_size=32,
                    validation_split=0.2)
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
