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
from keras.datasets import boston_housing
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
# get the boston housing data ...
#
((train_data, train_targets), 
 (test_data, test_targets)) = boston_housing.load_data()
#
print(train_data.shape)
print(train_targets.shape)
#
print(test_data.shape)
print(test_targets.shape)
#
# get range if indexes
#
print(max([max(sequence) for sequence in train_data]))
#
# Normalizing the data
#
# Note that the quantities used for normalizing the test data are computed using the
# training data. You should never use in your workflow any quantity computed on the
# test data, even for something as simple as data normalization.
#
mean = train_data.mean(axis=0)
train_data -= mean
#
std = train_data.std(axis=0)
train_data /= std
#
test_data -= mean
test_data /= std
#
# fucntion to create a model
#
def build_model():
    model = models.Sequential()
    #
    model.add(layers.Dense(64, 
                           activation='relu',
                           input_shape=(train_data.shape[1],)))
    model.add(layers.Dense(64, 
                           activation='relu'))
    model.add(layers.Dense(1))
    #
    # loss function is mean-squared error
    #
    model.compile(optimizer='rmsprop', 
                  loss='mse', 
                  metrics=['mae'])
    #
    return model
#
# data sets are small, so use k-fold validation
#
k = 4
num_val_samples = len(train_data) // k
num_epochs = 100
all_scores = []
#
for i in range(k):
    print('processing fold #', i)
    val_data = train_data[i * num_val_samples: (i + 1) * num_val_samples]
    val_targets = train_targets[i * num_val_samples: (i + 1) * num_val_samples]
    partial_train_data = np.concatenate([train_data[:i * num_val_samples],
                                         train_data[(i + 1) * num_val_samples:]],
                                        axis=0)
    partial_train_targets = np.concatenate([train_targets[:i * num_val_samples],
                                            train_targets[(i + 1) * num_val_samples:]],
                                            axis=0)
    #
    model = build_model()
    #
    model.fit(partial_train_data, 
              partial_train_targets,
              epochs=num_epochs, 
              batch_size=1, 
              verbose=0)
    val_mse, val_mae = model.evaluate(val_data, val_targets, verbose=0)
    all_scores.append(val_mae)
#
print(all_scores)
print(np.mean(all_scores))
#
exit(0)
