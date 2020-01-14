#!/usr/bin/python3
#
###############################################################################
#
# imports
#
import warnings
#
warnings.simplefilter(action='ignore', category=FutureWarning)
#
import os, shutil
import numpy as np
#
from keras import layers
from keras import models
from keras import optimizers
from keras.preprocessing.image import ImageDataGenerator
#
import matplotlib.pyplot as plt
#
# import pretrained network
#
from keras.applications import VGG16
#
###############################################################################
#
# globals
#
original_dataset_dir = '/mnt/d/sandbox/ml/manning-deep-learning-with-python/ch5/dogs-vs-cats/train/'
#
base_dir = '/mnt/d/sandbox/ml/manning-deep-learning-with-python/ch5/dogs-vs-cats/cats_and_dogs_small_2'
#
###############################################################################
#
# functions
#
# if directory exists, then remove it and recrete.
#
def rm_and_mk_dir(dirpath):
    #
    if os.path.exists(dirpath):
        print('Removing path:', dirpath)
        if os.path.isdir(dirpath):
            shutil.rmtree(dirpath)
        else:
            os.remove(dirpath)
    #
    print('Creating path:', dirpath)
    os.mkdir(dirpath)
#
def copy_files(src_dir, dest_dir, fnames):
    #
    print('Copy files from:', src_dir)
    print('Copy files to:', dest_dir)
    #
    for fname in fnames:
        src = os.path.join(src_dir, fname)
        dst = os.path.join(dest_dir, fname)
        #
        shutil.copyfile(src, dst)
    #
    print('Files copied:', len(os.listdir(dest_dir)))
#
def extract_features(directory, sample_count):
    #
    features = np.zeros(shape=(sample_count, 4, 4, 512))
    labels   = np.zeros(shape=(sample_count))
    #
    generator = datagen.flow_from_directory(directory,
                                            target_size=(150, 150),
                                            batch_size=batch_size,
                                            class_mode='binary')
    #
    i = 0
    for inputs_batch, labels_batch in generator:
        features_batch = conv_base.predict(inputs_batch)
        features[i * batch_size : (i + 1) * batch_size] = features_batch
        labels[i * batch_size : (i + 1) * batch_size] = labels_batch
        #
        i += 1
        if i * batch_size >= sample_count:
            break
    #
    return features, labels
#
def rm_and_mk_all_dirs(base_dir):
    #
    rm_and_mk_dir(base_dir)
    #
    train_dir = os.path.join(base_dir, 'train')
    rm_and_mk_dir(train_dir)
    #
    validation_dir = os.path.join(base_dir, 'validation')
    rm_and_mk_dir(validation_dir)
    #
    test_dir = os.path.join(base_dir, 'test')
    rm_and_mk_dir(test_dir)
    #
    train_cats_dir = os.path.join(train_dir, 'cats')
    rm_and_mk_dir(train_cats_dir)
    #
    train_dogs_dir = os.path.join(train_dir, 'dogs')
    rm_and_mk_dir(train_dogs_dir)
    #
    validation_cats_dir = os.path.join(validation_dir, 'cats')
    rm_and_mk_dir(validation_cats_dir)
    #
    validation_dogs_dir = os.path.join(validation_dir, 'dogs')
    rm_and_mk_dir(validation_dogs_dir)
    #
    test_cats_dir = os.path.join(test_dir, 'cats')
    rm_and_mk_dir(test_cats_dir)
    #
    test_dogs_dir = os.path.join(test_dir, 'dogs')
    rm_and_mk_dir(test_dogs_dir)
    #
    fnames = ['cat.{}.jpg'.format(i) for i in range(1000)]
    copy_files(original_dataset_dir, train_cats_dir, fnames)
    #
    fnames = ['cat.{}.jpg'.format(i) for i in range(1000, 1500)]
    copy_files(original_dataset_dir, validation_cats_dir, fnames)
    #
    fnames = ['cat.{}.jpg'.format(i) for i in range(1500, 2000)]
    copy_files(original_dataset_dir, test_cats_dir, fnames)
    # 
    fnames = ['dog.{}.jpg'.format(i) for i in range(1000)]
    copy_files(original_dataset_dir, train_dogs_dir, fnames)
    #
    fnames = ['dog.{}.jpg'.format(i) for i in range(1000, 1500)]
    copy_files(original_dataset_dir, validation_dogs_dir, fnames)
    #
    fnames = ['dog.{}.jpg'.format(i) for i in range(1500, 2000)]
    copy_files(original_dataset_dir, test_dogs_dir, fnames)
    #
    return train_dir, validation_dir, test_dir

#
###############################################################################
#
# main starts
#
# create data directories and copy data files.
#
print('Data Source Directory:', original_dataset_dir)
#
train_dir, validation_dir, test_dir = rm_and_mk_all_dirs(base_dir)
#
###############################################################################
#
# get pretrained convolution networks
#
conv_base = VGG16(weights='imagenet',
                  include_top=False,
                  input_shape=(150, 150, 3))
conv_base.summary()
#
###############################################################################
# 
# run data sets through existing convolution network
#
datagen = ImageDataGenerator(rescale=1./255)
batch_size = 20
#
train_features, train_labels           = extract_features(train_dir, 2000)
validation_features, validation_labels = extract_features(validation_dir, 1000)
test_features, test_labels             = extract_features(test_dir, 1000)
#
train_features      = np.reshape(train_features, (2000, 4 * 4 * 512))
validation_features = np.reshape(validation_features, (1000, 4 * 4 * 512))
test_features       = np.reshape(test_features, (1000, 4 * 4 * 512))
#
###############################################################################
#
print('Defining dense-layer model:')
#
model = models.Sequential()
#
model.add(layers.Dense(256, 
                       activation='relu', 
                       input_dim=4 * 4 * 512))
model.add(layers.Dropout(0.5))
model.add(layers.Dense(1, activation='sigmoid'))
#
model.compile(optimizer=optimizers.RMSprop(lr=2e-5),
              loss='binary_crossentropy',
              metrics=['acc'])
#
model.summary()
#
print('Fit dense-layer model:')
#
history = model.fit(train_features, 
                    train_labels,
                    epochs=30,
                    batch_size=20,
                    validation_data=(validation_features, 
                                     validation_labels))
#
model.compile(loss='binary_crossentropy',
               optimizer=optimizers.RMSprop(lr=1e-4),
               metrics=['acc'])
#
model.save('pretrained_cats_and_dogs_small.h5')
#
print('Plot dense-layer model results:')
#
acc      = history.history['acc']
val_acc  = history.history['val_acc']
loss     = history.history['loss']
val_loss = history.history['val_loss']
#
epochs = range(1, len(acc) + 1)
#
plt.plot(epochs, acc, 'bo', label='Training acc')
plt.plot(epochs, val_acc, 'b', label='Validation acc')
plt.title('Training and validation accuracy')
plt.legend()
#
plt.figure()
#
plt.plot(epochs, loss, 'bo', label='Training loss')
plt.plot(epochs, val_loss, 'b', label='Validation loss')
plt.title('Training and validation loss')
plt.legend()
#
plt.show()
#
exit(0)
