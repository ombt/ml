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
#
from keras import layers
from keras import models
from keras import optimizers
from keras.preprocessing.image import ImageDataGenerator
#
import matplotlib.pyplot as plt
#
###############################################################################
#
# globals
#
original_dataset_dir = '/mnt/d/sandbox/ml/manning-deep-learning-with-python/ch5/dogs-vs-cats/train/'
#
base_dir = '/mnt/d/sandbox/ml/manning-deep-learning-with-python/ch5/dogs-vs-cats/cats_and_dogs_small'
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
###############################################################################
#
# main
#
print('Data Source Directory:', original_dataset_dir)
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
print('Defining model:')
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
# preprocess images
#
train_datagen = ImageDataGenerator(rescale=1./255)
test_datagen = ImageDataGenerator(rescale=1./255)
#
train_generator = train_datagen.flow_from_directory(train_dir,
                                                    target_size=(150, 150),
                                                    batch_size=20,
                                                    class_mode='binary')
#
validation_generator = test_datagen.flow_from_directory(validation_dir,
                                                        target_size=(150, 150),
                                                        batch_size=20,
                                                        class_mode='binary')
#
history = model.fit_generator(train_generator,
                              steps_per_epoch=100,
                              epochs=10, # was 60
                              validation_data=validation_generator,
                              validation_steps=50)
#
model.save('cats_and_dogs_small_1.h5')
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



