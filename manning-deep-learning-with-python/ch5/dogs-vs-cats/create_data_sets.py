#!/usr/bin/python3
#
import os, shutil
#
# if directory exists, then remove it and recrete.
#
def rm_and_mk_dir(dirpath):
    #
    if os.path.exists(dirpath):
        if os.path.isdir(dirpath):
            shutil.rmtree(dirpath)
        else:
            os.remove(dirpath)
    #
    os.mkdir(dirpath)
#
def copy_files(src_dir, dest_dir, fnames):
    #
    for fname in fnames:
        src = os.path.join(src_dir, fname)
        dst = os.path.join(dest_dir, fname)
        #
        shutil.copyfile(src, dst)
#
# original_dataset_dir = '/Users/fchollet/Downloads/kaggle_original_data'
#
original_dataset_dir = '/mnt/d/sandbox/ml/manning-deep-learning-with-python/ch5/dogs-vs-cats/train/'
#
# base_dir = '/Users/fchollet/Downloads/cats_and_dogs_small'
#
base_dir = '/mnt/d/sandbox/ml/manning-deep-learning-with-python/ch5/dogs-vs-cats/cats_and_dogs_small'
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
print('total training cat images:', len(os.listdir(train_cats_dir)))
print('total training dog images:', len(os.listdir(train_dogs_dir)))
print('total validation cat images:', len(os.listdir(validation_cats_dir)))
print('total validation dog images:', len(os.listdir(validation_dogs_dir)))
print('total test cat images:', len(os.listdir(test_cats_dir)))
print('total test dog images:', len(os.listdir(test_dogs_dir)))
#
# exit(0)
