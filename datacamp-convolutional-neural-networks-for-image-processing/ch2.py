####################################################################################
# what is a convolution?
array = np.array([0, 0, 0, 0, 0, 1, 1, 1, 1, 1])
kernel = np.array([-1, 1])
conv = np.array([0, 0, 0, 0, 0, 0, 0, 0, 0])
conv[0] = (kernel * array[0:2]).sum()
conv[1] = (kernel * array[1:3]).sum()
conv[2] = (kernel * array[2:4]).sum()
...
for ii in range(8):
    conv[ii] = (kernel * array[ii:ii+2]).sum()

conv

# convolution in one dimension
array = np.array([0, 0, 1, 1, 0, 0, 1, 1, 0, 0])
kernel = np.array([-1, 1])

conv = np.array([0, 0, 0, 0, 0, 0, 0, 0, 0])

for ii in range(8):
    conv[ii] = (kernel * array[ii:ii+2]).sum()
conv

# two-dimensional convolution
kernel = np.array([[-1, 1],
                   [-1, 1]])
conv = np.zeros((27, 27)

for ii in range(27):
    for jj in range(27):
        window = image[ii:ii+2, jj:jj+2]
        conv[ii, jj] = np.sum(window * kernel)

####################################################################################
array = np.array([1, 0, 1, 0, 1, 0, 1, 0, 1, 0])
kernel = np.array([1, -1, 0])
conv = np.array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

# Output array
for ii in range(8):
    conv[ii] = (kernel * array[ii:ii+3]).sum() 

# Print conv
print(conv)

####################################################################################
kernel = np.array([[0, 1, 0], [1, 1, 1], [0, 1, 0]])
result = np.zeros(im.shape)

# Output array
for ii in range(im.shape[0] - 3):
    for jj in range(im.shape[1] - 3):
        result[ii, jj] = (im[ii:ii+3, jj:jj+3] * kernel).sum()

# Print result
print(result)
####################################################################################
# horizontal line
kernel = np.array([[-1, -1, -1], 
                   [1, 1, 1],
                   [-1, -1, -1]])

# bright spot surrounded by dark area
kernel = np.array([[-1, -1, -1], 
                   [-1, 1, -1],
                   [-1, -1, -1]])

# dark spot surrounded by bright area
kernel = np.array([[1, 1, 1], 
                   [1, -1, 1],
                   [1, 1, 1]])

####################################################################################
# keras convolution layer
from keras.layers import Conv2D
Conv2D(10, kernel_size=3, activation='relu')

# Integrating convolution layers into a network
from keras.models import Sequential
from keras.layers import Dense, Conv2D, Flatten

model = Sequential()
model.add(Conv2D(10, 
                 kernel_size=3, 
                 activation='relu',
                 input_shape=(img_rows, img_cols, 1)))

model.add(Flatten())
model.add(Dense(3, activation='softmax'))

# fitting a CNN
model.compile(optimizer='adam',
              loss='categorical_crossentropy',
              metrics=['accuracy'])
train_data.shape

(50, 28, 28, 1)

model.fit(train_data, 
          train_labels, 
          validation_split=0.2,
          epochs=3)

model.evaluate(test_data, test_labels, epochs=3)

####################################################################################
# Import the necessary components from Keras
from keras.models import Sequential
from keras.layers import Dense, Conv2D, Flatten

# Initialize the model object
model = Sequential()

# Add a convolutional layer
model.add(Conv2D(10, kernel_size=3, activation='relu', 
               input_shape=(img_rows, img_cols, 1)))

# Flatten the output of the convolutional layer
model.add(Flatten())
# Add an output layer for the 3 categories
model.add(Dense(3, activation='softmax'))

####################################################################################
# Compile the model 
model.compile(optimizer='adam', 
              loss='categorical_crossentropy', 
              metrics=['accuracy'])

# Fit the model on a training set
model.fit(train_data, train_labels, 
          validation_split=0.20, 
          epochs=3, batch_size=10)
####################################################################################
model.evaluate(test_data, test_labels, batch_size=10)

####################################################################################
# Zero padding in Keras 
# padding='valid' is the default and means 'no zero padding'
model.add(Conv2D(10, 
                 kernel_size=3, 
                 activation='relu',
                 input_shape=(img_rows, img_cols, 1)),
                 padding='valid')

# padding='same' enables Zero padding
model.add(Conv2D(10, 
                 kernel_size=3, 
                 activation='relu',
                 input_shape=(img_rows, img_cols, 1)),
                 padding='same') # enables Zero padding

# strides in Keras (default is one)
model.add(Conv2D(10, 
                 kernel_size=3, 
                 activation='relu',
                 input_shape=(img_rows, img_cols, 1)),
                 strides=1)

# strides not equal to one is NOT the default.
model.add(Conv2D(10, 
          kernel_size=3, 
          activation='relu',
          input_shape=(img_rows, img_cols, 1)),
          strides=2)

# Calculating the size of the output
#
# O = ((I − K + 2P )/S) + 1
#
# where
#
# I = size of the input
# K = size of the kernel
# P = size of the zero padding
# S = strides

# Dilation in Keras
model.add(Conv2D(10, 
          kernel_size=3, 
          activation='relu',
          input_shape=(img_rows, img_cols, 1)),
          dilation_rate=2)
####################################################################################
# Initialize the model
model = Sequential()

# Add the convolutional layer
model.add(Conv2D(10, kernel_size=3, activation='relu', 
                 input_shape=(img_rows, img_cols, 1), 
                 padding='same'))

# Feed into output layer
model.add(Flatten())
model.add(Dense(3, activation='softmax'))
####################################################################################
# Initialize the model
model = Sequential()

# Add the convolutional layer
model.add(Conv2D(10, kernel_size=3, activation='relu', 
              input_shape=(img_rows, img_cols, 1), 
              strides=2))

# Feed into output layer
model.add(Flatten())
model.add(Dense(3, activation='softmax'))
####################################################################################
