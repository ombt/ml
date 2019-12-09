####################################################################################
# tracking learning

training = model.fit(train_data, 
                     train_labels,
                     epochs=3, 
                     validation_split=0.2)

import matplotlib.pyplot as plt
plt.plot(training.history['loss'])
plt.plot(training.history['val_loss'])
plt.show()

####################################################################################
# storing the optimal parameters automatically
from keras.callbacks import ModelCheckpoint

# This checkpoint object will store the model parameters
# in the file "weights.hdf5"
checkpoint = ModelCheckpoint('weights.hdf5', 
                             monitor='val_loss',
                             save_best_only=True)
# Store in a list to be used during training
callbacks_list = [checkpoint]

# Fit the model on a training set, using the checkpoint as a callback
model.fit(train_data, 
          train_labels, 
          validation_split=0.2,
          epochs=3, 
          callbacks=callbacks_list)

# loading stored parameters
model.load_weights('weights.hdf5')
model.predict_classes(test_data)

####################################################################################
import matplotlib.pyplot as plt

# Train the model and store the training object
training = model.fit(train_data, 
                     train_labels,
                     epochs=3, 
                     validation_split=0.2,
                     batch_size=10)


# Extract the history from the training object
history = training.history

# Plot the training loss 
plt.plot(history['loss'])
# Plot the validation loss
plt.plot(history['val_loss'])

# Show the figure
plt.show()
####################################################################################
# Load the weights from file
model.load_weights('weights.hdf5')

# Predict from the first three images in the test data
model.predict(test_data)
####################################################################################
# Load the weights from file
model.load_weights('weights.hdf5')

# Predict from the first three images in the test data
model.predict(test_data[:3])

####################################################################################
# dropout in keras
from keras.models import Sequential
from keras.layers import Dense, Conv2D, Flatten, Dropout

model = Sequential()

model.add(Conv2D(5, 
                 kernel_size=3, 
                 activation='relu',
                 input_shape=(img_rows, img_cols, 1)))

model.add(Dropout(0.25))

model.add(Conv2D(15, 
                 kernel_size=3, 
                 activation='relu'))
model.add(Flatten())

model.add(Dense(3, activation='softmax'))

####################################################################################
# batch normalization in keras
from keras.models import Sequential
from keras.layers import Dense, Conv2D, Flatten, BatchNorma

model = Sequential()

model.add(Conv2D(5, 
          kernel_size=3, 
          activation='relu',
          input_shape=(img_rows, img_cols, 1)))

model.add(BatchNormalization())

model.add(Conv2D(15, 
                 kernel_size=3, 
                 activation='relu'))

model.add(Flatten())
model.add(Dense(3, activation='softmax'))

####################################################################################
# Add a convolutional layer
model.add(Conv2D(15, kernel_size=2, activation='relu', 
                 input_shape=(img_rows, img_cols, 1)))

# Add a dropout layer
model.add(Dropout(0.20))

# Add another convolutional layer
model.add(Conv2D(5, kernel_size=2, activation='relu'))

# Flatten and feed to output layer
model.add(Flatten())
model.add(Dense(3, activation='softmax'))
####################################################################################
# Add a convolutional layer
model.add(Conv2D(15, kernel_size=2, activation='relu', 
                 input_shape=(img_rows, img_cols, 1)))

# Add batch normalization layer
model.add(BatchNormalization())


# Add another convolutional layer
model.add(Conv2D(5, kernel_size=2, activation='relu'))

# Flatten and feed to output layer
model.add(Flatten())
model.add(Dense(3, activation='softmax'))
####################################################################################
# Load the weights into the model
model.load_weights('weights.hdf5')

# Get the first convolutional layer from the model
c1 = model.layers[0]

# Get the weights of the first convolutional layer
weights1 = c1.get_weights()

# Pull out the first channel of the first kernel in the first layer
kernel = weights1[0][...,0, 0]
print(kernel)
####################################################################################
import matplotlib.pyplot as plt

# Convolve with the fourth image in test_data
out = convolution(test_data[4, :, :, 0], kernel)

# Visualize the result
plt.imshow(out)
plt.show()
####################################################################################
import matplotlib.pyplot as plt

# Convolve with the fourth image in test_data
out = convolution(test_data[4, :, :, 0], kernel)

# Visualize the result
plt.imshow(out)
plt.show()
####################################################################################
####################################################################################
####################################################################################
####################################################################################
####################################################################################
####################################################################################
####################################################################################
####################################################################################
