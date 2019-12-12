####################################################################################
import matplotlib.pyplot as plt
data = plt.imread('stop_sign.jpg')
plt.imshow(data)
plt.show()

data[:, :, 1] = 0
data[:, :, 2] = 0
plt.imshow(data)
plt.show()

data[200:1200, 200:1200, :] = [0, 1, 0]
plt.imshow(data)
plt.show()

tshirt[10:20, 15:25] = 1
plt.imshow(tshirt)
plt.show()

####################################################################################
# Import matplotlib
import matplotlib.pyplot as plt

# Load the image
data = plt.imread('bricks.png')

# Display the image
plt.imshow(data)
plt.show()
####################################################################################
# Set the red channel in this part of the image to 1
data[:10,:10,0] = 1

# Set the green channel in this part of the image to 0
data[:10,:10,1] = 0

# Set the blue channel in this part of the image to 0
data[:10,:10,2] = 0

# Visualize the result
plt.imshow(data)
plt.show()
####################################################################################
# Representing class data: one-hot encoding
labels = ["shoe", "dress", "shoe", "t-shirt",
          "shoe", "t-shirt", "shoe", "dress"]

# Representing class data: one-hot encoding
array([[0., 0., 1.], <= shoe
       [0., 1., 0.], <= dress
       [0., 0., 1.], <= shoe
       [1., 0., 0.], <= t-shirt
       [0., 0., 1.], <= shoe
       [1., 0., 0.], <= t-shirt
       [0., 0., 1.], <= shoe
       [0., 1., 0.]]) <= dress ])

# One-hot encoding
categories = np.array(["t-shirt", "dress", "shoe"])

n_categories = 3

ohe_labels = np.zeros((len(labels), n_categories))

for ii in range(len(labels)):
    jj = np.where(categories == labels[ii])
    ohe_labels[ii, jj] = 1

####################################################################################
# The number of image categories
n_categories = 3

# The unique values of categories in the data
categories = np.array(["shirt", "dress", "shoe"])

# Initialize ohe_labels as all zeros
ohe_labels = np.zeros((len(labels), n_categories))

# Loop over the labels
for ii in range(len(labels)):
    # Find the location of this label in the categories variable
    jj = np.where(categories == labels[ii])
    ohe_labels[ii, jj] = 1
####################################################################################
# Calculate the number of correct predictions
number_correct = (test_labels * predictions).sum()
print(number_correct)

# Calculate the proportion of correct predictions
proportion_correct = number_correct/len(predictions)
print(proportion_correct)
####################################################################################
# Keras for image classi
from keras.models import Sequential
model = Sequential()

# Keras for image classification
from keras.layers import Dense
train_data.shape

model.add(Dense(10, activation='relu', input_shape=(784,)))
model.add(Dense(10, activation='relu'))
model.add(Dense(3, activation='softmax'))

model.compile(optimizer='adam',
              loss='categorical_crossentropy',
              metrics=['accuracy'])

train_data = train_data.reshape((50, 784))

model.fit(train_data, train_labels,
          validation_split=0.2,
          epochs=3)

model.fit(train_data, train_labels,
          validation_split=0.2,
          epochs=3)

test_data = test_data.reshape((10, 784))
model.evaluate(test_data, test_labels)

####################################################################################
# Imports components from Keras
from keras.models import Sequential
from keras.layers import Dense

# Initializes a sequential model
model = Sequential()

# First layer
model.add(Dense(10, activation='relu', input_shape=(784,)))

# Second layer
model.add(Dense(10, activation='relu'))

# Output layer
model.add(Dense(3, activation='softmax'))
####################################################################################
# Compile the model
model.compile(optimizer='adam', 
           loss='categorical_crossentropy', 
           metrics=['accuracy'])
####################################################################################
# Reshape the data to two-dimensional array
train_data = train_data.reshape((50,784))

# Fit the model
model.fit(train_data, train_labels, validation_split=0.2, epochs=3)
####################################################################################
# Reshape test data
test_data = test_data.reshape((10,784))

# Evaluate the model
model.evaluate(test_data, test_labels)
####################################################################################