##########################################################################
Importing data for use in TensorFlow
Data can be imported using tensorflow
	Useful for managing complex pipelines
	Not necessary for this chapter
Simpler option used in this chapter
	Import data using pandas
	Convert data to numpy array
	Use in tensorflow without modi

How to import and convert data

# Import numpy and pandas
import numpy as np
import pandas as pd

# Load data from csv
housing = pd.read_csv('kc_housing.csv')

# Convert to numpy array
housing = np.array(housing)

We will focus on data stored in csv format in this chapter
Pandas also has methods for handling data in other formats
E.g. read_json() , read_html() , read_excel()

Parameters of read_csv()
Parameter Description Default
filepath_or_buffer Accepts a or a URL None
sep Delimiter between columns. ,
delim_whitespace Boolean for whether to delimit whitespace. False
encoding Specifies encoding to be used if any None

Setting the data type
# Load KC dataset
housing = pd.read_csv('kc_housing.csv')
# Convert price column to float32
price = np.array(housing['price'], np.float32)
# Convert waterfront column to Boolean
waterfront = np.array(housing['waterfront'], np.bool)

Setting the data type
# Load KC dataset
housing = pd.read_csv('kc_housing.csv')
# Convert price column to float32
price = tf.cast(housing['price'], tf.float32)
# Convert waterfront column to Boolean
waterfront = tf.cast(housing['waterfront'], tf.bool)

##########################################################################
# Import pandas under the alias pd
import	 pandas as pd

# Assign the path to a string variable named data_path
data_path = 'kc_house_data.csv'

# Load the dataset as a dataframe named housing
housing = pd.read_csv(data_path)

# Print the price column of housing
print(housing['price'])
##########################################################################
# Import numpy and tensorflow with their standard aliases
import numpy as np
import tensorflow as tf

# Use a numpy array to define price as a 32-bit float
price = np.array(housing['price'], np.float32)

# Define waterfront as a Boolean using cast
waterfront = tf.cast(housing['waterfront'], tf.bool)

# Print price and waterfront
print(price)
print(waterfront)
##########################################################################
Introduction to loss functions
Fundamental tensorflow operation
	Used to train a model
	Measure of model fit
higher vlue -> worse fit
	minimize the loss function

Common loss functions in TensorFlow
TensorFlow has operations for common loss functions
Mean squared error (MSE)
Mean absolute error (MAE)
Huber error
Loss functions are accessible from tf.keras.losses()
tf.keras.losses.mse()
tf.keras.losses.mae()
tf.keras.losses.Huber()

Why do we care about loss functions?
MSE
Strongly penalizes outliers
High sensitivity near minimum
MAE
Scales linearly with size of error
Low sensitivity near minimum
Huber
Similar to MSE near minimum
Similar to MAE away from minimum

##########################################################################
Defining a loss fcuntion
# Import TensorFlow under standard alias
import tensorflow as tf
# Compute the MSE loss
loss = tf.keras.losses.mse(targets, predictions)

# Define a linear regression model
def linear_regression(intercept, slope = slope, features = features):
return intercept + features*slope
# Define a loss function to compute the MSE
def loss_function(intercept, slope, targets = targets, features = features):
# Compute the predictions for a linear model
predictions = linear_regression(intercept, slope)
# Return the loss
return tf.keras.losses.mse(targets, predictions)

# Compute the loss for test data inputs
loss_function(intercept, slope, test_targets, test_features)

# Compute the loss for default data inputs
loss_function(intercept, slope)

##########################################################################
# Initialize a variable named scalar
scalar = Variable(1.0, float32)

# Define the model
def model(scalar, features = features):
  	return scalar * features

# Define a loss function
def loss_function(scalar, features = features, targets = targets):
	# Compute the predicted values
	predictions = model(scalar, features)
    
	# Return the mean absolute error loss
	return keras.losses.mae(targets, predictions)

# Evaluate the loss function and print the loss
print(loss_function(scalar).numpy())
##########################################################################
The linear regression model
A linear regression model assumes a linear relationship:
	price = intercept + size ∗ slope + error
This is an example of a univariate regression.
	There is only one feature, size .
Multiple regression models have more than one feature.
	E.g. size and location

Linear regression in TensorFlow

# Define the targets and features
price = np.array(housing['price'], np.float32)
size = np.array(housing['sqft_living'], np.float32)

# Define the intercept and slope
intercept = tf.Variable(0.1, np.float32)
slope = tf.Variable(0.1, np.float32)

# Define a linear regression model
def linear_regression(intercept, slope, features = size):
	return intercept + features*slope

# Compute the predicted values and loss
def loss_function(intercept, slope, targets = price, features = size):
	predictions = linear_regression(intercept, slope)
	return tf.keras.losses.mse(targets, predictions)

# Define an optimization operation
opt = tf.keras.optimizers.Adam()

# Minimize the loss function and print the loss
for j in range(1000):
	opt.minimize(lambda: loss_function(intercept, slope),\
	var_list=[intercept, slope])
	print(loss_function(intercept, slope))

# Print the trained parameters
print(intercept.numpy(), slope.numpy())

##########################################################################
# Define a linear regression model
def linear_regression(intercept, slope, features = size_log):
	return intercept + slope*features

# Set loss_function() to take the variables as arguments
def loss_function(intercept, slope, features = size_log, targets = price_log):
	# Set the predicted values
	predictions = linear_regression(intercept, slope, features)
    
    # Return the mean squared error loss
	return keras.losses.mse(predictions, targets)

# Compute the loss for different slope and intercept values
print(loss_function(0.1, 0.1).numpy())
print(loss_function(0.1, 0.5).numpy())
##########################################################################
# Initialize an adam optimizer
opt = keras.optimizers.Adam(0.5)

for j in range(100):
	# Apply minimize, pass the loss function, and supply the variables
	opt.minimize(lambda: loss_function(intercept, slope), var_list=[intercept, slope])

	# Print every 10th value of the loss
	if j % 10 == 0:
		print(loss_function(intercept, slope).numpy())

# Plot data and regression line
plot_results(intercept, slope)
##########################################################################
##########################################################################
##########################################################################
##########################################################################
##########################################################################
##########################################################################
##########################################################################
