#####################################################################################
# Simple model with two outputs
from keras.layers import Input, Concatenate, Dense
input_tensor = Input(shape=(1,))
output_tensor = Dense(2)(input_tensor)

from keras.models import Model
model = Model(input_tensor, output_tensor)
model.compile(optimizer='adam', loss='mean_absolute_error')

# Fitting a model with 2 outputs
games_tourney_train[['seed_diff', 'score_1', 'score_2']].head()

seed_diff score_1 score_2
0 -3 41 50
1 4 61 55
2 5 59 63
3 3 50 41
4 1 54 63

X = games_tourney_train[['seed_diff']]
y = games_tourney_train[['score_1', 'score_2']]
model.fit(X, y, epochs=500)

# Inspecting a two output model
model.get_weights()

[array([[ 0.60714734, -0.5988793 ]], dtype=float32),
 array([70.39491, 70.39306], dtype=float32)]

# Evaluating a model with two outputs
X = games_tourney_test[['seed_diff']]
y = games_tourney_test[['score_1', 'score_2']]
model.evaluate(X, y)

11.528035634635021

#####################################################################################
# Define the input
input_tensor = Input(shape=(2,))

# Define the output
output_tensor = Dense(2)(input_tensor)

# Create a model
model = Model(input_tensor, output_tensor)

# Compile the model
model.compile(optimizer='adam', loss='mean_absolute_error')

#####################################################################################
# Fit the model
model.fit(games_tourney_train[['seed_diff', 'pred']],
  		  games_tourney_train[['score_1', 'score_2']],
  		  verbose=True,
  		  epochs=100,
  		  batch_size=16384)

#####################################################################################
# Print the model's weights
print(model.weights)

# Print the column means of the training data
print(games_tourney_train.describe())
#####################################################################################
# Print the model's weights
print(model.get_weights())

# Print the column means of the training data
print(games_tourney_train.mean())
#####################################################################################
# Evaluate the model on the tournament test data
print(model.evaluate(games_tourney_test[[ 'seed_diff', 'pred' ]], 
                     games_tourney_test[[ 'score_1', 'score_2' ]], verbose=False))
#####################################################################################
# Build a simple regressor/classifier

from keras.layers import Input, Dense
input_tensor = Input(shape=(1,))
output_tensor_reg = Dense(1)(input_tensor)
output_tensor_class = Dense(1, activation='sigmoid')(output_tensor_reg)

# Make a regressor/classifier
from keras.models import Model
model = Model(input_tensor, [output_tensor_reg, output_tensor_class])
model.compile(loss=['mean_absolute_error', 'binary_crossentropy'],
optimizer='adam')

# Fit the combination classifier/regressor
X = games_tourney_train[['seed_diff']]
y_reg = games_tourney_train[['score_diff']]
y_class = games_tourney_train[['won']]
model.fit(X, [y_reg, y_class], epochs=100)

# Look at the model's weights
model.get_weights()
[array([[1.2371823]], dtype=float32),
 array([-0.05451894], dtype=float32),
 array([[0.13870609]], dtype=float32),
 array([0.00734114], dtype=float32)]

from scipy.special import expit as sigmoid
print(sigmoid(1 * 0.13870609 + 0.00734114))
0.5364470465211318

# Evaluate the model on new data
X = games_tourney_test[['seed_diff']]
y_reg = games_tourney_test[['score_diff']]
y_class = games_tourney_test[['won']]
model.evaluate(X, [y_reg, y_class])
[9.866300069455413, 9.281179495657208, 0.585120575627864]
#####################################################################################
# Create an input layer with 2 columns
input_tensor = Input(shape=(2,))

# Create the first output
output_tensor_1 = Dense(1, activation='linear', use_bias=False)(input_tensor)

# Create the second output (use the first output as input here)
output_tensor_2 = Dense(1, activation='sigmoid', use_bias=False)(output_tensor_1)

# Create a model with 2 outputs
model = Model(input_tensor, [output_tensor_1, output_tensor_2])
#####################################################################################
# Import the Adam optimizer
from keras.optimizers import Adam

# Compile the model with 2 losses and the Adam optimzer with a higher learning rate
model.compile(loss=['mean_absolute_error', 'binary_crossentropy'], optimizer=Adam(.01))

# Fit the model to the tournament training data, with 2 inputs and 2 outputs
model.fit(games_tourney_train[['seed_diff', 'pred']],
          [games_tourney_train[['score_diff']], games_tourney_train[['won']]],
          epochs=10,
          verbose=True,
          batch_size=16384)
#####################################################################################
# Print the model weights
print(model.get_weights())

# Print the training data means
print(games_tourney_train.mean())
#####################################################################################
# Import the sigmoid function from scipy
from scipy.special import expit as sigmoid

# Weight from the model
weight = 0.14

# Print the approximate win probability of a predicted close game
print(sigmoid(1 * weight))

# Print the approximate win probability of a predicted blowout game
print(sigmoid(10 * weight))
#####################################################################################
# Evaluate the model on new data
print(model.evaluate(games_tourney_test[['seed_diff', 'pred']],
               [games_tourney_test[['score_diff']], games_tourney_test[['won']]], verbose=False))
#####################################################################################
#####################################################################################

