#####################################################################################
# simple model with three inputs
from keras.layers import Input, Concatenate, Dense
in_tensor_1 = Input(shape=(1,))
in_tensor_2 = Input(shape=(1,))
in_tensor_3 = Input(shape=(1,))
out_tensor = Concatenate()([in_tensor_1, in_tensor_2, in_tensor_3])
output_tensor = Dense(1)(out_tensor)

from keras.models import Model
model = Model([in_tensor_1, in_tensor_2, in_tensor_3], out_tensor)

# shared layers with three inputs
shared_layer = Dense(1)
shared_tensor_1 = shared_layer(in_tensor_1)
shared_tensor_2 = shared_layer(in_tensor_1)
out_tensor = Concatenate()([shared_tensor_1, shared_tensor_2, in_tensor_3])
out_tensor = Dense(1)(out_tensor)

from keras.models import Model
model = Model([in_tensor_1, in_tensor_2, in_tensor_3], out_tensor)

# fitting a 3 input model
from keras.models import Model
model = Model([in_tensor_1, in_tensor_2, in_tensor_3], out_tensor)
model.compile(loss='mae', optimizer='adam')
model.fit([[train['col1'], train['col2'], train['col3']],
train_data['target'])
model.evaluate([[test['col1'], test['col2'], test['col3']],
test['target'])

#####################################################################################
# Create an Input for each team
team_in_1 = Input(shape=(1,), name='Team-1-In')
team_in_2 = Input(shape=(1,), name='Team-2-In')

# Create an input for home vs away
home_in = Input(shape=(1,), name='Home-In')

# Lookup the team inputs in the team strength model
team_1_strength = team_strength_model(team_in_1)
team_2_strength = team_strength_model(team_in_2)

# Combine the team strengths with the home input using a Concatenate layer, then add a Dense layer
out = Concatenate()([team_1_strength, team_2_strength, home_in])
out = Dense(1)(out)
#####################################################################################
# Import the model class
from keras.models import Model

# Make a Model
model = Model([team_in_1, team_in_2, home_in], out)

# Compile the model
model.compile(optimizer='adam', loss='mean_absolute_error')
#####################################################################################
# Fit the model to the games_season dataset
model.fit([games_season['team_1'], games_season['team_2'], games_season['home']],
          games_season['score_diff'],
          epochs=1,
          verbose=True,
          validation_split=0.10,
          batch_size=2058)

# Evaluate the model on the games_tourney dataset
print(model.evaluate([games_tourney['team_1'], games_tourney['team_2'], games_tourney['home']], games_tourney['score_diff'], verbose=False))

#####################################################################################
# Imports
import matplotlib.pyplot as plt
from keras.utils import plot_model

# Plot the model
plot_model(model, to_file='model.png')

# Display the image
data = plt.imread('model.png')
plt.imshow(data)
plt.show()

#####################################################################################
# stacking model requires two data sets
from pandas import read_csv

games_season = read_csv('datasets/games_season.csv')
games_season.head()

games_tourney = read_csv('datasets/games_tourney.csv')
games_tourney.head()

# Enrich the tournament data
in_data_1 = games_tourney['team_1']
in_data_2 = games_tourney['team_2']
in_data_3 = games_tourney['home']

pred = regular_season_model.predict([in_data_1, in_data_2, in_data_3])
games_tourney['pred'] = pred
games_tourney.head()

# three input model with pure numeric data
games_tourney[['home','seed_diff','pred']].head()

# three input model with pure numeric data
from keras.layers import Input, Dense
from keras.models import Model

in_tensor = Input(shape=(3,))
out_tensor = Dense(1)(in_tensor)

model = Model(in_tensor, out_tensor)
model.compile(optimizer='adam', loss='mae')

train_X = train_data[['home','seed_diff','pred']]
train_y = train_data['score_diff']

model.fit(train_X,train_y, epochs=10, validation_split=.10)
test_X = test_data[['home','seed_diff','pred']]
test_y = test_data['score_diff']

model.evaluate(test_X, test_y)

#####################################################################################
# Predict
in_data_1 = games_tourney['team_1']
in_data_2 = games_tourney['team_2']
in_data_3 = games_tourney['home']

pred = model.predict([in_data_1, in_data_2, in_data_3])
games_tourney['pred'] = pred

#####################################################################################
# Create an input layer with 3 columns
input_tensor = Input((2,))

# Pass it to a Dense layer with 1 unit
output_tensor = Dense(1)(input_tensor)

# Create a model
model = Model(input_tensor, output_tensor)

# Compile the model
model.compile(optimizer='adam', loss='mean_absolute_error')
#####################################################################################
# Create an input layer with 3 columns
input_tensor = Input((3,))

# Pass it to a Dense layer with 1 unit
output_tensor = Dense(1)(input_tensor)

# Create a model
model = Model(input_tensor, output_tensor)

# Compile the model
model.compile(optimizer='adam', loss='mean_absolute_error')
#####################################################################################
# Fit the model
model.fit(games_tourney_train[['home', 'seed_diff', 'pred']],
          games_tourney_train['score_diff'],
          epochs=1,
          verbose=True)
#####################################################################################
# Evaluate the model on the games_tourney_test dataset
print(model.evaluate(games_tourney_test[['home','seed_diff','prediction']],
               games_tourney_test['score_diff'], verbose=False))
#####################################################################################
