################################################################################
Data import

In [1]: import pandas as pd
In [2]: import matplotlib.pyplot as plt
In [3]: iris = pd.read_csv('iris.csv', index_col=0)
In [4]: print(iris.shape)
(150, 5)

Line plot

In [5]: iris.head()
Out[5]:
sepal_length sepal_width petal_length petal_width species
0 5.1 3.5 1.4 0.2 setosa
1 4.9 3.0 1.4 0.2 setosa
2 4.7 3.2 1.3 0.2 setosa
3 4.6 3.1 1.5 0.2 setosa
4 5.0 3.6 1.4 0.2 setosa
In [6]: iris.plot(x='sepal_length', y='sepal_width')
In [7]: plt.show()

Sca!er plot

In [8]: iris.plot(x='sepal_length', y='sepal_width',
...: kind='scatter')
In [9]: plt.xlabel('sepal length (cm)')
In [10]: plt.ylabel('sepal width (cm)')
In [11]: plt.show()

Box plot

In [12]: iris.plot(y='sepal_length', kind='box')
In [13]: plt.ylabel('sepal width (cm)')
In [14]: plt.show()

Histogram

In [15]: iris.plot(y='sepal_length', kind='hist')
In [16]: plt.xlabel('sepal length (cm)')
In [17]: plt.show()

Histogram options
● bins (integer): number of intervals or bins
● range (tuple): extrema of bins (minimum, maximum)
● normed (boolean): whether to normalize to one
● cumulative (boolean): compute Cumulative Distribution
Function (CDF)
● … more Matplotlib customizations

Customizing histogram

In [18]: iris.plot(y='sepal_length', kind='hist',
...: bins=30, range=(4,8), normed=True)
In [19]: plt.xlabel('sepal length (cm)')
In [20]: plt.show()

Cumulative distribution

In [21]: iris.plot(y='sepal_length', kind='hist', bins=30,
...: range=(4,8), cumulative=True, normed=True)
In [22]: plt.xlabel('sepal length (cm)')
In [23]: plt.title('Cumulative distribution function (CDF)')
In [24]: plt.show()

Word of warning
● Three different DataFrame plot idioms
● iris.plot(kind=‘hist’)
● iris.plt.hist()
● iris.hist()
● Syntax/results differ!
● Pandas API still evolving: check documentation!

################################################################################
# Create a list of y-axis column names: y_columns
y_columns = [ 'AAPL', 'IBM']

# Generate a line plot
df.plot(x='Month', y=y_columns)

# Add the title
plt.title('Monthly stock prices')

# Add the y-axis label
plt.ylabel('Price ($US)')

# Display the plot
plt.show()
################################################################################
# Generate a scatter plot
df.plot(kind='scatter', x='hp', y='mpg', s=sizes)

# Add the title
plt.title('Fuel efficiency vs Horse-power')

# Add the x-axis label
plt.xlabel('Horse-power')

# Add the y-axis label
plt.ylabel('Fuel efficiency (mpg)')

# Display the plot
plt.show()
################################################################################
# Make a list of the column names to be plotted: cols
cols = ['weight','mpg']

# Generate the box plots
df[cols].plot(subplots=True, kind='box')

# Display the plot
plt.show()
################################################################################
# This formats the plots such that they appear on separate rows
fig, axes = plt.subplots(nrows=2, ncols=1)

# Plot the PDF
df.fraction.plot(ax=axes[0], kind='hist', normed=True, bins=30, range=(0,.3))
plt.show()

# Plot the CDF
df.fraction.plot(ax=axes[1], kind='hist', normed=True, cumulative=True, bins=30, range=(0,.3))
plt.show()
################################################################################
Summarizing with describe()

In [1]: iris.describe() # summary statistics
Out[1]:
sepal_length sepal_width petal_length petal_width
count 150.000000 150.000000 150.000000 150.000000
mean 5.843333 3.057333 3.758000 1.199333
std 0.828066 0.435866 1.765298 0.762238
min 4.300000 2.000000 1.000000 0.100000
25% 5.100000 2.800000 1.600000 0.300000
50% 5.800000 3.000000 4.350000 1.300000
75% 6.400000 3.300000 5.100000 1.800000
max 7.900000 4.400000 6.900000 2.500000

Describe
● count: number of entries
● mean: average of entries
● std: standard deviation
● min: minimum entry
● 25%: first quartile
● 50%: median or second quartile
● 75%: third quartile
● max: maximum entry

Counts

In [2]: iris['sepal_length'].count() # Applied to Series
Out[2]: 150
In [3]: iris['sepal_width'].count() # Applied to Series
Out[3]: 150
In [4]: iris[['petal_length', 'petal_width']].count() # Applied
...: to DataFrame
Out[4]:
petal_length 150
petal_width 150
dtype: int64
In [5]: type(iris[['petal_length', 'petal_width']].count()) #
...: returns Series
Out[5]: pandas.core.series.Series

Averages

In [6]: iris['sepal_length'].mean() # Applied to Series
Out[6]: 5.843333333333335
In [7]: iris.mean() # Applied to entire DataFrame
Out[7]:
sepal_length 5.843333
sepal_width 3.057333
petal_length 3.758000
petal_width 1.199333
dtype: float64

Standard deviations

In [8]: iris.std()
Out[8]:
sepal_length 0.828066
sepal_width 0.435866
petal_length 1.765298
petal_width 0.762238
dtype: float64

Medians

In [9]: iris.median()
Out[9]:
sepal_length 5.80
sepal_width 3.00
petal_length 4.35
petal_width 1.30
dtype: float64

Medians & 0.5 quantiles

In [10]: iris.median()
Out[10]:
sepal_length 5.80
sepal_width 3.00
petal_length 4.35
petal_width 1.30
dtype: float64

In [11]: q = 0.5
In [12]: iris.quantile(q)
Out[12]:
sepal_length 5.80
sepal_width 3.00
petal_length 4.35
petal_width 1.30
dtype: float64

Inter-quartile range (IQR)

In [13]: q = [0.25, 0.75]
In [14]: iris.quantile(q)
Out[14]:
sepal_length sepal_width petal_length petal_width
0.25 5.1 2.8 1.6 0.3
0.75 6.4 3.3 5.1 1.8

Ranges

In [15]: iris.min()
Out[15]:
sepal_length 4.3
sepal_width 2
petal_length 1
petal_width 0.1
species setosa
dtype: object

In [16]: iris.max()
Out[16]:
sepal_length 7.9
sepal_width 4.4
petal_length 6.9
petal_width 2.5
species virginica
dtype: object

Box plots

In [17]: iris.plot(kind= 'box')
Out[17]: <matplotlib.axes._subplots.AxesSubplot at 0x118a3d5f8>

In [18]: plt.ylabel('[cm]')
Out[18]: <matplotlib.text.Text at 0x118a524e0>

In [19]: plt.show()
`
Percentiles as quantiles

In [20]: iris.describe() # summary statistics
Out[20]:
sepal_length sepal_width petal_length petal_width
count 150.000000 150.000000 150.000000 150.000000
mean 5.843333 3.057333 3.758000 1.199333
std 0.828066 0.435866 1.765298 0.762238
min 4.300000 2.000000 1.000000 0.100000
25% 5.100000 2.800000 1.600000 0.300000
50% 5.800000 3.000000 4.350000 1.300000
75% 6.400000 3.300000 5.100000 1.800000
max 7.900000 4.400000 6.900000 2.500000

################################################################################
# Print the minimum value of the Engineering column
print(df['Engineering'].min())

# Print the maximum value of the Engineering column
print(df['Engineering'].max())

# Construct the mean percentage per year: mean
mean = df.mean(axis='columns')

# Plot the average percentage per year
mean.plot()

# Display the plot
plt.show()
################################################################################
# Print summary statistics of the fare column with .describe()
print(df.fare.describe())

# Generate a box plot of the fare column
df.fare.plot(kind='box')

# Show the plot
plt.show()
################################################################################
# Print the number of countries reported in 2015
print(df['2015'].count())

# Print the 5th and 95th percentiles
print(df.quantile([0.05,0.95]))

# Generate a box plot
years = ['1800','1850','1900','1950','2000']
df[years].plot(kind='box')
plt.show()
################################################################################
# Print the mean of the January and March data
print(january.mean(), march.mean())

# Print the standard deviation of the January and March data
print(january.std(), march.std())
################################################################################
# Compute the global mean and global standard deviation: global_mean, global_std
global_mean = df.mean()
global_std = df.std()

# Filter the US population from the origin column: us
us = df[df["origin"]=="us"]

# Compute the US mean and US standard deviation: us_mean, us_std
us_mean = us.mean()
us_std = us.std()

# Print the differences
print(us_mean - global_mean)
print(us_std - global_std)
################################################################################
# Compute the global mean and global standard deviation: global_mean, global_std
global_mean = df.mean()
global_std = df.std()

# Filter the US population from the origin column: us
us = df.loc[df['origin']=='US']

# Compute the US mean and US standard deviation: us_mean, us_std
us_mean = us.mean()
us_std = us.std()

# Print the differences
print(us_mean - global_mean)
print(us_std - global_std)
################################################################################
# Display the box plots on 3 separate rows and 1 column
fig, axes = plt.subplots(nrows=3, ncols=1)

# Generate a box plot of the fare prices for the First passenger class
titanic.loc[titanic['pclass'] == 1].plot(ax=axes[0], y='fare', kind='box')

# Generate a box plot of the fare prices for the Second passenger class
titanic.loc[titanic['pclass'] == 2].plot(ax=axes[1], y='fare', kind='box')

# Generate a box plot of the fare prices for the Third passenger class
titanic.loc[titanic['pclass']== 2].plot(ax=axes[2], y='fare', kind='box')

# Display the plot
plt.show()
################################################################################
