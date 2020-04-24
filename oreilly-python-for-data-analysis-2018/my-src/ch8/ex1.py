# import pandas as pd

# left = pd.DataFrame({'key1': ['foo', 'foo', 'bar'],
# 'key2': ['one', 'two', 'one'],
# 'lval': [1, 2, 3]})

# right = pd.DataFrame({'key1': ['foo', 'foo', 'bar', 'bar'],
# 'key2': ['one', 'one', 'one', 'two'],
# 'rval': [4, 5, 6, 7]})

# left1 = pd.DataFrame({'key': ['a', 'b', 'a', 'a', 'b', 'c'], 'value': range(6)})
# right1 = pd.DataFrame({'group_val': [3.5, 7]}, index=['a', 'b'])

# lefth = pd.DataFrame({'key1': ['Ohio', 'Ohio', 'Ohio',
#                                'Nevada', 'Nevada'],
#                       'key2': [2000, 2001, 2002, 2001, 2002],
#                       'data': np.arange(5.)})
# 
# righth = pd.DataFrame(np.arange(12).reshape((6, 2)),
#                       index=[['Nevada', 'Nevada', 'Ohio', 'Ohio',
#                               'Ohio', 'Ohio'],
#                              [2001, 2000, 2000, 2000, 2001, 2002]],
#                       columns=['event1', 'event2'])
# 

# left2 = pd.DataFrame([[1., 2.], [3., 4.], [5., 6.]],
# index=['a', 'c', 'e'],
# columns=['Ohio', 'Nevada'])

# right2 = pd.DataFrame([[7., 8.], [9., 10.], [11., 12.], [13, 14]],
# index=['b', 'c', 'd', 'e'],
# columns=['Missouri', 'Alabama'])

# another = pd.DataFrame([[7., 8.], [9., 10.], [11., 12.], [16., 17.]],
# index=['a', 'c', 'e', 'f'],
# columns=['New York', 'Oregon'])

# s1 = pd.Series([0, 1], index=['a', 'b'])
# s2 = pd.Series([2, 3, 4], index=['c', 'd', 'e'])
# s3 = pd.Series([5, 6], index=['f', 'g'])

df1 = pd.DataFrame(np.arange(6).reshape(3, 2), index=['a', 'b', 'c'],
columns=['one', 'two'])
df2 = pd.DataFrame(5 + np.arange(4).reshape(2, 2), index=['a', 'c'],
columns=['three', 'four'])

data = pd.DataFrame(np.arange(6).reshape((2, 3)),
index=pd.Index(['Ohio', 'Colorado'], name='state'),
columns=pd.Index(['one', 'two', 'three'],
name='number'))

