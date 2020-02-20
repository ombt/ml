
# coding: utf-8

# # DAT257x: Reinforcement Learning Explained
# 
# ## Lab 2: Bandits
# 
# ### Exercise 2.2B: Optimistic Greedy

# In[114]:


import numpy as np
import sys

if "../" not in sys.path:
    sys.path.append("../") 

from lib.envs.bandit import BanditEnv
from lib.simulation import Experiment


# In[115]:


#Policy interface
class Policy:
    #num_actions: (int) Number of arms [indexed by 0 ... num_actions-1]
    def __init__(self, num_actions):
        self.num_actions = num_actions
    
    def act(self):
        pass
        
    def feedback(self, action, reward):
        pass


# In[116]:


#Greedy policy
class Greedy(Policy):
    def __init__(self, num_actions):
        Policy.__init__(self, num_actions)
        self.name = "Greedy"
        self.total_rewards = np.zeros(num_actions, dtype = np.longdouble)
        self.total_counts = np.zeros(num_actions, dtype = np.longdouble)
    
    def act(self):
        current_averages = np.divide(self.total_rewards, self.total_counts, where = self.total_counts > 0)
        current_averages[self.total_counts <= 0] = 0.5      #Correctly handles Bernoulli rewards; over-estimates otherwise
        current_action = np.argmax(current_averages)
        return current_action
        
    def feedback(self, action, reward):
        self.total_rewards[action] += reward
        self.total_counts[action] += 1


# Now let's implement an optimistic greedy policy based on the policy interface. The optimistic greedy policy initialize the ra to a large initial value R, which is implemented in the __init__() function, and then play the greedy algorithm.

# We have given you some boiler plate code, you only need to modify the part as indicated.

# In[117]:


#Optimistic Greedy policy
class OptimisticGreedy(Greedy):
    def __init__(self, num_actions, initial_value):
        Greedy.__init__(self, num_actions)
        self.name = "Optimistic Greedy"
        
        """Implement optimistic greedy here"""
        self.total_rewards = np.full(num_actions, initial_value, dtype = np.longdouble)
        self.total_counts = np.full(num_actions, 1, dtype = np.longdouble)
        
        


# Let's prepare the simulation. We'll use the same parameters as the exercise with the epsilon greedy.

# In[118]:


evaluation_seed = 5016
num_actions = 10
trials = 10000
distribution = "bernoulli"


# First, let's use R = 0. Run the simulation and observe the results.

# In[119]:


R = 1
env = BanditEnv(num_actions, distribution, evaluation_seed)
agent = OptimisticGreedy(num_actions, R)
experiment = Experiment(env, agent)
experiment.run_bandit(trials)


# What about if R is a very large number? Say 10000, the same number as the number of trials? Run the simulation again and observe the results.

# Now, try several different number of R (1, 3, 5). Run the simulations and observe the results.

# Which epsilon performs best with this problem?

# Now let's prepare another simulation by setting a different seed. 

# In[120]:


evaluation_seed = 1239
num_actions = 10
trials = 10000
distribution = "bernoulli"


# Try the range of R again (0, 1, 3, 5, 10000), run the simulations and observe the results.
