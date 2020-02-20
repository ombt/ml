
# coding: utf-8

# # DAT257x: Reinforcement Learning Explained
# 
# ## Lab 2: Bandits
# 
# ### Exercise 2.4 Thompson Beta

# In[11]:


import numpy as np
import sys

if "../" not in sys.path:
    sys.path.append("../") 

from lib.envs.bandit import BanditEnv
from lib.simulation import Experiment


# In[12]:


#Policy interface
class Policy:
    #num_actions: (int) Number of arms [indexed by 0 ... num_actions-1]
    def __init__(self, num_actions):
        self.num_actions = num_actions
    
    def act(self):
        pass
        
    def feedback(self, action, reward):
        pass


# Now let's implement a Thompson Beta algorithm. 
# 
# 

# In[13]:


#Tompson Beta policy
class ThompsonBeta(Policy):
    def __init__(self, num_actions):
        Policy.__init__(self, num_actions)
        #PRIOR Hyper-params: successes = 1; failures = 1
        self.total_counts = np.zeros(num_actions, dtype = np.longdouble)
        self.name = "Thompson Beta"
        
        #For each arm, maintain success and failures
        self.successes = np.ones(num_actions, dtype = np.int)
        self.failures = np.ones(num_actions, dtype = np.int)
        
    def act(self):
        """Sample beta distribution from success and failures"""
        pa = np.random.beta(1+self.successes, 1+self.failures)
        
        """Play the max of the sampled values"""
        current_action = np.argmax(pa)
        
        return current_action
    
    def feedback(self, action, reward):
        if reward > 0:
            self.successes[action] += 1
        else:
            self.failures[action] += 1
        self.total_counts[action] += 1


# Now let's prepare the simulation. 

# In[14]:


evaluation_seed = 1239
num_actions = 10
trials = 10000
distribution = "normal"


# What do you think the regret graph would look like?

# In[15]:


env = BanditEnv(num_actions, distribution, evaluation_seed)
agent = ThompsonBeta(num_actions)
experiment = Experiment(env, agent)
experiment.run_bandit(trials)


# Now let's prepare another simulation by setting a different distribution, that is set distribion = "normal"

# Run the simulation and observe the results.

# What do you think the regret graph would look like?
