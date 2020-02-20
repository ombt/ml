
# coding: utf-8

# # DAT257x: Reinforcement Learning Explained
# 
# ## Lab 5: Temporal Difference Learning
# 
# ### Exercise 5.2: SARSA Agent

# In[ ]:


import numpy as np
import sys

if "../" not in sys.path:
    sys.path.append("../") 
    
from lib.envs.simple_rooms import SimpleRoomsEnv
from lib.envs.windy_gridworld import WindyGridworldEnv
from lib.envs.cliff_walking import CliffWalkingEnv
from lib.simulation import Experiment


# In[ ]:

class QActionValue:
    def __init__(self):
        self.data = dict()

    def insert(self, data, key1, key2=None):
        if (key2 == None):
            self.data[key1] = dict(data)
        else:
            self.data[key1][key2] = data
        return True

    def remove(self, key1, key2=None):
        if (key1 in self.data):
            if (key2 == None):
                try:
                    del self.data[key1]
                except:
                    pass
            else:
                try:
                    del self.data[key1][key2]
                except:
                    pass
        return True

    def read(self, key1, key2=None):
        if (key1 in self.data):
            if (key2 == None):
                return self.data[key1]
            elif (key2 in self.data[key1]):
                return self.data[key1][key2]
            else:
                return 0.0
        else:
            return 0.0

    def update(self, data, key1, key2=None):
        if (key2 == None):
            self.data[key1] = dict(data)
        else:
            self.data[key1][key2] = data
        return True

    def keys(self, key1=None):
        if (key1 == None):
            return self.data.keys()
        elif (key1 in self.data):
            return self.data[key1].keys()
        else:
            return dict()

class Agent(object):  
        
    def __init__(self, actions):
        self.actions = actions
        self.num_actions = len(actions)

    def act(self, state):
        raise NotImplementedError


# In[ ]:


class SarsaAgent(Agent):
    
    def __init__(self, actions, epsilon=0.01, alpha=0.5, gamma=1):
        super(SarsaAgent, self).__init__(actions)
        
        ## TODO 1
        ## Initialize empty dictionary here
        ## In addition, initialize the value of epsilon, alpha and gamma

        if (epsilon is None) or (epsilon < 0) or (epsilon > 1):
            print("Invalid epsilon:", epsilon)
            sys.exit(2)

        self.Q = QActionValue()

        self.epsilon = epsilon
        self.alpha = alpha
        self.gamma = gamma
        
    def stateToString(self, state):
        print("stateToString(state)", state)
        mystring = ""
        if np.isscalar(state):
            mystring = str(state)
        else:
            for digit in state:
                mystring += str(digit)
        return mystring    
    
    def act(self, state):
        stateStr = self.stateToString(state)      
        action = np.random.randint(0, self.num_actions) 
        
        ## TODO 2
        ## Implement epsilon greedy policy here

        # if (np.random.binomial(1, self.epsilon) == 1):
            # return action
        # else:
        return action

    def learn(self, state1, action1, reward, state2, action2):
        state1Str = self.stateToString(state1)
        state2Str = self.stateToString(state2)
        
        ## TODO 3
        ## Implement the sarsa update here
        
        """
        SARSA Update
        Q(s,a) <- Q(s,a) + alpha * (reward + gamma * Q(s',a') - Q(s,a))
        or
        Q(s,a) <- Q(s,a) + alpha * (td_target - Q(s,a))
        or
        Q(s,a) <- Q(s,a) + alpha * td_delta
        """

        # Q(s,a) <- Q(s,a) + alpha * (reward + gamma * Q(s',a') - Q(s,a))

        Qsa = self.Q.read(state1Str, action1)
        Qspap = self.Q.read(state2Str, action2)
        Qsa = Qsa + self.alpha * (reward + self.gamma * Qspap - Qsa)
        print("state1Str,action1:", state1Str, action1)
        self.Q.update(Qsa, state1Str, action1)

# In[ ]:


interactive = True
# get_ipython().magic('matplotlib nbagg')
env = SimpleRoomsEnv()
agent = SarsaAgent(range(env.action_space.n))
experiment = Experiment(env, agent)
experiment.run_sarsa(10, interactive)


# In[ ]:


interactive = False
# get_ipython().magic('matplotlib inline')
env = SimpleRoomsEnv()
agent = SarsaAgent(range(env.action_space.n))
experiment = Experiment(env, agent)
experiment.run_sarsa(50, interactive)


# In[ ]:


interactive = True
# get_ipython().magic('matplotlib nbagg')
env = CliffWalkingEnv()
agent = SarsaAgent(range(env.action_space.n))
experiment = Experiment(env, agent)
experiment.run_sarsa(10, interactive)


# In[ ]:


interactive = False
# get_ipython().magic('matplotlib inline')
env = CliffWalkingEnv()
agent = SarsaAgent(range(env.action_space.n))
experiment = Experiment(env, agent)
experiment.run_sarsa(100, interactive)


# In[ ]:


interactive = False
# get_ipython().magic('matplotlib inline')
env = WindyGridworldEnv()
agent = SarsaAgent(range(env.action_space.n))
experiment = Experiment(env, agent)
experiment.run_sarsa(50, interactive)

