
# coding: utf-8

# # DAT257x: Reinforcement Learning Explained
# 
# ## Lab 4: Dynamic Programming
# 
# ### Exercise 4.4 Value Iteration

# Value Iteration calculates the optimal policy for an MDP, given its full definition.  The full definition of an MDP is the set of states, the set of available actions for each state, the set of rewards, the discount factor, and the state/reward transition function.

# In[ ]:


import test_dp               # required for testing and grading your code
import gridworld_mdp as gw   # defines the MDP for a 4x4 gridworld


# **Implement the algorithm for Value Iteration**.  Value Iteration calculates the optimal policy for an MDP by iteration of a single step combining both policy evaluation and policy improvement.
# 
# A empty function **value_iteration** is provided below; implement the body of the function to correctly calculate the optimal policy for an MDP.  The function defines 5 parameters - a definition of each parameter is given in the comment block for the function.  For sample parameter values, see the calling code in the cell following the function.

# In[ ]:


def value_iteration(state_count, gamma, theta, get_available_actions, get_transitions):
    """
    This function computes the optimal value function and policy for the specified MDP, using the Value Iteration algorithm.
    
    'state_count' is the total number of states in the MDP. States are represented as 0-relative numbers.
    
    'gamma' is the MDP discount factor for rewards.
    
    'theta' is the small number threshold to signal convergence of the value function (see Iterative Policy Evaluation algorithm).
    
    'get_available_actions' returns a list of the MDP available actions for the specified state parameter.
    
    'get_transitions' is the MDP state / reward transiton function.  It accepts two parameters, state and action, and returns
        a list of tuples, where each tuple is of the form: (next_state, reward, probabiliity).  
    """
    V = state_count*[0]                # init all state value estimates to 0
    pi = state_count*[0]
    
    # init with a policy with first avail action for each state
    for s in range(state_count):
        avail_actions = get_available_actions(s)
        pi[s] = avail_actions[0]
        
    # insert code here to iterate using policy evaluation and policy improvement (see Policy Iteration algorithm)
    return (V, pi)        # return both the final value function and the final policy


# First, test our function using the MDP defined by gw.* functions.

# In[ ]:


n_states = gw.get_state_count()

# test our function
values, policy = value_iteration(state_count=n_states, gamma=.9, theta=.001, get_available_actions=gw.get_available_actions,     get_transitions=gw.get_transitions)

print("Values=", values)
print("Policy=", policy)


# **Expected output from running above cell:**
# 
# `
# Values= [0.0, -1.0, -1.9, -2.71, -1.0, -1.9, -2.71, -1.9, -1.9, -2.71, -1.9, -1.0, -2.71, -1.9, -1.0]
# Policy= ['up', 'left', 'left', 'down', 'up', 'up', 'up', 'down', 'up', 'up', 'down', 'down', 'up', 'right', 'right']
# `

# Now, test our function using the test_dp helper.  The helper also uses the gw MDP, but with a different gamma value.
# If our function passes all tests, a passcode will be printed.

# In[ ]:


# test our function using the test_db helper
test_dp.value_iteration_test( value_iteration ) 

