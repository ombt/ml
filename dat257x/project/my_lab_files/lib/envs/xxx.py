#!/usr/bin/python3

import numpy as np

shape = (4, 12)

def limit_coordinates(coord):
    coord[0] = min(coord[0], shape[0] - 1)
    coord[0] = max(coord[0], 0)
    coord[1] = min(coord[1], shape[1] - 1)
    coord[1] = max(coord[1], 0)
    return coord

def calculate_transition_prob(current, delta):
    new_position = np.array(current) + np.array(delta)
    new_position = limit_coordinates(new_position).astype(int)
    new_state = np.ravel_multi_index(tuple(new_position), shape)
    reward = -100.0 if cliff[tuple(new_position)] else -1.0
    is_done = cliff[tuple(new_position)] or (tuple(new_position) == (3,11))
    return [(1.0, new_state, reward, is_done)]

def convert_state(state):
    converted = np.unravel_index(state, shape)
    return np.asarray(list(converted), dtype=np.float32)
    
def step(action):
    reward = P[s][action][0][2]
    done = P[s][action][0][3]
    info = {'prob':P[self.s][action][0][0]}
    self.s = P[s][action][0][1]
    return (convert_state(s), reward, done, info)
    

nS = np.prod(shape)
nA = 4

# Cliff Location
cliff = np.zeros(shape, dtype=np.bool)
cliff[3, 1:-1] = True

# Calculate transition probabilities
P = {}
for s in range(nS):
    position = np.unravel_index(s, shape)
    P[s] = { a : [] for a in range(nA) }
    #UP = 0
    #RIGHT = 1
    #DOWN = 2
    #LEFT = 3
    P[s][0] = calculate_transition_prob(position, [-1, 0])
    P[s][1] = calculate_transition_prob(position, [0, 1])
    P[s][2] = calculate_transition_prob(position, [1, 0])
    P[s][3] = calculate_transition_prob(position, [0, -1])

for s in range(nS):
    print(s,'=',np.unravel_index(s, shape),',', 'P[',s,']=',P[s])

# We always start in state (3, 0)
isd = np.zeros(nS)
isd[np.ravel_multi_index((3,0), shape)] = 1.0

