import gym

env = gym.make('CartPole-v0')

env.reset()

for i in range(1000):
	env.render()
	env.step(env.action_space.sample())

exit(0)
