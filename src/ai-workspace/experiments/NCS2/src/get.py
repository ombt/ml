from RedisQueue import RedisQueue
q = RedisQueue('test')
result = q.get()

print(result)
