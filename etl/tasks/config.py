from huey import RedisHuey


huey = RedisHuey("tasks", events=True, host="localhost", port=6379, db=0)
