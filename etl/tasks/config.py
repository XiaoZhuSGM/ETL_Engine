from huey import RedisHuey
from config.config import config
import os

setting = config[os.getenv("ETL_ENVIREMENT", "dev")]

huey = RedisHuey(
    "tasks",
    host=setting.REDIS_HOST,
    port=setting.REDIS_PORT,
    db=setting.REDIS_DB,
    password=setting.REDIS_PASSWORD,
)
