from etl.etl import create_app, celery
from config.config import config
import os

envirement = os.environ.get('ETL_ENVIREMENT', 'dev')

app = create_app(config.get(envirement, config["dev"]))
print(celery.Task)