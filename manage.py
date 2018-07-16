# -*- coding: utf-8 -*-

"""
exam app runserver
"""
import os

from flask_migrate import MigrateCommand
from flask_script import Manager, Server

from config.config import config
from etl import create_app

envirement = os.environ.get('ETL_ENVIREMENT', 'dev')

app = create_app(config.get(envirement, config["dev"]))
manager = Manager(app)
manager.add_command("runserver",
                    Server(host="0.0.0.0", use_reloader=True if envirement in ("dev", "local") else False))
manager.add_command('db', MigrateCommand)

if __name__ == '__main__':
    manager.run()
