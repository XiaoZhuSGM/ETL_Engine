# -*- coding: utf-8 -*-

"""
exam app runserver
"""
import os
from flask_migrate import MigrateCommand, Migrate
from etl import create_app, db
from config.config import config
from flask_script import Manager, Server

envirement = os.environ.get('ETL_ENVIREMENT', 'local')

app = create_app(config.get(envirement, config["local"]))
migrate = Migrate(app, db)


manager = Manager(app)
manager.add_command("runserver",
                    Server(host="0.0.0.0", use_reloader=True if envirement in ("dev", "testing") else False))
manager.add_command('db', MigrateCommand)

if __name__ == '__main__':
    manager.run()
