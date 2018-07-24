# -*- coding: utf-8 -*-
from fabric import task, Connection

ENV = {"dev": "172.31.16.17"}


@task
def deploy(c, env="dev"):
    """Deploy in <env>ironment by <user>.
    """

    with Connection(host=ENV[env], user="centos") as c:
        with c.cd("/data/code/etl-engine/source"):
            c.run("git checkout -- .")
            c.run("git pull -r")
        c.run("supervisorctl -c /data/code/supervisord.conf restart etl-engine")


@task
def db_migrate(c, env="dev"):
    """Migrate database.
    """
    with Connection(host=ENV[env], user="centos") as c:
        with c.cd("/data/code/etl-engine"):
            c.run(
                f"ETL_ENVIREMENT={env} ./source/venv/bin/python source/manage.py db migrate"
            )
            c.run(
                f"ETL_ENVIREMENT={env} ./source/venv/bin/python source/manage.py db upgrade"
            )
