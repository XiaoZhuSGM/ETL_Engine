# -*- coding: utf-8 -*-
from fabric import task, Connection

***REMOVED***

CODE_HOME = "/data/app/etl-engine"


@task
def deploy(c, env="dev", branch="dev"):
    """Deploy <branch> with <env>.
    """
    with Connection(host=ENV[env], user="centos") as c:
        with c.cd(CODE_HOME):
            # 1. 丢弃远端的修改
            c.run("git checkout -- .")
            # 2. 切换到正确的分支并拉取代码
            c.run(f"git checkout {branch}")
            c.run("git pull -r")
            c.run(
                "sudo docker-compose up -d --scale gunicorn=2 --remove-orphans --build"
            )


@task
def manage(c, env="dev", command=""):
    """Use manage command in local.
    """
    with Connection(host=ENV[env], user="centos") as c:
        with c.cd(CODE_HOME):
            c.run(
                f"ETL_ENVIREMENT={env} ./source/venv/bin/python source/manage.py {command}",
                pty=True,
            )
