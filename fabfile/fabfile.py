# -*- coding: utf-8 -*-
from fabric import task, Connection

ENV = {"dev": "172.31.16.17"}


@task
def deploy(c, env="dev", branch="dev"):
    """Deploy <branch> with <env>.
    """

    with Connection(host=ENV[env], user="centos") as c:
        with c.cd("/data/code/etl-engine/source"):
            # 1. 丢弃远端的修改
            c.run("git checkout -- .")
            # 2. 切换到正确的分支并拉取代码
            c.run(f"git checkout {branch}")
            c.run("git pull -r")
            # 3. 更新包
            c.run(
                "venv/bin/pip install -r requirements.txt -i https://pypi.doubanio.com/simple/"
            )
        with c.cd("/data/code/etl-engine"):
            c.run("kill -HUP $(cat gunicorn.pid)")


@task
def manage(c, env="dev", command=""):
    """Use manage command in local.
    """
    with Connection(host=ENV[env], user="centos") as c:
        with c.cd("/data/code/etl-engine"):
            c.run(
                f"ETL_ENVIREMENT={env} ./source/venv/bin/python source/manage.py {command}",
                pty=True,
            )
