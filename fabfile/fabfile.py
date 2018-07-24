# -*- coding: utf-8 -*-
from fabric import task, Connection

ENV = {"dev": "172.31.16.17"}


@task
def deploy(c, user, env="dev"):
    """Deploy in <env>ironment by <user>.
    """

    with Connection(host=ENV[env], user=user) as c:
        with c.cd("/data/code/etl-engine/source"):
            c.run("git checkout -- .")
            c.run("git pull -r")
        c.run("supervisorctl -c /data/code/supervisord.conf restart etl-engine")
