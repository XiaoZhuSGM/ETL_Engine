# -*- coding: utf-8 -*-
from fabric import task


@task
def deploy(c):
        with c.cd("/data/code/etl-engine/source"):
            c.run("git checkout -- .")
            c.run("git pull -r")
        c.run("supervisorctl -c /data/code/supervisord.conf restart etl-engine")
