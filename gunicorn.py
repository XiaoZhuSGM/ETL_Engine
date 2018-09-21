import os
env = os.getenv("ETL_ENVIREMENT") or "dev"

capture_output = True
reuse_port = True
raw_env = [f"ETL_ENVIREMENT={env}"]
bind = "0.0.0.0:5000"
workers = 2
worker_class = "gthread"
threads = 8
proc_name = f"etl-engine-{env}"
timeout = 60
accesslog = "-"
errorlog = "-"
