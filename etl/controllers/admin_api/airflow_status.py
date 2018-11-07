from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, func
from flask import request
from config.config import config
from etl.etl import db
from . import etl_admin_api
from .. import jsonify_with_data, APIError
from datetime import datetime, timedelta
import os


@etl_admin_api.route("/ext/airflow/tasks/status", methods=["GET"])
def get_airflow_tasks_status():
    Base = automap_base()
    engine = create_engine(config[os.getenv("ETL_ENVIREMENT", "dev")].AIRFLOW_DB_URL)
    Base.prepare(engine, reflect=True)
    Session = db.create_scoped_session(options={"bind": engine})
    session = Session()
    TaskInstance = Base.classes.task_instance
    start_date = request.args.get("start_date", "1970-01-01")
    end_date = request.args.get(
        "end_date", (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    )
    cmid = request.args.get("cmid")
    dag = f"ext_{cmid}" if cmid else None
    task_type = request.args.get("task_type")
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    query = (
        session.query(TaskInstance)
        .with_entities(TaskInstance.state, func.count(TaskInstance.state))
        .group_by(TaskInstance.state)
        .filter(
            TaskInstance.execution_date >= start_date,
            TaskInstance.execution_date < end_date,
        )
    )
    if dag:
        query = query.filter_by(dag_id=dag)
    if task_type:
        query = query.filter(TaskInstance.task_id.like(f"{task_type}%"))
    state = dict(query.all())
    return jsonify_with_data(
        APIError.OK,
        data={"success": state.get("success", 0), "failed": state.get("failed", 0)},
    )
