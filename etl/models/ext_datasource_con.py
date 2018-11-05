from sqlalchemy import VARCHAR, Integer
from sqlalchemy.orm import relationship

from etl.etl import db
from .base import CRUDMixin


class ExtDatasourceCon(CRUDMixin, db.Model):
    source_id = db.Column(VARCHAR(15), unique=True)
    roll_back = db.Column(Integer)
    frequency = db.Column(Integer)
    period = db.Column(Integer)
    ext_time = db.Column(VARCHAR(10))

    datasource = relationship(
        "ExtDatasource",
        primaryjoin="remote(ExtDatasourceCon.source_id) == foreign(ExtDatasource.source_id)",
        uselist=False,
    )

    ext_tables = relationship(
        "ExtTableInfo",
        primaryjoin="foreign(ExtDatasourceCon.source_id) == remote(ExtTableInfo.source_id)",
    )

