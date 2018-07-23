from etl.etl import db
from .base import CRUDMixin
from sqlalchemy import VARCHAR, Integer
from sqlalchemy.orm import relationship


class ExtDatasourceCon(CRUDMixin, db.Model):
    source_id = db.Column(VARCHAR(15))
    cmid = db.Column(Integer)
    roll_back = db.Column(Integer)
    frequency = db.Column(Integer)
    period = db.Column(Integer)

    datasource = relationship(
        "ExtDatasource",
        primaryjoin="foreign(ExtDatasourceCon.cmid) == remote(ExtDatasource.cmid)",
        uselist=False,
    )

    ext_tables = relationship(
        "ExtTableInfo",
        primaryjoin="foreign(ExtDatasourceCon.cmid) == remote(ExtTableInfo.cmid)",
    )
