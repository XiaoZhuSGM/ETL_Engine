from etl.etl import db
from .base import CRUDMixin
from sqlalchemy import VARCHAR, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB


class ExtDatasourceCon(CRUDMixin, db.Model):
    source_id = db.Column(VARCHAR(15))
    cmid = db.Column(JSONB)
    roll_back = db.Column(Integer)
    frequency = db.Column(Integer)
    period = db.Column(Integer)

    datasource = relationship(
        "ExtDatasource",
        primaryjoin="foreign(ExtDatasourceCon.source_id) == remote(ExtDatasource.source_id)",
        uselist=False,
    )

    ext_tables = relationship(
        "ExtTableInfo",
        primaryjoin="foreign(ExtDatasourceCon.source_id) == remote(ExtTableInfo.source_id)",
    )
