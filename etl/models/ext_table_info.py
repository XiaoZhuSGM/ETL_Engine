from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from etl.etl import db
from .base import CRUDMixin


class ExtTableInfo(db.Model, CRUDMixin):
    __tablename__ = "ext_table_info"
    cmid = Column(Integer)
    table_name = Column(String(100))
    ext_pri_key = Column(String(200))
    order_column = Column(String(200))
    sync_column = Column(String(200))
    limit_num = Column(Integer)
    filter = Column(String(500))
    filter_format = Column(String(50))
    record_num = Column(Integer)
    weight = Column(Integer)
    ext_column = Column(JSONB)

    datasource = relationship(
        "ExtDatasource",
        primaryjoin="foreign(ExtTableInfo.cmid) == remote(ExtDatasource.cmid)",
        uselist=False,
        back_populates="ext_tables",
    )
