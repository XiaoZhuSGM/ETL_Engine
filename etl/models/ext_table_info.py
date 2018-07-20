from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Column, Integer, String
from etl.etl import db
from .base import CRUDMixin
from enum import IntEnum


class ExtTableInfo(db.Model, CRUDMixin):
    __tablename__ = "ext_table_info"
    cmid = Column(Integer)
    table_name = Column(String(50))
    ext_pri_key = Column(String(50))
    order_column = Column(String(100))
    sync_column = Column(String(100))
    limit_num = Column(Integer)
    filter = Column(String(500))
    filter_format = Column(String(50))
    record_num = Column(Integer)
    status = Column(Integer)
    ext_column = Column(JSONB)

    class Status(IntEnum):
        invalid = 0
        valid = 1
