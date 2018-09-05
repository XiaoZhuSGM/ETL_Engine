from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from etl.etl import db
from .base import CRUDMixin


class ExtTableInfo(db.Model, CRUDMixin):
    __tablename__ = "ext_table_info"
    source_id = db.Column(db.String(15))
    table_name = Column(String(100))
    alias_table_name = Column(String(100), comment="表的别名")
    ext_pri_key = Column(String(200))
    order_column = Column(String(200))
    sync_column = Column(String(200))
    limit_num = Column(Integer)  # 分页页数
    filter = Column(String(500))
    filter_format = Column(String(50))
    record_num = Column(Integer)
    weight = Column(Integer)
    ext_column = Column(JSONB)
    remark = Column(String(50))
    strategy = Column(Integer)
    special_column = Column(String(500), comment="特殊列")

    datasource = relationship(
        "ExtDatasource",
        primaryjoin="remote(ExtTableInfo.source_id) == foreign(ExtDatasource.source_id)",
        uselist=False,
        back_populates="ext_tables",
    )
