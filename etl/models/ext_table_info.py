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
    filter = Column(String(2000))
    filter_format = Column(String(50))
    record_num = Column(Integer)
    weight = Column(Integer, comment="表示是否抓数 0 不抓 1 抓 2 下次抓")
    ext_column = Column(JSONB)
    remark = Column(String(50))
    strategy = Column(Integer)
    special_column = Column(String(500), comment="特殊列")
    inventory_table = Column(Integer, comment="表示是否是库存表 0 不是 1 是")
    max_page_num = Column(Integer, comment="每页最多行数")

    datasource = relationship(
        "ExtDatasource",
        primaryjoin="remote(ExtTableInfo.source_id) == foreign(ExtDatasource.source_id)",
        uselist=False,
        back_populates="ext_tables",
    )
