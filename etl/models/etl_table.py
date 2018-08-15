from sqlalchemy import VARCHAR, REAL, Integer, DateTime, String, Column
from sqlalchemy.orm import relationship
from etl.etl import db
from .base import CRUDMixin


class ExtErpEnterprise(CRUDMixin, db.Model):
    name = Column(VARCHAR(100))
    version = Column(VARCHAR(50))
    remark = Column(VARCHAR(1000))



class ExtChainStoreOnline(CRUDMixin, db.Model):
    source_id = Column(String(15))
    cmid = Column(Integer)
    company_name = Column(VARCHAR(100))


class ExtStoreDetail(CRUDMixin, db.Model):
    source_id = Column(String(15))
    cmid = Column(Integer)
    store_id = Column(String(50))
    store_name = Column(VARCHAR(100))
    total_cost = Column(REAL)
    total_sale = Column(REAL)
    ext_date = Column(DateTime)


class ExtLogInfo(CRUDMixin, db.Model):
    source_id = Column(String(15))
    cmid = Column(Integer)
    task_type = Column(Integer)
    table_name = Column(String(512))
    record_num = Column(Integer)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    cost_time = Column(Integer)
    result = Column(Integer)
    remark = Column(VARCHAR(1000))


class ExtCleanInfo(CRUDMixin, db.Model):
    """
    合成目标表所需要信息
    """
    source_id = Column(String(15))
    origin_table = Column(VARCHAR(255), comment="合成目标表需要的原始表")
    target_table = Column(VARCHAR(50), comment="目标表，譬如goodsflew,chain_goods等")

    ext_erp = relationship(
        'ExtErpEnterprise',
        primaryjoin='remote(ExtCleanInfo.source_id) == foreign(ExtErpEnterprise.source_id)')


