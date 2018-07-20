from etl.etl import db
from sqlalchemy import VARCHAR, REAL, Integer, JSON, DateTime, Boolean, ForeignKey,String
from sqlalchemy.orm import relationship
from .base import CRUDMixin


class ExtDatasource(CRUDMixin, db.Model):
    source_id = db.Column(VARCHAR(15))
    cmid = db.Column(Integer, unique=True)
    company_name = db.Column(VARCHAR(100))
    erp_vendor = db.Column(VARCHAR(50))
    db_type = db.Column(VARCHAR(50))
    host = db.Column(VARCHAR(255))
    port = db.Column(Integer)
    username = db.Column(VARCHAR(50))
    password = db.Column(VARCHAR(50))
    db_schema = db.Column(VARCHAR(50))
    db_name = db.Column(JSON)
    traversal = db.Column(Boolean)
    delta = db.Column(Integer)
    status = db.Column(Integer)
    ext_tables = relationship('ExtTableInfo', back_populates='datasource')


class ExtTableInfo(CRUDMixin, db.Model):
    table_name = db.Column(VARCHAR(50))
    ext_pri_key = db.Column(VARCHAR(100))
    sync_column = db.Column(VARCHAR(100))
    order_column = db.Column(VARCHAR(100))
    limit_num = db.Column(Integer)
    filter = db.Column(VARCHAR(500))
    filter_format = db.Column(VARCHAR(50))
    status = db.Column(Integer)
    ext_column = db.Column(JSON)
    record_num = db.Column(Integer)
    cmid = db.Column(Integer, ForeignKey('ext_datasource.cmid'))
    datasource = relationship('ExtDatasource', back_populates='ext_tables')


class ExtDatasourceCon(CRUDMixin, db.Model):
    source_id = db.Column(VARCHAR(15))
    cmid = db.Column(Integer)
    roll_back = db.Column(Integer)
    frequency = db.Column(Integer)
    period = db.Column(Integer)


class ErpEnterprise(CRUDMixin, db.Model):
    name = db.Column(VARCHAR(100))
    version = db.Column(VARCHAR(50))
    remark = db.Column(VARCHAR(1000))


class ChainStoreOnline(CRUDMixin, db.Model):
    source_id = db.Column(VARCHAR(15))
    cmid = db.Column(Integer)
    company_name = db.Column(VARCHAR(100))


class StoreDetail(CRUDMixin, db.Model):
    source_id = db.Column(VARCHAR(15))
    cmid = db.Column(Integer)
    store_id = db.Column(String(50))
    store_name = db.Column(VARCHAR(100))
    total_cost = db.Column(REAL)
    total_sale = db.Column(REAL)
    ext_date = db.Column(DateTime)
