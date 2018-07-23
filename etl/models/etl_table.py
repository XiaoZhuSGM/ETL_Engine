from etl.etl import db
from sqlalchemy import VARCHAR, REAL, Integer, DateTime, String
from .base import CRUDMixin


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
