from sqlalchemy import VARCHAR, REAL, Integer, DateTime, String
from sqlalchemy.dialects.postgresql import JSONB
from etl.etl import db
from .base import CRUDMixin


class ExtErpEnterprise(CRUDMixin, db.Model):
    name = db.Column(VARCHAR(100))
    version = db.Column(VARCHAR(50))
    remark = db.Column(VARCHAR(1000))


class ExtChainStoreOnline(CRUDMixin, db.Model):
    source_id = db.Column(db.String(30))
    cmid = db.Column(JSONB)
    company_name = db.Column(VARCHAR(100))


class ExtStoreDetail(CRUDMixin, db.Model):
    source_id = db.Column(db.String(30))
    cmid = db.Column(JSONB)
    store_id = db.Column(String(50))
    store_name = db.Column(VARCHAR(100))
    total_cost = db.Column(REAL)
    total_sale = db.Column(REAL)
    ext_date = db.Column(DateTime)
