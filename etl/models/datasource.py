from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from etl import db
from .base import CRUDMixin


class ExtDatasource(CRUDMixin, db.Model):
    source_id = db.Column(db.String(30), nullable=False)
    cmid = db.Column(db.Integer, unique=True, nullable=False)
    company_name = db.Column(db.String(100), nullable=False)
    erp_vendor = db.Column(db.String(50), nullable=False)
    db_type = db.Column(db.String(50))
    host = db.Column(db.String(255))
    port = db.Column(db.Integer)
    username = db.Column(db.String(50))
    password = db.Column(db.String(50))
    db_name = db.Column(JSONB)
    traversal = db.Column(db.Boolean)
    delta = db.Column(db.Integer)
    status = db.Column(db.Integer)

    ext_tables = relationship(
        'ExtTableInfo',
        primaryjoin='remote(ExtDatasource.cmid) == foreign(ExtTableInfo.cmid)',
        back_populates='datasource')

    def to_dict(self):
        data = {col: getattr(self, col) for col in self.__table__.columns.keys()}
        data.pop('password', None)
        return data
