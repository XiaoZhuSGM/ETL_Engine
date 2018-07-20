from etl import db
from .base import CRUDMixin
from sqlalchemy.dialects.postgresql import JSONB

class Datasource(CRUDMixin, db.Model):
    source_id = db.Column(db.String(30), nullable=False)
    cmid = db.Column(db.Integer, unique=True, nullable=False)
    company_name = db.Column(db.String(100), nullable=False)
    erp_vendor = db.Column(db.String(50), nullable=False)
    db_type = db.Column(db.String(50))
    host = db.Column(db.String(255))
    port = db.Column(db.Integer)
    username = db.Column(db.String(50))
    password = db.Column(db.String(50))
    db_schema = db.Column(db.String(50))
    db_name = db.Column(JSONB)
    traversal = db.Column(db.Boolean)
    delta = db.Column(db.Integer)
    status = db.Column(db.Integer)

    @staticmethod
    def from_json(datasoureJson):
        return Datasource(
            source_id=datasoureJson['source_id'],
            cmid=datasoureJson['cmid'],
            company_name=datasoureJson['company_name'],
            erp_vendor=datasoureJson['erp_vendor'],
            db_type=datasoureJson['db_type'],
            host=datasoureJson['host'],
            port=datasoureJson['port'],
            username=datasoureJson['username'],
            password=datasoureJson['password'],
            db_schema=datasoureJson['db_schema'],
            db_name=datasoureJson['db_name'],
            traversal=datasoureJson['traversal'],
            delta=datasoureJson['delta'],
            status=datasoureJson['status'])

    @staticmethod
    def datasource_to_dict(datasource):
        return {
            'id': datasource.id,
            'source_id': datasource.source_id,
            'cmid': datasource.cmid,
            'company_name': datasource.company_name,
            'erp_vendor': datasource.erp_vendor,
            'db_type': datasource.db_type,
            'host': datasource.host,
            'port': datasource.port,
            'username': datasource.username,
            'password': datasource.password,
            'db_schema': datasource.db_schema,
            'db_name': datasource.db_name,
            'traversal': datasource.traversal,
            'delta': datasource.delta,
            'status': datasource.status
        }

    @staticmethod
    def dict_to_datasource(jsonObject):
        return Datasource(
            id=jsonObject['id'],
            source_id=jsonObject['source_id'],
            cmid=jsonObject['cmid'],
            company_name=jsonObject['company_name'],
            erp_vendor=jsonObject['erp_vendor'],
            db_type=jsonObject['db_type'],
            host=jsonObject['host'],
            port=jsonObject['port'],
            username=jsonObject['username'],
            password=jsonObject['password'],
            db_schema=jsonObject['db_schema'],
            db_name=jsonObject['db_name'],
            traversal=jsonObject['traversal'],
            delta=jsonObject['delta'],
            status=jsonObject['status'])
