from etl import db
from .base import CRUDMixin
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship



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
    db_schema = db.Column(db.String(50))
    db_name = db.Column(JSONB)
    traversal = db.Column(db.Boolean)
    delta = db.Column(db.Integer)
    status = db.Column(db.Integer)

    ext_tables = relationship(
        'ExtTableInfo',
        primaryjoin='remote(ExtDatasource.cmid) == foreign(ExtTableInfo.cmid)',
        back_populates='datasource')

    # @staticmethod
    # def from_json(datasoure_json):
    #     return ExtDatasource(
    #         source_id=datasoure_json['source_id'],
    #         cmid=datasoure_json['cmid'],
    #         company_name=datasoure_json['company_name'],
    #         erp_vendor=datasoure_json['erp_vendor'],
    #         db_type=datasoure_json['db_type'],
    #         host=datasoure_json['host'],
    #         port=datasoure_json['port'],
    #         username=datasoure_json['username'],
    #         password=datasoure_json['password'],
    #         db_schema=datasoure_json['db_schema'],
    #         db_name=datasoure_json['db_name'],
    #         traversal=datasoure_json['traversal'],
    #         delta=datasoure_json['delta'],
    #         status=datasoure_json['status'])

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

    # @staticmethod
    # def dict_to_datasource(json_object):
    #     return ExtDatasource(
    #         id=json_object['id'],
    #         source_id=json_object['source_id'],
    #         cmid=json_object['cmid'],
    #         company_name=json_object['company_name'],
    #         erp_vendor=json_object['erp_vendor'],
    #         db_type=json_object['db_type'],
    #         host=json_object['host'],
    #         port=json_object['port'],
    #         username=json_object['username'],
    #         password=json_object['password'],
    #         db_schema=json_object['db_schema'],
    #         db_name=json_object['db_name'],
    #         traversal=json_object['traversal'],
    #         delta=json_object['delta'],
    #         status=json_object['status'])
