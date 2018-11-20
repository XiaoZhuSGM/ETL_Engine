from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from etl import db
from .base import CRUDMixin
from datetime import datetime, timedelta


class ExtDatasource(CRUDMixin, db.Model):
    source_id = db.Column(db.String(15), unique=True, nullable=False)
    cmid = db.Column(JSONB, nullable=False)
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
    fetch_status = db.Column(db.String(10))

    ext_tables = relationship(
        "ExtTableInfo",
        primaryjoin="foreign(ExtDatasource.source_id) == remote(ExtTableInfo.source_id)",
        back_populates="datasource",
    )

    ext_datasource_config = relationship(
        "ExtDatasourceCon",
        primaryjoin="foreign(ExtDatasource.source_id) == remote(ExtDatasourceCon.source_id)",
        back_populates="datasource",
    )

    ext_clean_infos = relationship(
        "ExtCleanInfo",
        primaryjoin="foreign(ExtDatasource.source_id) == remote(ExtCleanInfo.source_id)",
        back_populates="datasource",
        uselist=True,
    )

    def to_dict_and_config(self):
        data = {}
        data["datasource"] = {
            col: getattr(self, col) for col in self.__table__.columns.keys()
        }
        if self.ext_datasource_config:
            data["datasource_config"] = {
                col: getattr(self.ext_datasource_config, col)
                for col in self.ext_datasource_config.__table__.columns.keys()
            }
        else:
            data["datasource_config"] = {}

        return data

    def to_clean_info_dict(self):
        data = {}
        data["erp_name"] = self.erp_vendor
        data["target_list"] = [
            clean_info.target_table
            for clean_info in self.ext_clean_infos
            if not clean_info.deleted
        ]
        ext_time = self.ext_datasource_config.ext_time
        cron_list = [x.lstrip("0") for x in ext_time.split(":")]
        cron_list = [x if x else "0" for x in cron_list]
        temp_time = datetime(2018, 6, 4, hour=int(cron_list[0]))
        temp_time = temp_time + timedelta(hours=-8)
        hour = temp_time.hour
        minute = cron_list[1]
        cron_expression = f"{minute} {hour} * * *"
        data["crontab"] = cron_expression
        return data

