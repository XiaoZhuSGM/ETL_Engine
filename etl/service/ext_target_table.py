from etl.models import ExtTargetInfo
from common.common import upload_body_to_s3, S3_BUCKET, TARGET_TABLE_KEY
import json


class TargetTableService:
    def store_target_table_info(self):
        table_info = ExtTargetInfo.query.all()
        data = {
            info.target_table: {
                "target_table": info.target_table,
                "remark": info.remark,
                "sync_column": info.sync_column
                if not info.sync_column
                else info.sync_column.split(","),
                "date_column": info.date_column,
            }
            for info in table_info
        }
        upload_body_to_s3(S3_BUCKET, TARGET_TABLE_KEY, json.dumps(data))
        return data
