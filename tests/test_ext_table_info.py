from flask import url_for, json

from etl.models.ext_table_info import ExtTableInfo


class TestExtTableInfo:
    def test_get_ext_table_infos(self, client, token):
        res = client.get(
            url_for("admin_api.get_ext_table_infos"),
            query_string={"source_id": "1"},
            headers={"token": token},
        )
        assert isinstance(res.json["data"]["items"], list)
        assert isinstance(res.json["data"]["total"], int)

    def test_get_ext_table_info(self, client, token):
        res = client.get(
            url_for("admin_api.get_ext_table_info", id=1), headers={"token": token}
        )
        assert res.json["meta"]["code"] == 200

    def test_create_ext_table_info(self, client, token):
        old = ExtTableInfo.query.order_by(ExtTableInfo.id.desc()).first()
        source_id = str(old.id + 1) if old else "1"
        data = {
            "source_id": source_id,
            "table_name": "unittest",
            "ext_pri_key": "unittest",
            "sync_column": ["unittest1", "unittest2"],
            "order_column": ["unittest1", "unittest2"],
            "limit_num": 10,
            "filter": "filter",
            "filter_format": "YYYY-mm-dd",
            "record_num": 10,
            "weight": 1,
            "ext_column": {"type": "object", "value": "key"},
        }
        res = client.post(
            url_for("admin_api.create_ext_table_info"),
            headers={"token": token},
            data=(json.dumps(data)),
            content_type="application/json",
        )
        assert res.json["meta"]["code"] == 200
        ext_table_info = ExtTableInfo.query.filter_by(source_id=source_id).first()
        assert isinstance(ext_table_info, ExtTableInfo)

    def test_modify_ext_table_info(self, client, token):
        old = ExtTableInfo.query.order_by(ExtTableInfo.id.desc()).first()
        data = {
            "table_name": "unittest",
            "ext_pri_key": "unittest",
            "sync_column": ["unittest2", "unittest1"],
            "order_column": ["unittest1", "unittest2"],
            "limit_num": 10,
            "filter": "filter",
            "filter_format": "YYYY-mm-dd",
            "record_num": 10,
            "weight": 1,
            "ext_column": {"type": "object", "value": "key"},
        }
        res = client.patch(
            url_for("admin_api.modify_ext_table_info", id=old.id),
            headers={"token": token},
            data=(json.dumps(data)),
            content_type="application/json",
        )
        assert res.json["meta"]["code"] == 200
