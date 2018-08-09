# -*- coding: utf-8 -*-
# @Time    : 2018/8/3 下午1:53
# @Author  : 范佳楠
import uuid

from flask import url_for, json

from etl.models.datasource import ExtDatasource


class TestExtDatasource:

    def test_get_ext_datasources(self, client, token):
        res = client.get(
            url_for("admin_api.get_all_datasource"),
            query_string={"per_page": "5", "page": "1"},
            headers={"token": token},
        )
        assert isinstance(res.json["data"]["items"], list)
        assert isinstance(res.json["data"]["total"], int)
        assert res.json['meta']['code'] == 200 and res.json['meta']['message'] == 'OK'

        res = client.get(
            url_for("admin_api.get_all_datasource"),
            headers={"token": token},
        )
        assert isinstance(res.json['data'], list)
        assert res.json['meta']['code'] == 200 and res.json['meta']['message'] == 'OK'

    def test_get_ext_datasource(self, client, token):
        res = client.get(
            url_for("admin_api.get_datasource", source_id=1), headers={"token": token}
        )
        assert res.json["meta"]["code"] == 200 or res.json['meta']['code'] == 404

    def test_create_ext_datasource(self, client, token):
        source_id = str(uuid.uuid1())[:15]
        data = {
            "source_id": source_id,
            "cmid": [1, 2, 3],
            'company_name': 'unittest',
            'erp_vendor': 'unittest',
            'db_type': 'unittest',
            'host': 'localhost',
            'port': 8080,
            'username': 'unittest',
            'password': 'unittest',
            'db_name': ['one', 'two'],
            'traversal': False,
            'delta': 10,
            'status': 10
        }
        res = client.post(
            url_for("admin_api.add_datasource"),
            headers={"token": token},
            data=(json.dumps(data)),
            content_type="application/json",
        )
        assert res.json["meta"]["code"] == 200
        ext_datasource = ExtDatasource.query.filter_by(source_id=source_id).first()
        assert isinstance(ext_datasource, ExtDatasource)

    def test_modify_ext_datasource(self, client, token):
        old = ExtDatasource.query.order_by(ExtDatasource.id.desc()).first()
        old_id = old.id
        source_id = str(uuid.uuid1())[:15]
        data = {
            'id': old_id,
            "source_id": source_id,
            "cmid": [1, 2, 3],
            'company_name': 'unittest',
            'erp_vendor': 'unittest',
            'db_type': 'unittest',
            'host': 'localhost',
            'port': 8080,
            'username': 'unittest',
            'password': 'unittest',
            'db_name': ['one', 'two'],
            'traversal': False,
            'delta': 20,
            'status': 10
        }

        res = client.patch(
            url_for("admin_api.update_datasource", id=old_id),
            headers={"token": token},
            data=(json.dumps(data)),
            content_type="application/json",
        )
        assert res.json['meta']['code'] == 200
        datasource = ExtDatasource.query.filter_by(id=old_id).one_or_none()
        assert isinstance(datasource, ExtDatasource)
        assert datasource.source_id == source_id
