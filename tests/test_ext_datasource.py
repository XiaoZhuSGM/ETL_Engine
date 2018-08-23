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

    def test_get_ext_datasource_by_erp(self, client, token):
        res = client.get(
            url_for("admin_api.get_datasouce_by_erp", erp_vendor='思'),
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
            "datasource": {
                "source_id": source_id,
                "cmid": [
                    34
                ],
                "company_name": "fsf",
                "erp_vendor": "fdsf",
                "db_type": "sqlserver",
                "host": "fdfsd",
                "port": 212,
                "username": "fanjianan",
                "password": "fabfa",
                "db_name": {
                    "database": "fdsffdsf",
                    "schema": [
                        "fsdfsafsfff"
                    ]
                },
                "traversal": True,
                "delta": 1,
                "status": 1
            },
            "datasource_config": {
                "source_id": source_id,
                "roll_back": 121,
                "frequency": 21,
                "period": 343,
                'ext_time': '12:21:21'
            }
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

    def test_generator_crontab_expression(self, client, token):
        datasource = ExtDatasource.query.order_by(ExtDatasource.id.desc()).first()
        res = client.get(
            url_for('admin_api.generator_crontab', source_id=datasource.source_id),
            headers={"token": token},
        )
        assert res.json["meta"]["code"] == 200 or res.json['meta']['code'] == 404

    def test_generator_extract_event(self, client, token):
        datasource = ExtDatasource.query.order_by(ExtDatasource.id.desc()).first()
        res = client.get(
            url_for('admin_api.generator_extract_event', source_id=datasource.source_id),
            headers={"token": token},
        )
        assert res.json["meta"]["code"] == 200 or res.json['meta']['code'] == 404

    def test_modify_ext_datasource(self, client, token):
        old = ExtDatasource.query.order_by(ExtDatasource.id.desc()).first()
        old_id = old.id
        source_id = str(uuid.uuid1())[:15]

        data = {
            "datasource": {
                'id': old_id,
                "source_id": source_id,
                "cmid": [
                    34
                ],
                "company_name": "fsf",
                "erp_vendor": "fdsf",
                "db_type": "sqlserver",
                "host": "fdfsd",
                "port": 212,
                "username": "fanjianan",
                "password": "fabfa",
                "db_name": {
                    "database": "fdsffdsf",
                    "schema": [
                        "fsdfsafsfff"
                    ]
                },
                "traversal": True,
                "delta": 1,
                "status": 1
            },
            "datasource_config": {
                "source_id": source_id,
                "roll_back": 121,
                "frequency": 21,
                "period": 343,
                'ext_time': '12:21:21'
            }
        }

        res = client.patch(
            url_for("admin_api.update_datasource", datasource_id=old_id),
            headers={"token": token},
            data=(json.dumps(data)),
            content_type="application/json",
        )
        assert res.json['meta']['code'] == 200
        datasource = ExtDatasource.query.filter_by(id=old_id).one_or_none()
        assert isinstance(datasource, ExtDatasource)
        assert datasource.source_id == source_id
