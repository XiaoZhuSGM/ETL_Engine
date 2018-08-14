# -*- coding: utf-8 -*-
# @Time    : 2018/8/3 下午1:53
# @Author  : 范佳楠
import uuid

from flask import url_for, json

from etl.models.etl_table import ExtErpEnterprise


class TestErpEnterprise:

    def test_get_ext_enterprises(self, client, token):
        res = client.get(
            url_for("admin_api.get_all_enterprise"),
            query_string={"per_page": "5", "page": "1"},
            headers={"token": token},
        )
        assert isinstance(res.json["data"]["items"], list)
        assert isinstance(res.json["data"]["total"], int)
        assert res.json['meta']['code'] == 200 and res.json['meta']['message'] == 'OK'

        res = client.get(
            url_for("admin_api.get_all_enterprise"),
            headers={"token": token},
        )
        assert isinstance(res.json['data'], list)
        assert res.json['meta']['code'] == 200 and res.json['meta']['message'] == 'OK'

    def test_get_ext_enterprise(self, client, token):
        res = client.get(
            url_for("admin_api.get_enterprise", enterprise_id=1), headers={"token": token}
        )
        assert res.json["meta"]["code"] == 200 or res.json['meta']['code'] == 404

    def test_create_ext_enterprise(self, client, token):
        value = str(uuid.uuid1())[:15]
        data = {
            "name": value,
            "version": value,
            "remark": value,
        }

        res = client.post(
            url_for("admin_api.add_enterprise"),
            headers={"token": token},
            data=(json.dumps(data)),
            content_type="application/json",
        )
        assert res.json["meta"]["code"] == 200
        enterprise = ExtErpEnterprise.query.filter_by(name=value).first()
        assert isinstance(enterprise, ExtErpEnterprise)

    def test_modify_ext_datasource(self, client, token):
        old = ExtErpEnterprise.query.order_by(ExtErpEnterprise.id.desc()).first()
        old_id = old.id
        # value = str(uuid.uuid1())[:15]
        value = 'abc'
        data = {
            'id': old_id,
            "name": value,
            "version": value,
            "remark": value,
        }

        res = client.patch(
            url_for("admin_api.update_enterprise", enterprise_id=old_id),
            headers={"token": token},
            data=(json.dumps(data)),
            content_type="application/json",
        )
        assert res.json['meta']['code'] == 200
        enterprise = ExtErpEnterprise.query.filter_by(id=old_id).one_or_none()
        assert isinstance(enterprise, ExtErpEnterprise)
        assert enterprise.name == value
