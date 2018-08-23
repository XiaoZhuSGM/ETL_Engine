# -*- coding: utf-8 -*-
# @Time    : 2018/8/23 17:02
# @Author  : 范佳楠

import uuid
from flask import url_for, json
from etl.models.etl_table import ExtLogInfo


class TestExtLog:

    def test_create_ext_log(self, client, token):
        source_id = str(uuid.uuid1())[:15]
        data = {"source_id": source_id, "cmid": 3201, "task_type": 1, "table_name": "goods", "record_num": 120,
                "start_time": "2018-08-21", "end_time": "2018-08-21", "cost_time": 111, "result": 1,
                "extract_date": "2018-08-21"}

        res = client.post(
            url_for("admin_api.add_log"),
            headers={"token": token},
            data=(json.dumps(data)),
            content_type="application/json",
        )
        assert res.json["meta"]["code"] == 200

    def test_get_ext_log(self, client, token):
        query_string = {
            'page': 1,
            'per_page': 2,
            'source_id': '72YYYYYYYYYYYYY',
            'result': 1,
            'table_name': 'store',
            'task_type': 1,
            'start_time': '2018-07-02',
            'end_time': '2018-09-02',

        }
        res = client.get(
            url_for("admin_api.get_log"),
            query_string=query_string,
            headers={"token": token},
        )
        assert isinstance(res.json["data"]["items"], list)
        assert isinstance(res.json["data"]["total"], int)
        assert res.json['meta']['code'] == 200 and res.json['meta']['message'] == 'OK'
