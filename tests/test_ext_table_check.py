import json

from flask import url_for


class TestExtTableCheck:
    def test_get_ext_check(self, client, token):
        res = client.get(
            url_for("admin_api.get_ext_check", source_id="99YYYYY"), headers={"token": token}
        )

        assert res.json["meta"]["code"] == 200 or res.json["meta"]["code"] == 404

    def test_create_sql(self, client, token):
        data = {
            "sql": "sql",
            "source_id": "32YYYYY",
        }
        res = client.post(
            url_for("admin_api.create_sql"),
            data=json.dumps(data),
            headers={"token": token},
            content_type="application/json",
        )

        assert res.json["meta"]["code"] == 404

    def test_ext_save_sql(self, client, token):
        data = {
            "sql": "sql",
            "source_id": "32YYYYY",
        }
        res = client.post(
            url_for("admin_api.ext_save_sql"),
            data=json.dumps(data),
            headers={"token": token},
            content_type="application/json",
        )

        assert res.json["meta"]["code"] == 200
