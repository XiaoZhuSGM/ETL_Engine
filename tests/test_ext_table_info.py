from flask import url_for


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
