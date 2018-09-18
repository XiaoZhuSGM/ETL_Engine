from flask import url_for


class TestExtTable:
    def test_download_tables(self, client, token):
        res = client.get(
            url_for("admin_api.download_tables", source_id="99YYYYY"),
            headers={"token": token}
        )
        assert res.json["meta"]["code"] == 200 or res.json["meta"]["code"] == 404

    def test_get_download_tables_status(self, client, token):
        res = client.get(
            url_for("admin_api.get_download_tables_status", source_id="99YYYYY"),
            headers={"token": token}
        )
        assert res.json["meta"]["code"] == 200 or res.json["meta"]["code"] == 404
