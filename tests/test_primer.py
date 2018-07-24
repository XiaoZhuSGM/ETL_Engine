from flask import url_for


class TestPrimer:
    def test_env(self, app):
        assert app.testing is True

    def test_ping(self, client):
        res = client.get(url_for("admin_api.ping"))
        assert res.json["ping"] == "pong"
