import pytest
from flask import url_for, json

from config.config import config
from etl import create_app


@pytest.fixture(scope="module")
def app():
    app = create_app(config=config["unittest"])
    return app


@pytest.fixture
def token(client):
    res = client.post(
        url_for("admin_api.login"),
        data=json.dumps({"username": "etl", "password": "chaomengdata"}),
        content_type="application/json",
    )
    return res.json["data"]["token"]
