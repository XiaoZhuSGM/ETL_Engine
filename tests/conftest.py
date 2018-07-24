from etl import create_app
from config.config import config
import pytest


@pytest.fixture(scope="module")
def app():
    app = create_app(config=config["unittest"])
    return app
