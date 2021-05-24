import pytest
from app.main import app
from starlette.testclient import TestClient


@pytest.fixture(scope="function")
def testclient():

    with TestClient(app) as client:
        # Application 'startup' handlers are called on entering the block.
        yield client
    # Application 'shutdown' handlers are called on exiting the block.
