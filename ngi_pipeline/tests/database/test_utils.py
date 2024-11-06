import mock
import os

from ngi_pipeline.database.utils import load_charon_variables


def test_load_charon_variables_w_conf():
    conf = {"charon": {"charon_api_token": "token", "charon_base_url": "URL"}}
    expected_charon_variables = {"charon_api_token": "token", "charon_base_url": "URL"}
    got_charon_variables = load_charon_variables(conf)
    assert got_charon_variables == expected_charon_variables


@mock.patch.dict(
    os.environ, {"CHARON_BASE_URL": "charon-url", "CHARON_API_TOKEN": "token"}
)
def test_load_charon_variables_wo_conf():
    expected_charon_variables = {
        "charon_api_token": "token",
        "charon_base_url": "charon-url",
    }
    got_charon_variables = load_charon_variables()
    assert got_charon_variables == expected_charon_variables
