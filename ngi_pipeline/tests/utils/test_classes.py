from ngi_pipeline.utils.classes import with_ngi_config


@with_ngi_config
def some_function(config=None, config_file_path=None):
    return config


def test_with_ngi_config():
    got_config = some_function(config_file_path="data/test_ngi_config_minimal.yaml")
    expected_config = {
        "mail": {"recipient": "some_user@some_email.com"},
        "logging": {"log_file": "/some/log/file.log"},
        "analysis": {
            "sthlm_root": "ngi2016003",
            "best_practice_analysis": {
                "wgs_germline": {"analysis_engine": "ngi_pipeline.engines.sarek"}
            },
            "top_dir": "/some/dir",
            "base_root": "/some/dir",
        },
        "database": {"record_tracking_db_path": "/some/other/dir"},
    }
    assert got_config == expected_config
