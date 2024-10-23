import os

from ngi_pipeline.utils.classes import with_ngi_config


@with_ngi_config
def load_charon_variables(config=None, config_file_path=None):
    """Attempts to locate Charon-specific variables CHARON_API_TOKEN
    and CHARON_BASE_URL; searches config file and then environmental variables.

    :param dict config: The parsed ngi_pipeline config file (optional)
    :param str config_file_path: The path to the ngi_pipeline config (optional)

    :returns: A dict of the variables by name
    :rtype: dict
    """
    vars_dict = {}
    var_names = ("charon_api_token", "charon_base_url")
    for var_name in var_names:
        var_value = config.get("charon", {}).get(var_name) or os.environ.get(
            var_name.upper()
        )
        if var_value:
            vars_dict[var_name] = var_value
    return vars_dict
