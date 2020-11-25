import json
import os
import xmltodict
import yaml


def locate_ngi_config():
    config_file_path = os.environ.get("NGI_CONFIG") or os.path.expandvars("$HOME/.ngipipeline/ngi_config.yaml")
    if not os.path.isfile(config_file_path):
        error_msg = ("Configuration file \"{}\" does not exist or is not a "
                     "file. Cannot proceed.".format(config_file_path))
        raise RuntimeError(error_msg)
    return config_file_path


def load_yaml_config(config_file_path):
    """Load YAML config file, expanding environmental variables.

    :param str config_file_path: The path to the configuration file.

    :returns: A dict of the parsed config file.
    :rtype: dict
    :raises IOError: If the config file cannot be opened.
    """
    return load_generic_config(config_file_path, config_format="yaml", Loader=yaml.FullLoader)


def load_generic_config(config_file_path, config_format="yaml", **kwargs):
    """Parse a configuration file, returning a dict. Supports yaml, xml, and json.

    :param str config_file_path: The path to the configuration file.
    :param str config_format: The format of the config file; default yaml.

    :returns: A dict of the configuration file with environment variables expanded.
    :rtype: dict
    :raises IOError: If the config file could not be opened.
    :raises ValueError: If config file could not be parsed.
    """
    parsers_dict = {"json": json.load,
                    "xml": xmltodict.parse,
                    "yaml": yaml.load,}
    try:
        parser_fn = parsers_dict[config_format.lower()]
    except KeyError:
        raise ValueError("Cannot parse config files in format specified "
                         "(\"{}\"): format not supported.".format(config_format))
    try:
        with open(config_file_path) as in_handle:
            config = parser_fn(in_handle, **kwargs)
            config = _expand_paths(config)
            return config
    except IOError as e:
        raise IOError("Could not open configuration file \"{}\".".format(config_file_path))


def _expand_paths(config):
    for field, setting in config.items():
        if isinstance(config[field], dict):
            config[field] = _expand_paths(config[field])
        else:
            config[field] = expand_path(setting)
    return config

def expand_path(path):
    """Combines os.path.expandvars with replacing ~ with $HOME."""
    try:
        return os.path.expandvars(path.replace("~", "$HOME"))
    except AttributeError:
        return path
