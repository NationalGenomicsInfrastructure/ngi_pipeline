import json
import os
import tempfile
import unittest
import yaml

from ngi_pipeline.utils.config import locate_ngi_config, load_yaml_config, \
                                        load_generic_config, _expand_paths, \
                                        expand_path
from ngi_pipeline.tests import generate_test_data as gtd

class TestConfigLoaders(unittest.TestCase):
    def setUp(self):
        self.config_dict = {'base_key':
                                {'string_key': 'string_value',
                                 'list_key': ['list_value_1', 'list_value_2'],
                                 }
                                }
        self.tmp_dir = tempfile.mkdtemp()

    def test_locate_ngi_config_environ(self):
        environ_var_holder = os.environ.get('NGI_CONFIG')
        temp_ngi_config = os.path.join(self.tmp_dir, 'ngi_config.yaml')
        open(temp_ngi_config, 'w').close()
        try:
            os.environ['NGI_CONFIG'] = temp_ngi_config
            assert(locate_ngi_config())
        finally:
            if environ_var_holder:
                os.environ['NGI_CONFIG'] = environ_var_holder
            else:
                os.environ.pop('NGI_CONFIG')

    def test_load_yaml_config(self):
        config_file_path = os.path.join(self.tmp_dir, 'config.yaml')
        with open(config_file_path, 'w') as config_file:
            config_file.write(yaml.dump(self.config_dict, default_flow_style=False))
        
        self.assertEqual(self.config_dict, load_yaml_config(config_file_path))

    def test_load_generic_config_IOError(self):
        config_file_path = '/no/such/file'
        with self.assertRaises(IOError):
            load_generic_config(config_file_path)

    def test_expand_path(self):
        self.assertEqual(os.environ['HOME'], expand_path('~'))

    def test_expand_paths(self):
        unexpanded_dict = {'home': '$HOME',
                           'home_dict': {'home': '~'}}
        home_path = os.environ['HOME']
        expanded_dict = {'home': home_path,
                         'home_dict': {'home': home_path}}
        self.assertEqual(expanded_dict, _expand_paths(unexpanded_dict))
