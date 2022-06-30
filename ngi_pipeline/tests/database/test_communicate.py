import unittest
import mock
import os

from ngi_pipeline.database.communicate import get_project_id_from_name
from ngi_pipeline.database.classes import CharonError

class TestCommunicate(unittest.TestCase):

    def setUp(self):
        self.project_id = 'P100000'
        self.project_name = 'S.One_20_01'

    @mock.patch.dict(os.environ, {'CHARON_BASE_URL': 'charon-url', 'CHARON_API_TOKEN': 'token'})
    @mock.patch('ngi_pipeline.database.communicate.CharonSession.project_get')
    def test_get_project_id_from_name(self, mock_get):
        """Return project ID given the project name"""
        mock_get.return_value = {'projectid': 'P100000'}
        self.assertEqual(self.project_id, 
                         get_project_id_from_name(self.project_name))

    @mock.patch('ngi_pipeline.database.communicate.CharonSession.project_get')
    def test_get_project_id_from_name_missing_proj(self, mock_get):
        """Raise ValueError if project is missing"""
        mock_get.side_effect = CharonError('Error', status_code=404)
        with self.assertRaises(ValueError):
            get_project_id_from_name(self.project_name)

    @mock.patch('ngi_pipeline.database.communicate.CharonSession.project_get')
    def test_get_project_id_from_name_missing_id(self, mock_get):
        """Raise ValueError if 'projectid' is missing"""
        mock_get.return_value = {}
        with self.assertRaises(ValueError):
            get_project_id_from_name(self.project_name)
