import unittest
import mock
from ngi_pipeline.conductor.classes import NGIAnalysis, NGIObject, \
                                            NGIProject, NGISample, \
                                                NGILibraryPrep, NGISeqRun, \
                                                    get_engine_for_bp, load_engine_module

class TestNGIAnalysis(unittest.TestCase):

    @mock.patch('ngi_pipeline.conductor.classes.get_engine_for_bp')
    def test_get_engine(self, mock_get_engine):
        mock_get_engine.return_value = 'some_engine'
        analysis = NGIAnalysis('P12345')
        self.assertEqual(analysis.engine, 'some_engine')
    
    @mock.patch('ngi_pipeline.conductor.classes.get_engine_for_bp')
    def test_get_engine_error(self, mock_get_engine):
        mock_get_engine.side_effect = RuntimeError('Error')
        analysis = NGIAnalysis('P12345')
        self.assertIsNone(analysis.engine)

class TestNGIObject(unittest.TestCase):

    def test_ngi_obj(self):
        obj1 = NGIObject('P123', 'dir_P123', 'subitem')
        obj2 = NGIObject('P123', 'dir_P123', 'subitem')
        self.assertEqual(obj1, obj2)

class TestNGIProject(unittest.TestCase):

    def test_ngi_proj(self):
        proj1 = NGIProject('S.One_20_01', 'dir_P123', 'P123', '/some/path')
        proj2 = NGIProject('S.One_20_01', 'dir_P123', 'P123', '/some/path')
        self.assertEqual(proj1, proj2)

class TestNGISample(unittest.TestCase):

    def test_ngi_sample(self):
        pass