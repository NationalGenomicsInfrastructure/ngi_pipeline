import unittest
import mock
from ngi_pipeline.conductor.classes import NGIAnalysis, NGIObject, \
                                            NGIProject, NGISeqRun, \
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

class TestHelperFunctions(unittest.TestCase):

    @mock.patch('ngi_pipeline.conductor.classes.CharonSession.project_get')
    @mock.patch('ngi_pipeline.conductor.classes.load_engine_module')
    def test_get_engine_for_bp(self, mock_load, mock_charon):
        mock_charon.return_value = {'best_practice_analysis': 'some_BP'}
        mock_load.return_value = 'some_engine'
        conf = {'dummy': 'conf'}
        project = NGIProject('S.One_20_01', 'dir_P123', 'P123', '/some/path')
        got_engine = get_engine_for_bp(project, config=conf)

        mock_load.assert_called_once_with('some_BP', conf)
        self.assertEqual(got_engine, 'some_engine')

    @mock.patch('ngi_pipeline.conductor.classes.importlib.import_module')
    def test_load_engine_module(self, mock_import):
        mock_import.return_value = 'some_engine'
        BP_analysis = 'some_BP'
        conf = {'analysis': {'best_practice_analysis': {'some_BP': {'analysis_engine': 'some_engine'}}}}
        got_engine = load_engine_module(BP_analysis, conf)
        
        mock_import.assert_called_once_with('some_engine')
        self.assertEqual(got_engine, 'some_engine')