import mock
import os

from ngi_pipeline.conductor.launchers import launch_analysis
from ngi_pipeline.conductor.classes import NGIProject

@mock.patch.dict(os.environ, {'CHARON_BASE_URL': 'charon-url', 'CHARON_API_TOKEN': 'token'})
@mock.patch('ngi_pipeline.conductor.classes.CharonSession.project_get')
@mock.patch('ngi_pipeline.engines.sarek.local_process_tracking.update_charon_with_local_jobs_status')
@mock.patch('ngi_pipeline.engines.sarek.analyze')
def test_launch_analysis(mock_analyze, mock_update, mock_get_engine):
    mock_get_engine.return_value = {'best_practice_analysis': 'wgs_germline',
                                    'status': 'OPEN'}
    project = NGIProject('S.One_20_01', 'dir_P123', 'P123', '/some/path')
    launch_analysis([project])
    mock_analyze.assert_called_once()