import mock
from ngi_pipeline.utils.post_analysis import run_multiqc

@mock.patch('ngi_pipeline.utils.post_analysis.safe_makedir')
@mock.patch('ngi_pipeline.utils.post_analysis.execute_command_line')
def test_run_multiqc(mock_exec, mock_mkdir):
    base_path = '/some/path'
    project_id = 'P123'
    project_name = 'S.One_20_04'
    run_multiqc(base_path, project_id, project_name)
    mock_exec.assert_called_once_with(['multiqc',
                                       '/some/path/ANALYSIS/P123',
                                       '-o', '/some/path/ANALYSIS/P123/multiqc',
                                       '-i', 'S.One_20_04',
                                       '-n', 'S.One_20_04', 
                                       '-q',
                                       '-f'
                                       ])