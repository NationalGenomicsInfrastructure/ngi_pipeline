import unittest
import mock
import tempfile
import shutil
import os

import ngi_pipeline.engines.qc_ngi.launchers as launchers
from ngi_pipeline.conductor.classes import NGIProject, NGISample

class TestLaunchers(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.project = NGIProject('S.One_20_01', 
                                  'dir_P123', 
                                  'P123', 
                                  self.tmp_dir)
        self.sample = NGISample('P123_1001', 'dir_P123_1001')
    
    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.tmp_dir)

    @mock.patch('ngi_pipeline.engines.qc_ngi.launchers.return_cls_for_workflow')
    @mock.patch('ngi_pipeline.engines.qc_ngi.launchers.create_sbatch_file')
    @mock.patch('ngi_pipeline.engines.qc_ngi.launchers.queue_sbatch_file')
    def test_analyze(self, mock_queue, mock_create, mock_commands):
        mock_commands.return_value = [['echo', 'Hello!']]
        mock_queue.return_value = 123
        config = {}
        launchers.analyze(self.project, self.sample, config=config)
        job_file = os.path.join(self.tmp_dir, 
                                'ANALYSIS', 
                                'P123', 
                                'qc_ngi', 
                                'logs', 
                                'P123-P123_1001.slurmjobid')
        with open(job_file, 'r') as file:
            got_jobid = file.read().strip('\n')
        self.assertEqual(got_jobid, '123')

    @mock.patch('ngi_pipeline.engines.qc_ngi.launchers.execute_command_line')
    def test_queue_sbatch_file(self, mock_exec):
        mock_exec().communicate.return_value = (b'Submitted batch job 12345', b'')
        sbatch_file_path = 'some/path/job.sbatch'
        job_id = launchers.queue_sbatch_file(sbatch_file_path)
        self.assertEqual(job_id, 12345)

    def test_create_sbatch_file(self):
        cl_list = [['echo', 'Hello!']]
        config = {'environment': {'project_id': 'slurm_proj_id'},
                  'slurm': {'extra_params': {'Param': 'Value'}}
                  }
        out_file = os.path.join(self.tmp_dir, 
                                'ANALYSIS',
                                'P123',
                                'qc_ngi',
                                'logs',
                                'P123-P123_1001_sbatch.out')
        err_file = os.path.join(self.tmp_dir, 
                                'ANALYSIS',
                                'P123',
                                'qc_ngi',
                                'logs',
                                'P123-P123_1001_sbatch.err')

        got_file = launchers.create_sbatch_file(cl_list, 
                                                self.project, 
                                                self.sample, 
                                                config)
        with open(got_file, 'r') as file:
            got_content = file.read()

        expected_file = ('data/sbatch_template.sbatch')
        with open(expected_file, 'r') as file:
            expected_content = file.read().format(out_file, err_file)

        self.assertEqual(got_content, expected_content)