import unittest
import mock

import ngi_pipeline.engines.piper_ngi.workflows as workflows


class TestWorkflows(unittest.TestCase):

    def test_get_subtasks_for_level(self):
        sample_subtask = workflows.get_subtasks_for_level('sample')
        self.assertEqual(sample_subtask, ('merge_process_variantcall',))

        genotype_subtask = workflows.get_subtasks_for_level('genotype')
        self.assertEqual(genotype_subtask, ('genotype_concordance',))

        unknown_subtask = workflows.get_subtasks_for_level('unkown')
        self.assertEqual(unknown_subtask, [])

    @mock.patch('ngi_pipeline.engines.piper_ngi.workflows.workflow_genotype_concordance')
    def test_return_cl_for_workflow(self, mock_workflow):
        mock_workflow.return_value = 'some_command --option'
        workflow_name = 'genotype_concordance'
        qscripts_dir_path = 'path'
        setup_xml_path = 'other/path'
        config = {'some': 'config'}
        got_command_line = workflows.return_cl_for_workflow(workflow_name, 
                                                            qscripts_dir_path, 
                                                            setup_xml_path, 
                                                            config=config)
        expected_command_line = 'some_command --option'
        self.assertEqual(got_command_line, expected_command_line)
        mock_workflow.assert_called_once_with(config={'some': 'config'}, 
                                              exec_mode='local', 
                                              genotype_file=None, 
                                              output_dir=None, 
                                              qscripts_dir_path='path', 
                                              setup_xml_path='other/path')

        with self.assertRaises(NotImplementedError):
            missing_workflow_name = 'not_implemented'
            got_command_line = workflows.return_cl_for_workflow(missing_workflow_name, 
                                                                qscripts_dir_path, 
                                                                setup_xml_path)

    def test_workflow_merge_process_variantcall(self):
        qscripts_dir_path = 'path'
        setup_xml_path = 'other/path'
        config = {'slurm': {'time': '1-00:00:00'},
                  'piper': {'threads': 2,
                            'jobNative': ['A', 'B'],
                            'gatk_key' : 'gatk_file.txt'
                            }
                  }        
        exec_mode = 'sbatch'
        output_dir = 'path'
        got_command_line = workflows.workflow_merge_process_variantcall(qscripts_dir_path, 
                                                                        setup_xml_path,
                                                                        config, 
                                                                        exec_mode, 
                                                                        output_dir=output_dir)

        expected_command_line = ('piper -Djava.io.tmpdir=$SNIC_TMP/java_tempdir '
                                 '-S path/DNABestPracticeVariantCalling.scala '
                                 '--xml_input other/path '
                                 '--global_config $PIPER_CONF '
                                 '--number_of_threads 2 '
                                 '--scatter_gather 1 '
                                 '--job_scatter_gather_directory $SNIC_TMP/scatter_gather '
                                 '--temp_directory $SNIC_TMP/piper_tempdir '
                                 '--run_directory $SNIC_TMP/piper_rundir '
                                 '-jobRunner ParallelShell '
                                 '--super_charge '
                                 '--ways_to_split 4 '
                                 '--job_walltime 86400 '
                                 '--disableJobReport '
                                 '-run '
                                 '--skip_recalibration '
                                 '--output_directory path '
                                 '-jobNative A B '
                                 '--variant_calling '
                                 '--analyze_separately '
                                 '--retry_failed 2 '
                                 '--keep_pre_bqsr_bam')
        self.assertEqual(got_command_line, expected_command_line)

    def test_workflow_dna_variantcalling(self):
        qscripts_dir_path = 'path'
        setup_xml_path = 'other/path'
        config = {'slurm': {'time': '1-00:00:00'},
                  'piper': {'threads': 2,
                            'jobNative': ['A', 'B'],
                            'gatk_key' : 'gatk_file.txt'
                            }
                  }        
        exec_mode = 'sbatch'
        output_dir = 'path'
        got_command_line = workflows.workflow_dna_variantcalling(qscripts_dir_path, 
                                                                 setup_xml_path,
                                                                 config, 
                                                                 exec_mode, 
                                                                 output_dir=output_dir)

        expected_command_line = ('piper -Djava.io.tmpdir=$SNIC_TMP/java_tempdir '
                                 '-S path/DNABestPracticeVariantCalling.scala '
                                 '--xml_input other/path '
                                 '--global_config $PIPER_CONF '
                                 '--number_of_threads 2 '
                                 '--scatter_gather 1 '
                                 '--job_scatter_gather_directory $SNIC_TMP/scatter_gather '
                                 '--temp_directory $SNIC_TMP/piper_tempdir '
                                 '--run_directory $SNIC_TMP/piper_rundir '
                                 '-jobRunner ParallelShell '
                                 '--super_charge '
                                 '--ways_to_split 4 '
                                 '--job_walltime 86400 '
                                 '--disableJobReport '
                                 '-run '
                                 '--skip_recalibration '
                                 '--output_directory path '
                                 '-jobNative A B')
        self.assertEqual(got_command_line, expected_command_line)