import unittest
import mock
import tempfile
import shutil
import os

from ngi_pipeline.conductor.flowcell import match_fastq_sample_number_to_samplesheet, \
                                                organize_projects_from_flowcell, \
                                                    setup_analysis_directory_structure, \
                                                        parse_flowcell

class TestFlowcell(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.tmp_dir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.tmp_dir)

    def test_match_fastq_sample_number_to_samplesheet(self):
        test_samples = {
            'S1': [['S1', 'proj1', 'sample-name', '', '', 1],
                   ['S1', 'proj1', 'sample-name', '', '', 2],
                   ['S1', 'proj1', 'sample-name', '', '', 3],
                   ['S1', 'proj1', 'sample-name', '', '', 4]],
            'S2': [['S2', 'proj1', 'sample-name', '', '', 1],
                   ['S2', 'proj1', 'sample-name', '', '', 2]]
        }
        test_data = [
            ['sample-name_S1_L001_R1_001.fastq.gz', test_samples['S1']+test_samples['S2'], None, test_samples['S1'][0]],
            ['sample-name_S1_L003_R1_001.fastq.gz', test_samples['S1']+test_samples['S2'], None, test_samples['S1'][2]],
            ['sample-name_S1_L001_R1_001.fastq.gz', test_samples['S1']+test_samples['S2'], 'not-proj1', None],
            ['sample-name_S2_L002_R1_001.fastq.gz', test_samples['S1']+test_samples['S2'], None, test_samples['S2'][1]],
            ['not-sample-name_S2_L002_R1_001.fastq.gz', test_samples['S1']+test_samples['S2'], None, None],
            ['sample-name_S2_L003_R1_001.fastq.gz', test_samples['S1']+test_samples['S2'], None, None],
            ['sample-name_S3_L003_R1_001.fastq.gz', test_samples['S1']+test_samples['S2'], None, None],
            ['sample-name_S3_L004_R1_001.fastq.gz', None, None, None],
            ['sample-name_S3_L004_R1_001.fastq.gz', [], None, None],
            ['sample-name_S3_L004_R1_001.fastq.gz', [[]], None, None],
            ['sample-name_SX_L004_R1_001.fastq.gz', test_samples['S1'] + test_samples['S2'], None, None]
        ]
        for test_item in test_data:
            self.assertEqual(
                test_item[3],
                match_fastq_sample_number_to_samplesheet(test_item[0], test_item[1], test_item[2]))


    @mock.patch('ngi_pipeline.conductor.flowcell.locate_flowcell')
    @mock.patch('ngi_pipeline.conductor.flowcell.setup_analysis_directory_structure')
    def test_organize_projects_from_flowcell(self, mock_dir_setup, mock_locate):
        mock_dir_setup.return_value = {'some_dir': 'P12345'}
        demux_fcid_dirs = ['201103_A00187_0332_AHFCFLDSXX']
        expected_projects = ['P12345']
        got_projects = organize_projects_from_flowcell(demux_fcid_dirs)
        self.assertEqual(expected_projects, got_projects)

    @mock.patch('ngi_pipeline.conductor.flowcell.safe_makedir')
    @mock.patch('ngi_pipeline.conductor.flowcell.os.path.exists')
    @mock.patch('ngi_pipeline.conductor.flowcell.parse_flowcell')
    @mock.patch('ngi_pipeline.conductor.flowcell.get_project_id_from_name')
    def test_setup_analysis_directory_structure(self, mock_id, mock_parse, mock_path, mock_makedir):
        fc_dir = '/ngi2016003/201103_A00187_0332_AHFCFLDSXX'
        mock_parse.return_value = {'fc_dir': fc_dir,
                                   'fc_full_id': '201103_A00187_0332_AHFCFLDSXX',
                                   'projects': [{'project_name': 'S.One_20_01',
                                                 'project_original_name': 'something',
                                                 'samples': [{'sample_name': 'one'}]
                                                 }]
                                   }
        mock_id.return_value = 'P12345'
        projects_to_analyze = {}
        expected_project = 'S.One_20_01'
        got_projects = setup_analysis_directory_structure(fc_dir, 
                                                          projects_to_analyze, 
                                                          create_files=False)
        got_project = got_projects['/lupus/ngi/staging/wildwest/ngi2016001/nobackup/NGI/DATA/P12345']
        self.assertEqual(expected_project, got_project.name)

    def test_parse_flowcell(self):

        def _setup_sample_dir(project_dir, sample_id, sample_num, sample_name=None):
            sample_dir = os.path.join(project_dir, f"Sample_{sample_id}")
            os.makedirs(sample_dir, exist_ok=True)
            sample_name = sample_name or f"{sample_id}-S"
            fqfiles = [f"{sample_name}_S{sample_num}_L00{lane}_R{rd}_001.fastq.gz"
                for lane in (1, 2, 3) for rd in (1, 2)]
            for fqfile in fqfiles:
                open(os.path.join(sample_dir, fqfile), 'w').close()
            return {
                "sample_dir": os.path.basename(sample_dir),
                "sample_id": sample_id,
                "sample_name": sample_name,
                "files": fqfiles
            }

        def _setup_project_dir(base_path, project_original_name, samples):
            project_dir = os.path.join(base_path, project_original_name.replace('.', '__'))
            os.makedirs(project_dir, exist_ok=True)
            sample_data = [
                _setup_sample_dir(project_dir, sample["id"], num, sample.get("name"))
                for num, sample in enumerate(samples)]
            return {
                "data_dir": os.path.basename(base_path),
                "project_dir": os.path.basename(project_dir),
                "project_name": project_original_name.replace('__', '.'),
                "project_original_name": project_original_name,
                "samples": sample_data
            }

        def _setup_flowcell_dir(base_path, flowcell):
            fc_dir = os.path.join(self.tmp_dir, flowcell)
            samplesheet_path = os.path.join(fc_dir, 'SampleSheet.csv')

            # Set up tmp dir
            os.mkdir(fc_dir)
            open(samplesheet_path, 'w').close()

            return {
                "fc_dir": fc_dir,
                "fc_full_id": flowcell,
                "samplesheet_path": samplesheet_path,
                "projects": []
            }

        flowcell = '201103_A00187_0332_AHFCFLDSXX'
        fc_data = _setup_flowcell_dir(self.tmp_dir, flowcell)
        data_dir = os.path.join(fc_data["fc_dir"], "Demultiplexing")

        projects = [
            {
                "project_name": "S__One_20_01",
                "samples": [
                    {
                        "id": "P12345_1101"}]},
            {
                "project_name": "T.Two_20_01",
                "samples": [
                    {
                        "id": "P345_1201",
                        "name": "P345_120"},
                    {
                        "id": "P345_1202",
                        "name": "P345_120"
                    }]},
            {
                "project_name": "AB-1234",
                "samples": [
                    {
                        "id": "AB-1234-101",
                        "name": "AB-1234-101"
                    },
                    {
                        "id": "AB-1234-101-2",
                        "name": "AB-1234-101"
                    },
                    {
                        "id": "AB-1234-102"
                    }]}]

        for project in projects:
            fc_data["projects"].append(
                _setup_project_dir(data_dir, project["project_name"], project["samples"]))

        # Get data
        got_info = parse_flowcell(fc_data["fc_dir"])
        self.assertCountEqual(fc_data, got_info)
