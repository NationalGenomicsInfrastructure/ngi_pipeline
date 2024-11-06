import unittest
import mock
import tempfile
import shutil
import os

import ngi_pipeline.engines.qc_ngi.workflows as workflows


class TestWorkflows(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.tmp_dir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.tmp_dir)

    @mock.patch("ngi_pipeline.engines.qc_ngi.workflows.workflow_qc")
    def test_return_cls_for_workflow(self, mock_qc):
        config = {"A": "B"}
        mock_qc.return_value = ["command", "--option"]
        workflow_name = "qc"
        input_files = ["file", "another_file"]
        output_dir = "output"

        command_line_list = workflows.return_cls_for_workflow(
            workflow_name, input_files, output_dir, config=config
        )
        self.assertEqual(command_line_list, [["command", "--option"]])
        mock_qc.assert_called_once_with(input_files, output_dir, config)

        with self.assertRaises(NotImplementedError):
            missing_workflow_name = "something"
            command_line_list = workflows.return_cls_for_workflow(
                missing_workflow_name, input_files, output_dir, config=config
            )

    @mock.patch("ngi_pipeline.engines.qc_ngi.workflows.workflow_fastqc")
    @mock.patch("ngi_pipeline.engines.qc_ngi.workflows.workflow_fastq_screen")
    def test_workflow_qc(self, mock_fastq_screen, mock_fastqc):
        config = {"qc": {"make_md5": True}}
        mock_fastqc.return_value = ["fastqc", "--option1"]
        mock_fastq_screen.return_value = ["fastq_screen", "--option2"]
        input_files = ["file", "another_file"]
        output_dir = "output"
        command_line_list = workflows.workflow_qc(input_files, output_dir, config)
        expected_command_line_list = [
            ["fastqc", "--option1"],
            ["fastq_screen", "--option2"],
            [
                'files_for_md5="file another_file"',
                "for fl in $files_for_md5; "
                "do md5sum $fl | "
                "awk '{printf $1}' > "
                "${fl}.md5; done",
            ],
        ]
        self.assertEqual(command_line_list, expected_command_line_list)

    def test_workflow_fastqc(self):
        config = {"paths": {"fastqc": "/path/to/fastqc"}, "qc": {"load_modules": ["A"]}}
        dummy_fastq = os.path.join(
            self.tmp_dir, "201111_A00187_0335_AHFCFLDSXX", "P123_1001.fastq.gz"
        )
        input_files = [dummy_fastq]
        output_dir = os.path.join(self.tmp_dir, "fastqc")
        output_fastq = os.path.join(output_dir, "P123_1001_AHFCFLDSXX.fastq.gz")

        command_line_list = workflows.workflow_fastqc(input_files, output_dir, config)
        expected_command_line_list = [
            "module load A",
            "ln -s {} {}".format(dummy_fastq, output_fastq),
            "/path/to/fastqc -t 1 -o {} {}".format(output_dir, output_fastq),
            "rm {}".format(output_fastq),
        ]

        self.assertEqual(command_line_list, expected_command_line_list)

    def test_workflow_fastq_screen(self):
        fastq_screen_conf = os.path.join(self.tmp_dir, "fastq_screen_conf.yaml")
        open(fastq_screen_conf, "w").close()
        config = {
            "paths": {"fastq_screen": "/path/to/fastq_screen"},
            "qc": {
                "load_modules": ["B"],
                "fastq_screen": {
                    "config_path": fastq_screen_conf,
                    "subsample_reads": 1,
                },
            },
        }

        dummy_fastq = os.path.join(
            self.tmp_dir, "201111_A00187_0335_AHFCFLDSXX", "P123_1001.fastq.gz"
        )
        input_files = [dummy_fastq]
        output_dir = os.path.join(self.tmp_dir, "fastq_screen")
        output_fastq = os.path.join(output_dir, "P123_1001_AHFCFLDSXX.fastq.gz")

        command_line_list = workflows.workflow_fastq_screen(
            input_files, output_dir, config
        )
        expected_command_line_list = [
            "module load B",
            "ln -s {} {}".format(dummy_fastq, output_fastq),
            "/path/to/fastq_screen --aligner bowtie2 --outdir {} --subset 1 --threads 1 --conf {} {}".format(
                output_dir, fastq_screen_conf, output_fastq
            ),
            "rm {}".format(output_fastq),
        ]

        self.assertEqual(command_line_list, expected_command_line_list)

    def test_fastq_to_be_analysed(self):
        dummy_fastq = os.path.join(
            self.tmp_dir, "201111_A00187_0335_AHFCFLDSXX", "P123_1001.fastq.gz"
        )
        fastq_files = [dummy_fastq]
        analysis_dir = os.path.join(self.tmp_dir, "ANALYSIS")
        output_footers = ("{}_fastqc.zip", "{}_fastqc.html")

        found_fastq = workflows.fastq_to_be_analysed(
            fastq_files, analysis_dir, output_footers
        )
        expected_fastq = [
            [dummy_fastq, os.path.join(analysis_dir, "P123_1001_AHFCFLDSXX.fastq.gz")]
        ]

        self.assertEqual(found_fastq, expected_fastq)

    def test_get_all_modules_for_workflow(self):
        binary_name = "some_bin"
        config = {
            "qc": {"load_modules": ["A", "B"], "some_bin": {"load_modules": ["C"]}}
        }

        got_modules = workflows.get_all_modules_for_workflow(binary_name, config)
        expected_modules = ["A", "B", "C"]

        self.assertEqual(got_modules, expected_modules)

    @mock.patch("ngi_pipeline.engines.qc_ngi.workflows.load_modules")
    @mock.patch("ngi_pipeline.engines.qc_ngi.workflows.subprocess.check_call")
    def test_find_on_path(self, mock_subprocess, mock_load):
        mock_subprocess.side_effect = [None, OSError("Error")]
        config = {
            "qc": {"load_modules": ["A", "B"], "some_bin": {"load_modules": ["C"]}}
        }
        binary_name = "some_bin"

        in_path = workflows.find_on_path(binary_name, config=config)
        self.assertTrue(in_path)

        in_path = workflows.find_on_path(binary_name, config=config)
        self.assertFalse(in_path)
