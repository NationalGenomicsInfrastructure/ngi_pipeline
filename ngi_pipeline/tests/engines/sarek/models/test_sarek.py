import mock
import os
import shutil
import tempfile
import unittest

from ngi_pipeline.engines.sarek.database import CharonConnector
from ngi_pipeline.engines.sarek.exceptions import BestPracticeAnalysisNotRecognized, SampleNotValidForAnalysisError
from ngi_pipeline.engines.sarek.models.resources import ReferenceGenome
from ngi_pipeline.engines.sarek.models.sample import SarekAnalysisSample
from ngi_pipeline.engines.sarek.models.sarek import SarekAnalysis, SarekGermlineAnalysis
from ngi_pipeline.engines.sarek.parsers import ParserIntegrator
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.tests.engines.sarek.test_launchers import TestLaunchers


@mock.patch("ngi_pipeline.engines.sarek.models.sarek.ReferenceGenome", autospec=True)
@mock.patch("ngi_pipeline.engines.sarek.database.CharonConnector", autospec=True)
class TestSarekAnalysis(unittest.TestCase):

    CONFIG = {
            "nextflow": {
                "config": "this-is-a-site-specific-config-file",
                "profile": "this-is-another-profile",
                "nf_path": "this-is-the-path-to-nextflow"},
            "sarek": {
                "sarek_path": "this-is-the-path-to-sarek",
                "unique-key": "this-is-not-in-the-default-config",
                "tools": ["tool-A", "tool-B"]}
    }

    def setUp(self):
        self.log = minimal_logger(__name__, to_file=False, debug=True)
        self.config = TestSarekAnalysis.CONFIG
        self.analysis_obj = TestLaunchers.get_NGIAnalysis(log=self.log)

    def test_get_analysis_instance_for_project_germline(self, charon_connector_mock, reference_genome_mock):
        reference_genome_mock.get_instance.return_value = reference_genome_mock
        reference_genome_mock.get_genomes_base_path.return_value = "this-is-the-genomes-base-path"
        charon_connector = charon_connector_mock.return_value
        charon_connector.best_practice_analysis.return_value = "wgs_germline"
        charon_connector.analysis_pipeline.return_value = "sarek"
        expected_analysis_class = SarekGermlineAnalysis
        observed_analysis_instance = SarekAnalysis.get_analysis_instance_for_project(
            "this-is-a-project-id",
            self.config,
            self.log,
            charon_connector=charon_connector)
        self.assertIsInstance(observed_analysis_instance, expected_analysis_class)

    def test_get_analysis_instance_for_project_somatic(self, charon_connector_mock, reference_genome_mock):
        reference_genome_mock.get_instance.return_value = "this-is-a-reference-genome"
        charon_connector = charon_connector_mock.return_value
        charon_connector.best_practice_analysis.return_value = "wgs_somatic"
        charon_connector.analysis_pipeline.return_value = "sarek"
        with self.assertRaises(NotImplementedError):
            SarekAnalysis.get_analysis_instance_for_project(
                "this-is-a-project-id",
                self.config,
                self.log,
                charon_connector=charon_connector)

    def test_get_analysis_instance_for_project_unknown(self, charon_connector_mock, reference_genome_mock):
        reference_genome_mock.get_instance.return_value = "this-is-a-reference-genome"
        charon_connector = charon_connector_mock.return_value
        charon_connector.best_practice_analysis.return_value = "something_unknown_something"
        with self.assertRaises(BestPracticeAnalysisNotRecognized):
            SarekAnalysis.get_analysis_instance_for_project(
                "this-is-a-project-id",
                self.config,
                self.log,
                charon_connector=charon_connector)

    def test_status_should_be_started(self, charon_connector_mock, reference_genome_mock):

        def _analysis_status_wrapper(status, *args):
            return CharonConnector(
                self.config, self.log, charon_session="charon-session").analysis_status_from_process_status(status)

        def _alignment_status_wrapper(status, *args):
            return CharonConnector(
                self.config, self.log, charon_session="charon-session").alignment_status_from_analysis_status(status)

        charon_connector = charon_connector_mock.return_value
        charon_connector.analysis_status_from_process_status.side_effect = _analysis_status_wrapper
        charon_connector.alignment_status_from_analysis_status.side_effect = _alignment_status_wrapper
        sarek_analysis = SarekAnalysis(
            reference_genome_mock.return_value, self.config, self.log, charon_connector=charon_connector)

        for restart_mode in True, False:
            self.assertEqual(
                restart_mode,
                sarek_analysis.status_should_be_started(
                    "FAILED",
                    restart_failed_jobs=restart_mode))

        for restart_mode in True, False:
            self.assertEqual(
                restart_mode,
                sarek_analysis.status_should_be_started(
                    "ANALYZED",
                    restart_finished_jobs=restart_mode))

        for restart_mode in True, False:
            self.assertEqual(
                restart_mode,
                sarek_analysis.status_should_be_started(
                    "UNDER_ANALYSIS",
                    restart_running_jobs=restart_mode))

        self.assertTrue(
            sarek_analysis.status_should_be_started("TO_BE_ANALYZED"))

    def test_configure_analysis(self, charon_connector_mock, reference_genome_mock):

        sarek_analysis = SarekAnalysis(
            reference_genome_mock.return_value,
            self.config,
            self.log,
            charon_connector=charon_connector_mock.return_value)

        # assert that the supplied config was split up and the correct key-value pairs were set
        for key in self.config["sarek"].keys():
            self.assertEqual(self.config["sarek"][key], sarek_analysis.sarek_config[key])
        for key in self.config["nextflow"].keys():
            self.assertEqual(self.config["nextflow"][key], sarek_analysis.nextflow_config[key])

    def test_configure_analysis_missing_key(self, charon_connector_mock, reference_genome_mock):

        config = self.config.copy()
        del(config["nextflow"]["profile"])

        sarek_analysis = SarekAnalysis(
            reference_genome_mock.return_value,
            config,
            self.log,
            charon_connector=charon_connector_mock.return_value)

        # assert that the default dict is used if key is missing
        self.assertEqual(
            SarekAnalysis.DEFAULT_CONFIG["nextflow"]["profile"],
            sarek_analysis.nextflow_config["profile"])

    def test_configure_analysis_additional_options(self, charon_connector_mock, reference_genome_mock):

        sarek_analysis = SarekAnalysis(
            reference_genome_mock.return_value,
            self.config,
            self.log,
            charon_connector=charon_connector_mock.return_value)

        # assert that additional options passed are included and overrides the default ones
        extra_options = {
            "nextflow": {
                "unique-key": "this-is-not-in-the-default-config"
            },
            "sarek": {
                "sarek_path": "this-is-the-overridden-path-to-sarek"
            }
        }
        merged_config = sarek_analysis.configure_analysis(opts=extra_options)
        for section in extra_options.keys():
            for key in extra_options[section].keys():
                self.assertEqual(extra_options[section][key], merged_config[section][key])

    def test_configure_analysis_genomes_paths(self, charon_connector_mock, reference_genome_mock):

        # test setting the genomes_base parameter
        expected_path = os.path.join("this", "is", "the", "right", "path")
        config = {
            "genomes_base_paths": {
                "GRCh37": expected_path,
                "GRCh38": os.path.join("this", "is", "the", "wrong", "path")
            }
        }
        sarek_analysis = SarekAnalysis(
            ReferenceGenome.get_instance("GRCh37"),
            {"sarek": config},
            self.log,
            charon_connector=charon_connector_mock.return_value)
        self.assertIn("genomes_base", sarek_analysis.sarek_config)
        self.assertEqual(expected_path, sarek_analysis.sarek_config["genomes_base"])
        self.assertNotIn("genomes_base_paths", sarek_analysis.sarek_config)

    def test_merge_configs(self, *mocks):
        default_config = {
            "A": "this-is-the-default-A",
            "B": "this-is-the-default-B",
            "C": "this-is-the-default-C"
        }
        global_config = {
            "A": "this-is-the-global-A",
            "B": "this-is-the-global-B"
        }
        local_config = {
            "A": "this-is-the-local-A"
        }
        merged_config = SarekAnalysis.merge_configs(
            default_config,
            global_config,
            **local_config)
        self.assertEqual(local_config["A"], merged_config["A"])
        self.assertEqual(global_config["B"], merged_config["B"])
        self.assertEqual(default_config["C"], merged_config["C"])

    def test_command_line(self, charon_connector_mock, reference_genome_mock):

        sarek_analysis = SarekAnalysis(
            reference_genome_mock.return_value,
            self.config,
            self.log,
            charon_connector=charon_connector_mock.return_value)

        with self.assertRaises(NotImplementedError):
            sarek_analysis.command_line(None)

    def test_generate_tsv_file_contents(self, charon_connector_mock, reference_genome_mock):

        sarek_analysis = SarekAnalysis(
            reference_genome_mock.return_value,
            self.config,
            self.log,
            charon_connector=charon_connector_mock.return_value)

        with self.assertRaises(NotImplementedError):
            sarek_analysis.generate_tsv_file_contents(None)

    def test_create_tsv_file(self, charon_connector_mock, reference_genome_mock):
        sarek_analysis = SarekAnalysis(
            reference_genome_mock.return_value,
            self.config,
            self.log,
            charon_connector=charon_connector_mock.return_value)
        sample_obj = self.analysis_obj.project.samples.values()[0]
        analysis_sample = SarekAnalysisSample(self.analysis_obj.project, sample_obj, sarek_analysis)
        with mock.patch.object(sarek_analysis, "generate_tsv_file_contents") as tsv_mock, \
                mock.patch.object(analysis_sample, "sample_analysis_tsv_file") as tsv_path_mock:
            tsv_mock.return_value = []
            with self.assertRaises(SampleNotValidForAnalysisError) as e:
                sarek_analysis.create_tsv_file(analysis_sample)
            tsv_mock.return_value = [["this", "is"], ["some", "content"]]
            tempdir = tempfile.mkdtemp(prefix="test_create_tsv_file_")
            tsv_path_mock.return_value = os.path.join(tempdir, "tsv_parent_folder", "tsv_file.tsv")
            sarek_analysis.create_tsv_file(analysis_sample)
            self.assertTrue(os.path.exists(tsv_path_mock.return_value))
            shutil.rmtree(tempdir)


@mock.patch("ngi_pipeline.engines.sarek.models.resources.ReferenceGenome", autospec=True)
@mock.patch("ngi_pipeline.engines.sarek.database.CharonConnector", autospec=True)
@mock.patch("ngi_pipeline.engines.sarek.database.TrackingConnector", autospec=True)
@mock.patch("ngi_pipeline.engines.sarek.process.ProcessConnector", autospec=True)
class TestSarekGermlineAnalysis(unittest.TestCase):

    CONFIG = {
        "nextflow": {
            "profile": "this-is-a-profile",
            "command": os.path.join("/this", "is", "the", "nextflow", "path"),
            "subcommand": "this-is-the-sub"},
        "sarek": {
            "tools": ["haplotypecaller", "manta"],
            "command": os.path.join("/this", "is", "the", "path", "to", "sarek"),
            "genome": "this-is-the-genome"}}

    def setUp(self):
        self.log = minimal_logger(__name__, to_file=False, debug=True)
        self.config = TestSarekGermlineAnalysis.CONFIG
        self.analysis_obj = TestLaunchers.get_NGIAnalysis(log=self.log)

    def get_instance(
            self, process_connector_mock, tracking_connector_mock, charon_connector_mock, reference_genome_mock):
        reference_genome = reference_genome_mock.return_value
        reference_genome.__str__.return_value = self.config["sarek"]["genome"]
        return SarekGermlineAnalysis(
            reference_genome,
            self.config,
            self.log,
            charon_connector=charon_connector_mock,
            tracking_connector=tracking_connector_mock,
            process_connector=process_connector_mock)

    def test_cleanup(self, *mocks):
        sarek_analysis = self.get_instance(*mocks)
        with mock.patch("ngi_pipeline.engines.sarek.models.sample.SarekAnalysisSample", autospec=True) as sample_mock:
            expected_work_dir = "path/to/work/dir"
            sample_mock.sample_analysis_work_dir.return_value = expected_work_dir
            sarek_analysis.cleanup(sample_mock)
            sarek_analysis.process_connector.cleanup.assert_called_once_with(expected_work_dir)

    def test_command_line(
            self, process_connector_mock, tracking_connector_mock, charon_connector_mock, reference_genome_mock):
        sarek_analysis = self.get_instance(
            process_connector_mock, tracking_connector_mock, charon_connector_mock, reference_genome_mock)
        sample_obj = self.analysis_obj.project.samples.values()[0]
        analysis_sample = SarekAnalysisSample(self.analysis_obj.project, sample_obj, sarek_analysis)
        self.config["outDir"] = analysis_sample.sample_analysis_path()
        self.config["input"] = analysis_sample.sample_analysis_tsv_file()
        observed_cmd = sarek_analysis.command_line(analysis_sample)

        self.assertIn("--tools {}".format(",".join(self.config["sarek"]["tools"])), observed_cmd)
        self.assertTrue(observed_cmd.startswith(self.config["nextflow"]["command"]))
        for key in filter(lambda x: x not in ["command", "subcommand"], self.config["nextflow"].keys()):
            self.assertIn("-{} {}".format(key, self.config["nextflow"][key]), observed_cmd)
        for key in filter(lambda x: x not in ["command", "tools"], self.config["sarek"].keys()):
            self.assertIn("--{} {}".format(key, self.config["sarek"][key]), observed_cmd)
        self.assertNotIn("-command", observed_cmd)
        self.assertNotIn("-subcommand", observed_cmd)
        self.assertNotIn("--command", observed_cmd)

    def test_generate_tsv_file_contents(
            self, process_connector_mock, tracking_connector_mock, charon_connector_mock, reference_genome_mock):
        sarek_analysis = self.get_instance(
            process_connector_mock, tracking_connector_mock, charon_connector_mock, reference_genome_mock)
        sample_obj = self.analysis_obj.project.samples.values()[0]
        analysis_sample = SarekAnalysisSample(self.analysis_obj.project, sample_obj, sarek_analysis)
        sample_data_path = analysis_sample.sample_data_path()
        with mock.patch.object(
                sarek_analysis, "libprep_should_be_started") as libprep_mock, \
                mock.patch.object(
                    sarek_analysis, "seqrun_should_be_started") as seqrun_mock:
            for start_mode in True, False:
                libprep_mock.return_value = start_mode
                seqrun_mock.return_value = not start_mode
                self.assertListEqual(
                    [],
                    sarek_analysis.generate_tsv_file_contents(analysis_sample))
            libprep_mock.return_value = True
            seqrun_mock.return_value = True
            expected_tsv = []
            for libprepid in map(lambda lp: '{}-{}'.format(sample_obj.name, lp), ['libprep1', 'libprep2']):
                for sample_index in ['1', '2']:
                    tsv_row = [sample_obj.name, 'ZZ', 0, sample_obj.name, 'ABC00{0}CXY.1.S{0}'.format(sample_index)]
                    tsv_row.extend([
                        os.path.join(
                            sample_data_path,
                            libprepid,
                            '180411_ST-0123_001{0}_AABC00{0}CXY'.format(sample_index),
                            '{}_S{}_L001_R{}_001.fastq.gz'.format(
                                libprepid, sample_index, read_num)) for read_num in ['1', '2']])
                    expected_tsv.append(tsv_row)
            observed_tsv = sarek_analysis.generate_tsv_file_contents(analysis_sample)
            self.assertListEqual(sorted(expected_tsv), sorted(observed_tsv))

    def test_analyze_sample(
            self, process_connector_mock, tracking_connector_mock, charon_connector_mock, reference_genome_mock):
        sarek_analysis = self.get_instance(
            process_connector_mock, tracking_connector_mock, charon_connector_mock, reference_genome_mock)
        with mock.patch.object(
                sarek_analysis, "create_tsv_file", return_value=os.path.join("/path", "to", "tsv", "file")) as tsv_mock:
            for sample_obj in self.analysis_obj.project:
                sarek_analysis.analyze_sample(sample_obj, self.analysis_obj)

    def test_runid_and_fastq_files_from_tsv_file(self, *args):
        fh, fake_tsv_file = tempfile.mkstemp(prefix="test_fastq_files_from_tsv_")
        with mock.patch("ngi_pipeline.engines.sarek.models.sarek.csv.reader", autospec=True) as csv_mock:
            expected_fastq_files = []
            for row in range(3):
                expected_fastq_files.append(
                    ["id.for.row.{}".format(row)] + [
                        os.path.join("/path", "to", "fastq", "file_{}-R{}.fastq.gz".format(row, i)) for i in [1, 2]])
            for row in range(2):
                expected_fastq_files.append(
                    ["id.for.row.{}".format(row+3)] +
                    [os.path.join("/path", "to", "fastq", "file_{}-R1.fastq.gz".format(row))])
            mocked_tsv_data = (
                ["col1", "col2", "col3", "col4"] +
                runid_and_fastq_files for runid_and_fastq_files in expected_fastq_files)
            csv_mock.return_value = mocked_tsv_data
            observed_fastq_files = list(SarekGermlineAnalysis.runid_and_fastq_files_from_tsv_file(fake_tsv_file))
            self.assertListEqual(sorted(expected_fastq_files), sorted(observed_fastq_files))
        os.unlink(fake_tsv_file)

    def test_collect_analysis_metrics(
            self, process_connector_mock, tracking_connector_mock, charon_connector_mock, reference_genome_mock):
        sarek_analysis = self.get_instance(
            process_connector_mock, tracking_connector_mock, charon_connector_mock, reference_genome_mock)
        expected_metrics = {
            "percent_duplication": [25.3],
            "autosomal_coverage": [35.8],
            "total_reads": [123456789]
        }

        def _serve_metric(metric):
            return expected_metrics[metric[4:]]

        with mock.patch.object(
                sarek_analysis, "processing_steps") as processing_steps, \
                mock.patch(
                    "ngi_pipeline.engines.sarek.models.sarek.ParserIntegrator", autospec=ParserIntegrator) as \
                        parser_mock:
            processing_steps.return_value = []
            parser_instance = parser_mock.return_value
            query_mock = parser_instance.query_parsers
            query_mock.side_effect = _serve_metric
            observed_metrics = sarek_analysis.collect_analysis_metrics("this-is-a-sample-analysis")
            self.assertDictEqual({metric: value[0] for metric, value in expected_metrics.items()}, observed_metrics)
