import unittest
import mock
import tempfile
import shutil
import os

import ngi_pipeline.engines.piper_ngi.local_process_tracking as tracking


class TestLocalProcessTracking(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.config = {
            "database": {
                "record_tracking_db_path": "{}/record_tracking_database.sql".format(
                    self.tmp_dir
                )
            }
        }
        self.workflow_name = "workflow"
        self.project_base_path = self.tmp_dir
        self.project_name = "S.One"
        self.project_id = "P123"
        self.sample_id = "P123_1001"

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.tmp_dir)

    @mock.patch(
        "ngi_pipeline.engines.piper_ngi.local_process_tracking.parse_genotype_concordance"
    )
    @mock.patch("ngi_pipeline.engines.piper_ngi.local_process_tracking.CharonSession")
    def test_update_gtc_for_sample(self, mock_charon, mock_parse):
        mock_parse.return_value = {"P123_1001": 0.9}
        piper_gtc_path = "gt_path"
        tracking.update_gtc_for_sample(self.project_id, self.sample_id, piper_gtc_path)
        mock_charon().sample_update.assert_called_once_with(
            genotype_concordance=0.9, projectid="P123", sampleid="P123_1001"
        )

    @mock.patch(
        "ngi_pipeline.engines.piper_ngi.local_process_tracking.parse_deduplication_percentage"
    )
    @mock.patch(
        "ngi_pipeline.engines.piper_ngi.local_process_tracking.parse_qualimap_coverage"
    )
    @mock.patch(
        "ngi_pipeline.engines.piper_ngi.local_process_tracking.parse_qualimap_reads"
    )
    @mock.patch("ngi_pipeline.engines.piper_ngi.local_process_tracking.CharonSession")
    def test_update_sample_duplication_and_coverage(
        self, mock_charon, mock_reads, mock_cov, mock_dup
    ):
        mock_dup.return_value = 50
        mock_cov.return_value = 0.5
        mock_reads.return_value = 100

        tracking.update_sample_duplication_and_coverage(
            self.project_id, self.sample_id, self.project_base_path
        )
        mock_charon().sample_update.assert_called_once_with(
            duplication_pc=50,
            projectid="P123",
            sampleid="P123_1001",
            total_autosomal_coverage=0.5,
            total_sequenced_reads=100,
        )

    @mock.patch(
        "ngi_pipeline.engines.piper_ngi.local_process_tracking.get_finished_seqruns_for_sample"
    )
    @mock.patch("ngi_pipeline.engines.piper_ngi.local_process_tracking.CharonSession")
    @mock.patch(
        "ngi_pipeline.engines.piper_ngi.local_process_tracking.parse_mean_coverage_from_qualimap"
    )
    @mock.patch(
        "ngi_pipeline.engines.piper_ngi.local_process_tracking.parse_qualimap_reads"
    )
    def test_update_coverage_for_sample_seqruns(
        self, mock_reads, mock_cov, mock_charon, mock_get_seqruns
    ):
        mock_get_seqruns.return_value = {"libprep_01": ["seqrun_01"]}
        mock_cov.return_value = 50
        mock_reads.return_value = 100

        piper_qc_dir = os.path.join(self.tmp_dir, "02_preliminary_alignment_qc")
        os.mkdir(piper_qc_dir)
        seqrun_location = os.path.join(piper_qc_dir, "P123_1001.01.qc")
        os.mkdir(seqrun_location)
        genome_results = os.path.join(seqrun_location, "genome_results.txt")
        open(genome_results, "w").close()

        tracking.update_coverage_for_sample_seqruns(
            self.project_id, self.sample_id, piper_qc_dir
        )
        mock_charon().seqrun_update.assert_called_once_with(
            libprepid="libprep_01",
            mean_autosomal_coverage=50,
            projectid="P123",
            sampleid="P123_1001",
            seqrunid="seqrun_01",
            total_reads=100,
        )

    @mock.patch(
        "ngi_pipeline.engines.piper_ngi.local_process_tracking.create_exit_code_file_path"
    )
    def test_get_exit_code(self, mock_path):
        exit_file = os.path.join(self.tmp_dir, "file.exit")
        with open(exit_file, "w") as f:
            f.write("0")
        mock_path.return_value = exit_file
        got_exit_code = tracking.get_exit_code(
            self.workflow_name,
            self.project_base_path,
            self.project_name,
            self.project_id,
            self.sample_id,
        )
        self.assertEqual(got_exit_code, 0)
