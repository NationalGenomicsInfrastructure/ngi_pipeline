import unittest
import mock
import os

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.database.classes import CharonError
from ngi_pipeline.database.filesystem import create_charon_entries_from_project


class TestCharonFunctions(unittest.TestCase):
    def setUp(self):
        # Details
        self.project_id = "P100001"
        self.project_name = "S.One_20_02"
        self.project_path = "/some/path"
        self.sample_id = "P100001_101"
        self.libprep_id = "A"
        self.seqrun_id = "201030_A00187_0332_AHFCFLDSXX"

        # Objects
        self.project_obj = NGIProject(
            name=self.project_name,
            dirname=self.project_name,
            project_id=self.project_id,
            base_path=self.project_path,
        )
        self.sample_obj = self.project_obj.add_sample(
            name=self.sample_id, dirname=self.sample_id
        )
        self.libprep_obj = self.sample_obj.add_libprep(
            name=self.libprep_id, dirname=self.libprep_id
        )
        self.seqrun_obj = self.libprep_obj.add_seqrun(
            name=self.seqrun_id, dirname=self.seqrun_id
        )

    @mock.patch.dict(
        os.environ, {"CHARON_BASE_URL": "charon-url", "CHARON_API_TOKEN": "token"}
    )
    @mock.patch("ngi_pipeline.database.filesystem.CharonSession.project_create")
    @mock.patch("ngi_pipeline.database.filesystem.CharonSession.sample_create")
    @mock.patch("ngi_pipeline.database.filesystem.CharonSession.libprep_create")
    @mock.patch("ngi_pipeline.database.filesystem.CharonSession.seqrun_create")
    def test_create_charon_entries_from_project(
        self, mock_seqrun, mock_libprep, mock_sample, mock_proj
    ):
        create_charon_entries_from_project(self.project_obj)
        mock_proj.assert_called_once_with(
            best_practice_analysis="whole_genome_reseq",
            name="S.One_20_02",
            projectid="P100001",
            sequencing_facility="NGI-S",
            status="OPEN",
        )
        mock_sample.assert_called_once_with(
            analysis_status="TO_ANALYZE", projectid="P100001", sampleid="P100001_101"
        )
        mock_libprep.assert_called_once_with(
            libprepid="A", projectid="P100001", qc="PASSED", sampleid="P100001_101"
        )
        mock_seqrun.assert_called_once_with(
            alignment_status="NOT_RUNNING",
            libprepid="A",
            mean_autosomal_coverage=0,
            projectid="P100001",
            sampleid="P100001_101",
            seqrunid="201030_A00187_0332_AHFCFLDSXX",
            total_reads=0,
        )

    @mock.patch.dict(
        os.environ, {"CHARON_BASE_URL": "charon-url", "CHARON_API_TOKEN": "token"}
    )
    @mock.patch("ngi_pipeline.database.filesystem.CharonSession.project_create")
    @mock.patch("ngi_pipeline.database.filesystem.CharonSession.project_update")
    @mock.patch("ngi_pipeline.database.filesystem.CharonSession.sample_create")
    @mock.patch("ngi_pipeline.database.filesystem.CharonSession.sample_update")
    @mock.patch("ngi_pipeline.database.filesystem.CharonSession.libprep_create")
    @mock.patch("ngi_pipeline.database.filesystem.CharonSession.libprep_update")
    @mock.patch("ngi_pipeline.database.filesystem.CharonSession.seqrun_create")
    @mock.patch("ngi_pipeline.database.filesystem.CharonSession.seqrun_update")
    def test_create_charon_entries_from_project_update(
        self,
        mock_seqrun_ud,
        mock_seqrun_cr,
        mock_libprep_ud,
        mock_libprep_cr,
        mock_sample_ud,
        mock_sample_cr,
        mock_project_ud,
        mock_project_cr,
    ):
        # Not the neatest of tests but gets the job done...
        mock_project_cr.side_effect = CharonError("Error", status_code=400)
        mock_sample_cr.side_effect = CharonError("Error", status_code=400)
        mock_libprep_cr.side_effect = CharonError("Error", status_code=400)
        mock_seqrun_cr.side_effect = CharonError("Error", status_code=400)

        create_charon_entries_from_project(self.project_obj, force_overwrite=True)

        mock_project_ud.assert_called_once_with(
            best_practice_analysis="whole_genome_reseq",
            name="S.One_20_02",
            projectid="P100001",
            sequencing_facility="NGI-S",
            status="OPEN",
        )
        mock_sample_ud.assert_called_once_with(
            analysis_status="TO_ANALYZE",
            projectid="P100001",
            sampleid="P100001_101",
            status="STALE",
        )
        mock_libprep_ud.assert_called_once_with(
            libprepid="A", projectid="P100001", qc="PASSED", sampleid="P100001_101"
        )
        mock_seqrun_ud.assert_called_once_with(
            alignment_status="NOT_RUNNING",
            libprepid="A",
            mean_autosomal_coverage=0,
            projectid="P100001",
            sampleid="P100001_101",
            seqrunid="201030_A00187_0332_AHFCFLDSXX",
            total_reads=0,
        )
