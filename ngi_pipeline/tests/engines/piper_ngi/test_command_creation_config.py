import mock
import unittest
import tempfile
import os
from ngi_pipeline.engines.piper_ngi.command_creation_config import (
    build_piper_cl,
    build_setup_xml,
)
from ngi_pipeline.engines.piper_ngi.launchers import analyze
from ngi_pipeline import conductor
from ngi_pipeline.conductor.classes import NGIProject, NGIAnalysis
from ngi_pipeline.utils.config import load_yaml_config, locate_ngi_config


class TestCommandCreation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.proj_name = "Y.Mom_15_01"
        cls.proj_id = "P1155"
        cls.libprep_id = "P1155_prepA"
        cls.seqrun_id = "P1155_seqrunA"
        cls.sample_name = "P1155_101"
        cls.engine_name = "piper_ngi"
        cls.proj_basepath = tempfile.mkdtemp()
        cls.workflow_name = "merge_process_variantcall"
        cls.xml_path = os.path.join(cls.proj_basepath, "some_config.xml")
        cls.exit_file = os.path.join(cls.proj_basepath, "some_file.exit")
        cls.config = load_yaml_config(locate_ngi_config())
        cls.project_obj = NGIProject(
            name=cls.proj_name,
            dirname=cls.proj_name,
            project_id=cls.proj_id,
            base_path=cls.proj_basepath,
        )
        cls.sample_obj = cls.project_obj.add_sample(
            name=cls.sample_name, dirname=cls.sample_name
        )
        # create a mock that can replace calls to charon
        cls.charon_mock = mock.Mock()
        cls.charon_mock.sample_get_libpreps = mock.Mock(
            return_value={"libpreps": [{"qc": "PASSED", "libprepid": cls.libprep_id}]}
        )
        cls.charon_mock.libprep_get_seqruns = mock.Mock(
            return_value={"seqruns": [{"seqrunid": cls.seqrun_id}]}
        )
        cls.charon_mock.seqrun_get = mock.Mock(return_value={"alignment_status": ""})
        cls.charon_mock.project_get = mock.Mock(
            return_value={"sequencing_facility": "Unknown"}
        )

    def test_build_setup_xml(self):
        with mock.patch(
            "ngi_pipeline.engines.piper_ngi.command_creation_config.CharonSession",
            spec=False,
            return_value=self.charon_mock,
        ) as dbmock:
            setup_xml_cl, output_xml_filepath = build_setup_xml(
                project=self.project_obj,
                sample=self.sample_obj,
                workflow=self.workflow_name,
                local_scratch_mode=True,
                config=self.config,
            )
        expected_filepath = os.path.join(
            self.proj_basepath,
            "ANALYSIS",
            self.proj_name,
            "piper_ngi",
            "setup_xml_files",
            "{}-{}-{}-setup.xml".format(
                self.proj_name, self.sample_name, self.workflow_name
            ),
        )
        expected_list = ["setupFileCreator"]
        expected_list.append("--output {}".format(expected_filepath))
        expected_list.append("--project_name {}".format(self.proj_name))
        expected_list.append("--sequencing_platform Illumina")
        expected_list.append("--sequencing_center Unknown")
        expected_list.append(
            "--uppnex_project_id {}".format(self.config["environment"]["project_id"])
        )
        expected_list.append(
            "--reference {}".format(self.config["supported_genomes"]["GRCh37"])
        )
        qos = self.config.get("slurm", {}).get("extra_params", {}).get("--qos")
        if qos:
            expected_list.append("--qos {}".format(qos))
        expected_cl = " ".join(expected_list)

        self.assertEquals(setup_xml_cl, expected_cl)
        self.assertEquals(output_xml_filepath, expected_filepath)

    def test_build_piper_cl(self):
        def _validate_cl(tcl):
            assert tcl[0] == "piper"
            assert "--xml_input" in tcl
            assert tcl[tcl.index("--xml_input") + 1] == self.xml_path
            assert "--output_directory" in tcl
            assert tcl[tcl.index("--output_directory") + 1] == os.path.join(
                "$SNIC_TMP", "ANALYSIS", self.proj_name, "piper_ngi"
            )
            assert "--variant_calling" in tcl

        kwargs = {
            "project": self.project_obj,
            "workflow_name": self.workflow_name,
            "setup_xml_path": self.xml_path,
            "exit_code_path": self.exit_file,
            "config": self.config,
            "exec_mode": "sbatch",
        }

        cl = build_piper_cl(**kwargs).split(" ")
        _validate_cl(cl)
        assert "--keep_pre_bqsr_bam" in cl
