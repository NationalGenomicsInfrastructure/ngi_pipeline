import mock
import os
import unittest

from ngi_pipeline.conductor.classes import NGIAnalysis, NGIProject
from ngi_pipeline.log.loggers import minimal_logger
from six.moves import map


class TestLaunchers(unittest.TestCase):

    CONFIG = {
        "analysis": {
            "best_practice_analysis": {
                "wgs_germline": {
                    "analysis_engine": "ngi_pipeline.engines.sarek"
                }
            }
        }
    }

    @staticmethod
    def add_seqruns(libprep):
        for name in map(str, list(range(1, 3))):
            seqrun_name = "180411_ST-0123_001{}_AABC00{}CXY".format(name, name)
            seqrun = libprep.add_seqrun(seqrun_name, seqrun_name)
            seqrun.add_fastq_files(["{}_S{}_L001_R{}_001.fastq.gz".format(libprep.name, name, i) for i in range(1, 3)])
            seqrun.add_fastq_files(["{}_S{}_L001_I1_001.fastq.gz".format(libprep.name, name)])

    @staticmethod
    def add_libpreps(sample):
        for name in map(str, list(range(1, 3))):
            libprep_name = "{}-libprep{}".format(sample.name, name)
            libprep = sample.add_libprep(libprep_name, libprep_name)
            TestLaunchers.add_seqruns(libprep)

    @staticmethod
    def add_samples(project):
        for name in map(str, list(range(1, 3))):
            sample_name = "{}-sample{}".format(project.name, name)
            sample = project.add_sample(sample_name, sample_name)
            TestLaunchers.add_libpreps(sample)

    @staticmethod
    def get_NGIProject(n):
        name = "{}_{}".format(NGIProject.__name__, n)
        project = NGIProject(
            name, "{}".format(name), "{}".format(name), os.path.join("/path", "to", name, "base"))
        TestLaunchers.add_samples(project)
        return project

    @staticmethod
    def get_NGIAnalysis(best_practice_analysis="wgs_germline", config=None, log=None):
        project = TestLaunchers.get_NGIProject(1)
        with mock.patch("ngi_pipeline.conductor.classes.CharonSession", autospec=True) as CharonSessionMock:
            charon_mock = CharonSessionMock.return_value
            charon_mock.project_get.return_value = {"best_practice_analysis": best_practice_analysis}
            return NGIAnalysis(
                project,
                config=(config or TestLaunchers.CONFIG),
                log=(log or minimal_logger(__name__, to_file=False, debug=True)))

    def setUp(self):
        self.config = TestLaunchers.CONFIG
        self.log = minimal_logger(__name__, to_file=False, debug=True)
        self.analysis_object = TestLaunchers.get_NGIAnalysis(config=self.config, log=self.log)

    def test_analyze(self):
        with mock.patch("ngi_pipeline.engines.sarek.models.sarek.SarekAnalysis", autospec=True) as SarekAnalysisMock:
            pass