import os
import unittest

from ngi_pipeline.engines.sarek.exceptions import ReferenceGenomeNotRecognized
from ngi_pipeline.engines.sarek.models.resources import ReferenceGenome, Runfolder, SampleFastq


class TestReferenceGenome(unittest.TestCase):

    def test_get_instance(self):
        self.assertEqual("GRCh37", str(ReferenceGenome.get_instance("GRCh37")))
        self.assertEqual("GRCh38", str(ReferenceGenome.get_instance("GRCh38")))
        with self.assertRaises(ReferenceGenomeNotRecognized):
            ReferenceGenome.get_instance("unknown")

    def test_get_genomes_base_path(self):
        genomes_base_paths = {
            "GRCh37": os.path.join("this", "is", "the", "path", "to", "GRCh37"),
            "GRCh38": os.path.join("this", "is", "the", "path", "to", "GRCh38")
        }
        for ref, expected_path in genomes_base_paths.items():
            self.assertEqual(
                expected_path,
                ReferenceGenome.get_instance(ref).get_genomes_base_path(
                    {"genomes_base_paths": genomes_base_paths}))
        self.assertIsNone(
            ReferenceGenome.get_instance("GRCh37").get_genomes_base_path({}))


class TestSampleFastq(unittest.TestCase):

    def setUp(self):
        self.sample_name = "AB-0123_Sample-456"
        self.sample_number = "S2"
        self.lane_number = "3"
        self.read_number = "2"
        self.file_extension = ".fastq.gz"

    def get_file_name(self):
        return "_".join([
            self.sample_name,
            self.sample_number,
            "L00{}".format(self.lane_number),
            "R{}".format(self.read_number),
            "001{}".format(self.file_extension)])

    def test_split_filename(self):
        fastq_file_name = self.get_file_name()
        fastq_file_path = os.path.join("/path", "to")
        sample_fastq = SampleFastq(os.path.join(fastq_file_path, fastq_file_name))
        self.assertEqual(self.sample_name, sample_fastq.sample_name)
        self.assertEqual(self.sample_number, sample_fastq.sample_number)
        self.assertEqual(self.lane_number, sample_fastq.lane_number)
        self.assertEqual(self.read_number, sample_fastq.read_number)
        self.assertEqual(fastq_file_path, sample_fastq.dirname)
        self.assertEqual(fastq_file_name, sample_fastq.filename)

    def test__sample_fastq_file_pair_sorting(self, *args):
        sample_numbers = {
            "S03": "sample_A",
            "S1": "sample_A",
            "S12": "sample_A",
            "S2": "sample_A",
            "S3": "sample_B"}
        lane_numbers = ["2", "4", "5"]
        sample_fastq_files = {
            "1": [],
            "2": []}
        for sample_number in sorted(sample_numbers.keys()):
            sample_name = sample_numbers[sample_number]
            for lane_number in lane_numbers:
                for read_number in sorted(sample_fastq_files.keys()):
                    sample_fastq_files[read_number].append(
                        SampleFastq("{}_{}_L00{}_R{}_001.fastq.gz".format(
                            sample_name, sample_number, lane_number, read_number)))
        n = 0
        for fastq_pair in SampleFastq.sample_fastq_file_pair(
                sample_fastq_files["1"] + sample_fastq_files["2"]):
            for i in [0, 1]:
                self.assertEqual(sample_fastq_files[sorted(sample_fastq_files.keys())[i]][n], fastq_pair[i])
            n += 1

    def test__sample_fastq_file_pair_single_read(self, *args):
        sample_fastq_files = map(SampleFastq, [
            "sample_A_S1_L001_R1_001.fastq.gz",
            "sample_A_S1_L002_R1_001.fastq.gz",
            "sample_A_S2_L001_R1_001.fastq.gz",
            "sample_A_S2_L005_R1_001.fastq.gz",
            "sample_B_S3_L002_R1_001.fastq.gz"
        ])
        n = 0
        for fastq_pair in SampleFastq.sample_fastq_file_pair(sample_fastq_files):
            self.assertEqual(1, len(fastq_pair))
            self.assertEqual(sample_fastq_files[n], fastq_pair[0])
            n += 1


class TestRunfolder(unittest.TestCase):

    def setUp(self):
        self.run_date = "180406"
        self.instrument_id = "ST-01234"
        self.run_number = "0123"
        self.flowcell_position = "A"
        self.flowcell_id = "ABC123CXY"

    def get_runfolder_name(self):
        return "_".join([
            self.run_date,
            self.instrument_id,
            self.run_number,
            "{}{}".format(self.flowcell_position, self.flowcell_id)])

    def test_split_runfolder_name(self):
        runfolder_path = os.path.join("/path", "to")
        runfolder_name = self.get_runfolder_name()
        runfolder = Runfolder(os.path.join(runfolder_path, runfolder_name))
        self.assertEqual(runfolder_path, runfolder.dirname)
        self.assertEqual(runfolder_name, runfolder.runfolder_name)
        self.assertEqual(self.run_date, runfolder.run_date)
        self.assertEqual(self.instrument_id, runfolder.instrument_id)
        self.assertEqual(self.run_number, runfolder.run_number)
        self.assertEqual(self.flowcell_position, runfolder.flowcell_position)
        self.assertEqual(self.flowcell_id, runfolder.flowcell_id)

