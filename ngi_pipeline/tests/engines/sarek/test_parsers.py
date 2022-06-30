import locale
import mock
import os
import tempfile
import unittest

from ngi_pipeline.engines.sarek.parsers import QualiMapParser, PicardMarkDuplicatesParser, ParserIntegrator

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

class ExampleData(object):

    qualimap_input = {
            "numeric_assignment": [
                "     std insert size = 1,478,053.3171",
                "     number of sequenced bases = 131,169,423 bp",
                "     number of reads = 967,817,737",
                "     GC percentage = 47.15%"],
            "assignment": [
                "     outfile = U004/genome_results.txt"],
            "cumulative_coverage": [
                "     There is a 0% of reference with a coverageData >= 49X",
                "     There is a 1.07% of reference with a coverageData >= 1X"],
            "contig_coverage": [
                "        chr1    248956422       58788   2.3613771248688655E-4   0.029014388002610237",
                "        chr1_KI270706v1_random  175055  0       0.0     0.0",
                "        21    77554325       780800934   10.067793562770355   1.07"],
            "invalid": [
                "",
                "                     ",
                ">>>>>>> Coverage per contig"
            ]
        }

    qualimap_output = {
            "numeric_assignment": [
                {"std insert size": 1478053.3171},
                {"number of sequenced bases": 131169423},
                {"number of reads": 967817737},
                {"GC percentage": 47.15}],
            "assignment": [
                {"outfile": "U004/genome_results.txt"}],
            "cumulative_coverage": [
                {"49X": 0},
                {"1X": 1.07}],
            "contig_coverage": [
                {"chr1 coverage": {
                    "contig": "chr1",
                    "length": 248956422,
                    "mapped bases": 58788,
                    "mean coverage": 2.3613771248688655E-4,
                    "standard deviation": 0.029014388002610237}},
                {"chr1_KI270706v1_random coverage": {
                    "contig": "chr1_KI270706v1_random",
                    "length": 175055,
                    "mapped bases": 0,
                    "mean coverage": 0,
                    "standard deviation": 0}},
                {"21 coverage": {
                    "contig": "21",
                    "length": 77554325,
                    "mapped bases": 780800934,
                    "mean coverage": 10.067793562770355,
                    "standard deviation": 1.07}}]}

    markduplicates_input = """
        ## htsjdk.samtools.metrics.StringHeader
        # picard.sam.MarkDuplicates INPUT=[/scratch/ANALYSIS/EQ-5678/piper_ngi/05_processed
        ## htsjdk.samtools.metrics.StringHeader
        # Started on: Tue Sep 04 13:35:35 CEST 2018
        
        ## METRICS CLASS        picard.sam.DuplicationMetrics
        LIBRARY UNPAIRED_READS_EXAMINED READ_PAIRS_EXAMINED     UNMAPPED_READS  UNPAIRED_READ_DUPLICATES        READ_PAIR_DUPLICATES    READ_PAIR_OPTICAL_DUPLICATES    PERCENT_DUPLICATION     ESTIMATED_LIBRARY_SIZE
        SXYX_default-SeqLib.v22 15      1454662 19      0       62237   5723    0.042784        18088186
        SXYX_default-SeqLib.v23 12      484880  14      0       7033    1845    0.014504        22325476
        """

    markduplicates_output = [
            {
                "LIBRARY": "SXYX_default-SeqLib.v22",
                "UNPAIRED_READS_EXAMINED": 15,
                "READ_PAIRS_EXAMINED": 1454662,
                "UNMAPPED_READS": 19,
                "UNPAIRED_READ_DUPLICATES": 0,
                "READ_PAIR_DUPLICATES": 62237,
                "READ_PAIR_OPTICAL_DUPLICATES": 5723,
                "PERCENT_DUPLICATION": 0.042784,
                "ESTIMATED_LIBRARY_SIZE": 18088186
            },
            {
                "LIBRARY": "SXYX_default-SeqLib.v23",
                "UNPAIRED_READS_EXAMINED": 12,
                "READ_PAIRS_EXAMINED": 484880,
                "UNMAPPED_READS": 14,
                "UNPAIRED_READ_DUPLICATES": 0,
                "READ_PAIR_DUPLICATES": 7033,
                "READ_PAIR_OPTICAL_DUPLICATES": 1845,
                "PERCENT_DUPLICATION": 0.014504,
                "ESTIMATED_LIBRARY_SIZE": 22325476
            }]


class TestQualiMapParser(unittest.TestCase):

    def setUp(self):
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
        self.examples = ExampleData.qualimap_input
        self.examples_expected = ExampleData.qualimap_output

    def tearDown(self):
        locale.resetlocale()

    def helper(self, test_key, test_fn):
        for i in range(len(self.examples[test_key])):
            self.assertDictEqual(
                self.examples_expected[test_key][i],
                test_fn(self.examples[test_key][i]) or dict())
        for key in [k for k in list(self.examples.keys()) if k != test_key]:
            self.assertTrue(
                all([test_fn(l) is None for l in self.examples[key]]))

    def test___parse_numeric_assignment(self):
        test_key = "numeric_assignment"
        test_fn = QualiMapParser._parse_numeric_assignment
        self.helper(test_key, test_fn)

    def test___parse_assignment(self):
        test_key = "assignment"
        test_fn = QualiMapParser._parse_assignment
        for i in range(len(self.examples[test_key])):
            self.assertDictEqual(
                self.examples_expected[test_key][i],
                test_fn(self.examples[test_key][i]) or dict())

    def test___parse_cumulative_coverage(self):
        test_key = "cumulative_coverage"
        test_fn = QualiMapParser._parse_cumulative_coverage
        self.helper(test_key, test_fn)

    def test___parse_contig_coverage(self):
        test_key = "contig_coverage"
        test_fn = QualiMapParser._parse_contig_coverage
        self.helper(test_key, test_fn)

    def test_get_autosomal_coverage(self):
        with mock.patch.object(QualiMapParser, "parse_result_file"):
            parser = QualiMapParser("mocked-result-file")
        QualiMapParser.AUTOSOMES = ["1", "21"]
        for contig in self.examples_expected["contig_coverage"]:
            parser.data.update(contig)
        expected_coverage = float(
            1. *
            sum([parser.data["{} coverage".format(c)]["mapped bases"] for c in ["chr1", "21"]]) /
            sum([parser.data["{} coverage".format(c)]["length"] for c in ["chr1", "21"]]))
        self.assertEqual(expected_coverage, parser.get_autosomal_coverage())

    def test_get_total_reads(self):
        with mock.patch.object(QualiMapParser, "parse_result_file"):
            parser = QualiMapParser("mocked-result-file")
        expected_number_of_reads = 1234567890
        parser.data["number of reads"] = expected_number_of_reads
        self.assertEqual(expected_number_of_reads, parser.get_total_reads())


class TestPicardMarkDuplicatesParser(unittest.TestCase):

    def setUp(self):
        self.example_output = ExampleData.markduplicates_input
        self.example_handle = StringIO(self.example_output)
        self.example_data = ExampleData.markduplicates_output

    def test__parse_metrics_handle(self):
        with mock.patch.object(PicardMarkDuplicatesParser, "parse_result_file"):
            parser = PicardMarkDuplicatesParser("mocked-result-file")
        parser._parse_metrics_handle(self.example_handle)
        number_of_example_libs = len(self.example_data)
        self.assertEqual(number_of_example_libs, len(parser.data["metrics"]))
        for i in range(number_of_example_libs):
            self.assertDictEqual(self.example_data[i], parser.data["metrics"][i])

    def test_get_percent_duplication(self):
        with mock.patch.object(PicardMarkDuplicatesParser, "parse_result_file"):
            parser = PicardMarkDuplicatesParser("mocked-result-file")
        parser.data = {"metrics": self.example_data}
        epsilon = 1e-4
        self.assertLess(
            abs(
                100*self.example_data[1]["PERCENT_DUPLICATION"] -
                parser.get_percent_duplication(library=self.example_data[1]["LIBRARY"])),
            epsilon)
        total_duplication = 3.571436857568654
        self.assertLess(
            abs(
                total_duplication -
                parser.get_percent_duplication()
            ),
            epsilon
        )

    def test__convert_to_unit(self):
        test_data = {
            int: [
                ["5", 5],
                ["-5", -5]],
            float: [
                ["3.5", 3.5],
                ["-3.5", -3.5]],
            str: [
                ["not-a-value", "not-a-value"],
                ["3-5", "3-5"]]}

        for expected_type in test_data.keys():
            for case_data in test_data[expected_type]:
                observed_value = PicardMarkDuplicatesParser._convert_to_unit(case_data[0])
                self.assertTrue(isinstance(observed_value, expected_type))
                self.assertEqual(case_data[1], observed_value)


class TestParserIntegrator(unittest.TestCase):

    markdups_resultfile = None
    qualimap_resultfile = None

    @classmethod
    def setUpClass(cls):

        def _write_data(data):
            with tempfile.NamedTemporaryFile(delete=False, mode='w+t') as fh:
                fh.write(data)
                return fh.name

        cls.markdups_resultfile = _write_data(ExampleData.markduplicates_input)
        cls.qualimap_resultfile = _write_data(
            "\n".join([
                "\n".join(rows) for rows in ExampleData.qualimap_input.values()]))

    @classmethod
    def tearDownClass(cls):
        for f in [cls.markdups_resultfile, cls.qualimap_resultfile]:
            os.unlink(f)

    def setUp(self):
        self.qualimap_parser = QualiMapParser(self.qualimap_resultfile)
        self.qualimap_parser.AUTOSOMES = ["1", "21"]
        self.markdups_parser = PicardMarkDuplicatesParser(self.markdups_resultfile)
        self.integrator = ParserIntegrator()

    def test_add_parser(self):
        expected_parsers = [
            self.markdups_parser,
            self.qualimap_parser
        ]
        for parser in expected_parsers:
            self.integrator.add_parser(parser)
        self.assertListEqual(expected_parsers, self.integrator.parsers)
        with self.assertRaises(TypeError):
            self.integrator.add_parser("this-is-not-a-parser")

    def test_query_parser(self):
        self.integrator.add_parser(self.markdups_parser)
        self.integrator.add_parser(self.qualimap_parser)
        expected_results = {
            "get_total_reads": self.qualimap_parser.get_total_reads(),
            "get_autosomal_coverage": self.qualimap_parser.get_autosomal_coverage(),
            "get_percent_duplication": self.markdups_parser.get_percent_duplication()
        }
        for attribute in expected_results.keys():
            self.assertListEqual([expected_results[attribute]], self.integrator.query_parsers(attribute))
        for library in ExampleData.markduplicates_output:
            self.assertListEqual(
                [self.markdups_parser.get_percent_duplication(library=library["LIBRARY"])],
                self.integrator.query_parsers("get_percent_duplication", library=library["LIBRARY"]))
