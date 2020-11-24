import unittest
import mock
import tempfile
import os
import shutil

import ngi_pipeline.engines.piper_ngi.parsers as parsers

class TestParsers(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.genome_results_file = os.path.join(self.tmp_dir, 'genome_file.txt')
        genome_results_content = ['>>>>>>> Globals',
                                  'number of reads = 10,000',
                                  '>>>>>>> Coverage per contig',
                                  '10 100 50']
        with open(self.genome_results_file, 'w') as f:
            f.write('\n'.join(genome_results_content))

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.tmp_dir)

    @mock.patch('ngi_pipeline.engines.piper_ngi.parsers.parse_qualimap_reads')
    def test_parse_results_for_workflow(self, mock_parse):
        mock_parse.return_value = True
        workflow_name = 'qualimap_reads'
        missing_workflow_name = 'missing'
        
        parsed = parsers.parse_results_for_workflow(workflow_name, 'arg', some_kwarg='Hi')
        self.assertTrue(parsed)
        mock_parse.assert_called_once_with('arg', some_kwarg='Hi')

        with self.assertRaises(NotImplementedError):
            not_parsed = parsers.parse_results_for_workflow(missing_workflow_name)

    def test_parse_qualimap_reads(self):
        got_reads = parsers.parse_qualimap_reads(self.genome_results_file)
        self.assertEqual(got_reads, 10000)

    def test_parse_qualimap_coverage(self):
        got_coverage = parsers.parse_qualimap_coverage(self.genome_results_file)
        self.assertEqual(got_coverage, 0.5)

    def test_parse_mean_coverage_from_qualimap(self):
        piper_qc_dir = os.path.join(self.tmp_dir, 'piper_qc_dir')
        os.makedirs(os.path.join(piper_qc_dir, 
                                 'P123_1001.201112_A00187_0331_AHFCFLDSXX.P123_1001', 
                                 'qc_dir'))
        genome_results_file = os.path.join(piper_qc_dir, 
                                           'P123_1001.201112_A00187_0331_AHFCFLDSXX.P123_1001', 
                                           'genome_results.txt')
        shutil.copyfile(self.genome_results_file, genome_results_file)
        sample_id = 'P123_1001'
        fcid = '201112_A00187_0331_AHFCFLDSXX'

        got_coverage = parsers.parse_mean_coverage_from_qualimap(piper_qc_dir, 
                                                                 sample_id, 
                                                                 fcid=fcid)
        self.assertEqual(got_coverage, 0.5)

    def test_parse_genotype_concordance(self):
        genotype_concordance_file = os.path.join(self.tmp_dir, 'gtc_file.txt')
        file_content = ['#:GATKTable:GenotypeConcordance_Summary:Per-sample summary statistics: NRS, NRD, and OGC',
                        'Sample   Non-Reference Sensitivity  Non-Reference Discrepancy  Overall_Genotype_Concordance',
                        'ALL                          0.010                      0.087                         0.913',
                        'P123_1001                    0.010                      0.087                         0.913']
        with open(genotype_concordance_file, 'w') as f:
            f.write('\n'.join(file_content))
        
        got_gtc = parsers.parse_genotype_concordance(genotype_concordance_file)
        expected_gtc = {'P123_1001': 0.913}
        self.assertEqual(got_gtc, expected_gtc)

    def test_parse_deduplication_percentage(self):
        deduplication_file = os.path.join(self.tmp_dir, 'duplication.txt')
        file_content = ['## METRICS CLASS picard.sam.DuplicationMetrics',
                        'PERCENT_DUPLICATION SOMETHING_ELSE',
                        '0.5 10']
        with open(deduplication_file, 'w') as f:
            f.write('\n'.join(file_content))
        percent_duplication = parsers.parse_deduplication_percentage(deduplication_file)
        self.assertEqual(percent_duplication, 50)