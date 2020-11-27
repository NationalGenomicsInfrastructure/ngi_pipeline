import json
import locale
import re

from ngi_pipeline.engines.sarek.exceptions import ParserException, ParserMetricNotFoundException
from six.moves import map
from six.moves import zip


class ParserIntegrator(object):
    """
    The ParserIntegrator instance holds a list of ReportParser instances which can all be queried by a single call.
    """

    def __init__(self):
        """
        Create a new ParserIntegrator instance without any associated Reportparsers. ReportParsers are added by calling
        the `add_parser` method.
        """
        self.parsers = []

    def add_parser(self, parser):
        """
        Append a ReportParser instance to the list of integrated Parsers.

        :param parser: the ReportParser to integrate
        :return: None
        """
        if not isinstance(parser, ReportParser):
            raise TypeError("{} is not a sublass of {}".format(type(parser).__name__, type(ReportParser).__name__))
        self.parsers.append(parser)

    def query_parsers(self, attribute, *args, **kwargs):
        """
        Query all ReportParsers added to this ParserIntegrator for a specific attribute, optionally specifying
        parameters to use in the query. Note that the attribute should be a method. Each ReportParser
        is checked by calling the `getattr` function. The results from successful queries are returned in a list.

        :param attribute: the name of the attribute to query for, e.g. a method name like `get_total_reads`
        :param args: additional arguments are passed to the ReportParser query method
        :param kwargs: additional keyword arguments are passed to the ReportParser query method
        :return: a list of the results returned from the ReportParsers
        """
        results = []
        for parser in self.parsers:
            try:
                results.append(getattr(parser, attribute)(*args, **kwargs))
            except (AttributeError, ParserMetricNotFoundException):
                pass
        return results


class ReportParser(object):
    """
    Base class for report parsers
    """

    def __init__(self, result_file):
        """
        Create a new instance of a ReportParser. Will parse the supplied result file and populate the data structure.

        :param result_file: path to the result file which the parser should parse
        """
        self.data = {}
        self.parse_result_file(result_file)

    def _raise_implementation_error(self, metric):
        raise ParserMetricNotFoundException(self, metric)

    def get_percent_duplication(self, *args, **kwargs):
        self._raise_implementation_error("get_percent_duplication")

    def get_autosomal_coverage(self, *args, **kwargs):
        self._raise_implementation_error("get_autosomal_coverage")

    def get_total_reads(self, *args, **kwargs):
        self._raise_implementation_error("get_total_reads")

    def parse_result_file(self, result_file):
        raise NotImplementedError("{} has not implemented result parsing".format(type(self).__name__))


class MultiQCParser(ReportParser):

    def data_source(self, tool, section):
        sources = self.data["report_data_sources"][tool][section]
        return list(sources.keys())

    def parse_result_file(self, json_data_file):
        with open(json_data_file) as fh:
            self.data = json.load(fh)

    def multiqc_raw_data(self):
        return self.data["report_saved_raw_data"]

    def multiqc_general_stats(self):
        return self.multiqc_raw_data()["multiqc_general_stats"]

    def multiqc_samtools_stats(self):
        return self.multiqc_raw_data()["multiqc_samtools_stats"]

    def autosomal_coverage(self):
        pass

    def total_sequenced_reads(self):
        source = [s for s in self.data_source("Samtools", "stats") if s.endswith(".real")].pop()
        source_data = self.multiqc_samtools_stats()[source]
        return source_data["raw_total_sequences"]


class QualiMapParser(ReportParser):
    """
    Parser for the QualiMap output result file.
    """
    AUTOSOMES = [str(i) for i in range(1, 23)]

    def parse_result_file(self, genome_results_file):
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
        with open(genome_results_file) as fh:
            self._parse_genome_results_lines(fh)

    def _parse_genome_results_lines(self, fh):
        for line in fh:
            self.data.update(self._parse_entry(line))

    def get_autosomal_coverage(self):
        """
        Calculate the average coverage on autosomes only. Coverage is calculated by summing the total number of bases
        mapped to autosomes, divided by the total number of bases on the autosomes.

        :return: Coverage as a fraction
        """
        mapped_bases = 0
        total_bases = 0
        for key in ["chr{} coverage".format(chromosome) for chromosome in self.AUTOSOMES]:
            try:
                data = self.data.get(key) or self.data[key[3:]]
                mapped_bases += data["mapped bases"]
                total_bases += data["length"]
            except KeyError as ke:
                raise ParserException(
                    self, "no coverage data parsed for {}: {}".format(key.split()[0], ke))
        return float(1. * mapped_bases / total_bases)

    def get_total_reads(self):
        """
        Get the number of reads reported by QualiMap.

        :return: the number of reads reported by QualiMap
        """
        try:
            return self.data["number of reads"]
        except KeyError:
            # no matching key was found
            pass

    @staticmethod
    def _parse_numeric_assignment(line):
        """
        Parse a numeric assignemnt from a string, typically a line from the QualiMap result report. If no assignment
        can be parsed, None is returned.

        :param line: the input string
        :return: the assignemnt as a dict, with the value as a float or None if no numeric assignment could be parsed
        """
        # identify key = value assignments with numeric values
        try:
            key, value = re.search(r'^\s+([^=]+) = ([0-9,.]+)', line).groups()
            value = locale.atof(value)
            key = key.strip()
            return {key: value}
        except ValueError:
            # it was an assignment but the conversion to numeric value failed
            pass
        except AttributeError:
            # not an assignment or not a numeric assignment
            pass

    @staticmethod
    def _parse_assignment(line):
        """
        Parse an assignemnt from a string, typically a line from the QualiMap result report. If no assignment
        can be parsed, None is returned.

        :param line: the input string
        :return: the assignemnt as a dict, with the value as a string or None if no assignment could be parsed
        """
        # identify key = value assignments
        try:
            key, value = re.search(r'^\s+([^=]+)= (.+)$', line).groups()
            key, value = list(map(str.strip, [key, value]))
            return {key: value}
        except AttributeError:
            # not an assignment
            pass

    @staticmethod
    def _parse_cumulative_coverage(line):
        """
        Parse cumulative coverage report from a string, typically a line from the QualiMap result report.
        If no cumulative coverage can be parsed, None is returned.

        :param line: the input string
        :return: the cumulative coverage as a dict, with the value as a float or None if no numeric assignment could be
        parsed
        """
        # identify cumulative coverage calculations
        try:
            value, key = re.search(r'([0-9.]+)%.*>= ([0-9]+X)', line).groups()
            value = locale.atof(value)
            return {key: value}
        except AttributeError:
            pass

    @staticmethod
    def _parse_contig_coverage(line):
        """
        Parse contig coverage from a string, typically a line from the QualiMap result report.
        If no contig coverage can be parsed, None is returned.

        :param line: the input string
        :return: the contig coverage as a dict, with "[CONTIG] coverage" as key and a dict with the contig coverage
        information as value or None if no contig coverage could be parsed
        """
        # identify contig coverage
        try:
            values = re.search(r'^\s+(\S+)\s+([0-9]+)\s+([0-9]+)\s+(\S+)\s+(\S+)\s*$', line).groups()
            key = "{} coverage".format(values[0])
            value = dict()
            value["contig"] = values[0]
            value["length"] = locale.atoi(values[1])
            value["mapped bases"] = locale.atoi(values[2])
            value["mean coverage"] = locale.atof(values[3])
            value["standard deviation"] = locale.atof(values[4])
            return {key: value}
        except ValueError:
            # problems with the conversion to numeric values
            pass
        except AttributeError:
            # not a contig coverage row
            pass

    def _parse_entry(self, line):
        """
        Parse a string, typically a line from the Qualimap result report and try to make the relevant assignment. This
        is done by trying different assignments until one is successful. The order of assignments is:
          - _parse_numeric_assignment
          - _parse_assignment
          - _parse_cumulative_coverage
          - _parse_contig_coverage

        :param line: the input string
        :return: the assignment as a dict returned by the methods above, or an empty dict if no assignment  could be
        parsed
        """
        return self._parse_numeric_assignment(line) or \
               self._parse_assignment(line) or \
               self._parse_cumulative_coverage(line) or \
               self._parse_contig_coverage(line) or \
               dict()


class PicardMarkDuplicatesParser(ReportParser):
    """
    Parser for Picard's MarkDuplicates output result file.
    """

    def parse_result_file(self, metrics_file):
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
        with open(metrics_file, "r") as fh:
            self._parse_metrics_handle(fh)

    def _parse_metrics_handle(self, fh):

        def __parse_library_lines():
            local_libraries = []
            local_line = fh.next().strip()
            while len(local_line) > 0 and not local_line.startswith("##"):
                local_libraries.append(
                    list(map(
                        self._convert_to_unit,
                        local_line.split())))
                local_line = fh.next().strip()
            return local_libraries

        for line in fh:
            if line is None or not line.strip().startswith("## METRICS CLASS"):
                continue
            headers = fh.next().strip().split()
            self.data = {"metrics": [
                dict(list(zip(headers, library))) for library in __parse_library_lines()]}

    @staticmethod
    def _convert_to_unit(raw_value):
        try:
            return locale.atoi(raw_value)
        except ValueError:
            # this was obviously not an integer
            pass
        try:
            return locale.atof(raw_value)
        except ValueError:
            # apparently not a float either
            pass
        # if all fails, return the value as it is
        return raw_value

    def get_percent_duplication(self, library=None):
        """
        Get the percentage of duplication reported by MarkDuplicates. If multiple libraries are present, this will
        calculate the duplication across all libraries, i.e. the total number of duplicate reads divided by the
        total number of reads. A percentage is returned.

        :param library: optional, if specified, only the numbers for the specified library will be returned. Default is
        to return the average across all libraries
        :return: The percent of duplication as a float
        """
        libraries = [l for l in self.data.get("metrics", []) 
                     if library is None 
                     or l["LIBRARY"] == library]
        total_reads = \
            sum([lib["UNPAIRED_READS_EXAMINED"] for lib in libraries]) + \
            2*sum([lib["READ_PAIRS_EXAMINED"] for lib in libraries])
        duplicate_reads = \
            sum([lib["UNPAIRED_READ_DUPLICATES"] for lib in libraries]) + \
            2*sum([lib["READ_PAIR_DUPLICATES"] for lib in libraries])
        return 100. * duplicate_reads / total_reads
