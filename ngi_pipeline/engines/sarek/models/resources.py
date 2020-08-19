import os
import re

from ngi_pipeline.engines.sarek.exceptions import ReferenceGenomeNotRecognized


class SampleFastq(object):
    """
    SampleFastq represents a fastq file and takes care of identifying properties such as sample name, lane, read etc.
    by splitting the path based on an expected regexp.
    """
    # the regexp used for splitting the file name
    FASTQ_REGEXP = r'^(.+)_([^_]+)_L00(\d)_R(\d)[^\.]*(\..*)$'

    def __init__(self, path):
        """
        Creates a SampleFastq object representing a fastq file.

        :param path: the path to the fastq file, where the file name will be used to identify properties
        """
        self.path = path
        self.dirname = os.path.dirname(self.path)
        self.filename = os.path.basename(self.path)
        self.sample_name, \
            self.sample_number, \
            self.lane_number, \
            self.read_number, \
            self.file_extension = self.split_filename(self.filename)

    def split_filename(self, filename):
        """
        Split the filename according to the regexp and return the identified groups. If the regexp was not matched,
        None will be returned for all properties.

        :param filename: the filename to split by the regexp
        :return: a list of the values captured by the regexp or a list of None values if the regexp did not capture
        anything
        """
        match = re.match(self.FASTQ_REGEXP, filename)
        return match.groups() if match is not None else [None for i in range(5)]

    @staticmethod
    def sample_fastq_file_pair(sample_fastqs):
        """
        Take a list of SampleFastq objects and return a generator where each element is a list containing a fastq file
        pair (if paired-end, for single-end the list will just contain one entry), with first element being R1 and
        second element being R2.

        :param sample_fastqs: a list of SampleFastq objects in any order
        :return: a generator of lists of fastq file pairs
        """
        sorted_fastqs = sorted(sample_fastqs, key=lambda f: (f.sample_number, f.lane_number, f.read_number))
        n = 0
        while n < len(sorted_fastqs):
            n += 1
            if int(sorted_fastqs[n-1].read_number) > 1:
                continue
            fastqs_to_yield = [sorted_fastqs[n-1]]
            try:
                if int(sorted_fastqs[n].read_number) > 1:
                    fastqs_to_yield.append(sorted_fastqs[n])
                    n += 1
            except IndexError:
                pass
            yield fastqs_to_yield


class Runfolder(object):
    """
    Runfolder represents a runfolder and identifies properties such as run date, instrument id etc. based on the
    name element of the path and a regexp used to identify them.
    """
    # the regexp used to split the runfolder name
    RUNFOLDER_REGEXP = r'^(\d{6})_([^_]+)_(\d+)_([AB])(\w+)$'

    def __init__(self, path):
        """
        Creates a Runfolder object representing a runfolder directory.

        :param path: the path to the runfolder where the name will be used to identify properties
        """
        self.path = path
        self.dirname = os.path.dirname(self.path)
        self.runfolder_name = os.path.basename(self.path)
        self.run_date, \
            self.instrument_id, \
            self.run_number, \
            self.flowcell_position, \
            self.flowcell_id = self.split_runfolder_name(self.runfolder_name)

    def split_runfolder_name(self, runfolder_name):
        """
        Split the runfolder name according to the regexp and return the identified groups. If the regexp was not
        matched, None will be returned for all properties.

        :param runfolder_name: the runfolder name to split by the regexp
        :return: a list of the values captured by the regexp or a list of None values if the regexp did not capture
        anything
        """
        match = re.match(self.RUNFOLDER_REGEXP, runfolder_name)
        return match.groups() if match is not None else [None for i in range(5)]


class ReferenceGenome(object):
    """
    The ReferenceGenome class represents the reference genome used by Sarek for the analysis. It has a factory method
    for returning the correct instance from a string representation.
    """
    NAME = None

    def __repr__(self):
        return self.NAME

    @staticmethod
    def get_instance(genome_name):
        """
        Factory method to get a ReferenceGenome instance representing the genome from a string.

        :raises: ReferenceGenomeNotRecognized if the genome name was not recognized
        :param genome_name: the name of the reference as a string, e.g. "GRCh37" or "GRCh38"
        :return: a ReferenceGenome instance
        """
        try:
            return filter(
                lambda ref: str(ref).lower() == genome_name.lower(),
                (GRCh37(), GRCh38())
            )[0]
        except IndexError:
            raise ReferenceGenomeNotRecognized(genome_name)

    def get_genomes_base_path(self, config):
        """
        Return the genomes_base path specified in the config for this reference genome or None if not specified

        :param config: a sarek config dict to look for the genomes_base path parameter in
        :return: the corresponding genomes_base path as a string or None if not found
        """
        return config.get("genomes_base_paths", {}).get(self.NAME)


class GRCh37(ReferenceGenome):
    """Class representing the GRCh37 reference genome"""
    NAME = "GRCh37"


class GRCh38(ReferenceGenome):
    """Class representing the GRCh38 reference genome"""
    NAME = "GRCh38"
