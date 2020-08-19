import os

from ngi_pipeline.engines.sarek.models.resources import Runfolder, SampleFastq
from ngi_pipeline.utils.filesystem import is_index_file


class SarekAnalysisSample(object):
    """
    The SarekAnalysisSample class is a convenience class for methods and operations that are related to the a sample
    being analyzed by a workflow modeled by a SarekAnalysis instance. The idea is that the sample object should not
    need to know about logic that are specific to the analysis workflow (e.g. what result files there are and where
    they can be found) but still provide a good interface for a sample being processed. For workflow-specific logic,
    the sample keeps a reference to the analysis object and delegates queries when needed.
    """

    def __init__(self, project_object, sample_object, analysis_object, restart_options=None):
        """
        Create an instance of SarekAnalysisSample

        :param project_object: a ngi_pipeline.conductor.classes.NGIProject instance representing the project the
        sample belongs to
        :param sample_object: a ngi_pipeline.conductor.classes.NGISample instance representing the sample
        :param analysis_object: a reference to the SarekAnalysis instance that this sample is analyzed with
        :param restart_options: a dict specifying the conditions under which a previously run job should be started
        """
        self.analysis_object = analysis_object
        self.sampleid = sample_object.name
        self.projectid = project_object.project_id
        self.project_base_path = project_object.base_path
        self.restart_options = {
            key: False for key in ["restart_failed_jobs", "restart_finished_jobs", "restart_running_jobs"]}
        self.restart_options.update(restart_options or {})
        self.sample_ngi_object = sample_object

    def sample_data_path(self):
        """
        :return: the path to the sample input data
        """
        return self.analysis_object.sample_data_path(self.project_base_path, self.projectid, self.sampleid)

    def sample_analysis_path(self):
        """
        :return: the path to the analysis output for the sample
        """
        return self.analysis_object.sample_analysis_path(self.project_base_path, self.projectid, self.sampleid)

    def sample_seqrun_path(self, libprepid, seqrunid):
        """
        :param libprepid: the libprep id whose data the path will refer to
        :param seqrunid: the seqrun id whose data the path will refer to
        :return: the path to the data for a particular libprep and seqrun combo
        """
        return os.path.join(self.sample_data_path(), libprepid, seqrunid)

    def sample_analysis_exit_code_path(self):
        """
        :return: the path to the exit code file for this sample and analysis
        """
        return self.analysis_object.sample_analysis_exit_code_path(
            self.project_base_path, self.projectid, self.sampleid)

    def sample_analysis_tsv_file(self):
        """
        :return: the path to the tsv file specifying the details of the analysis
        """
        return self.analysis_object.sample_analysis_tsv_file(self.project_base_path, self.projectid, self.sampleid)

    def sample_analysis_work_dir(self):
        return self.analysis_object.sample_analysis_work_dir(self.project_base_path, self.projectid, self.sampleid)

    def sample_analysis_results_dir(self):
        return self.analysis_object.sample_analysis_results_dir(self.project_base_path, self.projectid, self.sampleid)

    def _get_sample_librep(self, libprepid):
        """
        :param libprepid: the libprep id to return the corresponding NGILibprep object for
        :return: get the NGILibprep object corresponding to the supplied libprep id
        """
        return list(filter(lambda lp: lp.name == libprepid, self.sample_ngi_object)).pop()

    def sample_libprep_ids(self):
        """
        :return: a list of the libprep ids associated with the sample (based on the NGISample object)
        """
        return list(map(lambda x: x.name, self.sample_ngi_object))

    def libprep_seqrun_ids(self, libprepid):
        """
        :param libprepid: a libprep id to get the associated seqrun ids for
        :return: a list of the seqrun ids associated with the sample and specified libprep id
        (based on the NGISample object)
        """
        return list(map(lambda x: x.name, self._get_sample_librep(libprepid)))

    def libpreps_to_analyze(self):
        """
        Get a list of NGILibprep objects representing the libpreps that qualify for being analyzed. This will ask
        the attached analysis instance (which in turn checks the libprep QC status in Charon) for inclusion.

        :return: a list of NGILibprep objects representing libpreps that fulfil the criteria for being analyzed
        """
        return filter(
            lambda lp: self.analysis_object.libprep_should_be_started(
                self.projectid, self.sampleid, lp.name),
            self.sample_ngi_object)

    def seqruns_to_analyze(self, libprep):
        """
        Get a list of NGISeqrun objects representing the seqruns for the specified libprep object that qualify for
        being analyzed. This will ask the attached analysis instance (which in turn checks the seqrun status in Charon)
        for inclusion.

        :param libprep: the NGILibprep object for which to get the NGISeqrun objects that should be included in the
        analysis
        :return: a list of NGISeqrun objects representing seqruns that fulfil the criteria for being analyzed
        """
        return filter(
            lambda sr: self.analysis_object.seqrun_should_be_started(
                self.projectid, self.sampleid, libprep.name, sr.name, self.restart_options),
            libprep)

    def runid_and_fastq_files_for_sample(self):
        """
        Get an ordered representation of the fastq files belonging to this sample. Additionally, an identifier is
        constructed according to FCID.LANE.SAMPLE_NUMBER and can be used as a read tag in the analysis.

        The fastq files are returned ordered according to read number.

        :return: an iterator where each element is a list having the elements [identifier, fastq file R1,
        fastq file R2 (if available)]
        """
        for libprep in self.libpreps_to_analyze():
            for libprep_fastq in self._get_runid_and_fastq_files_for_libprep(libprep):
                yield libprep_fastq

    def _get_runid_and_fastq_files_for_libprep(self, libprep):
        """
        See `runid_and_fastq_files_for_sample`. This will iterate over the seqruns returned by `seqruns_to_analyze`
        for the specified libprep.

        :param libprep: the libprep object for which to get the run id and fastq files
        :return: an iterator where each element is a list having the elements [identifier, fastq file R1,
        fastq file R2 (if available)]
        """
        for seqrun in self.seqruns_to_analyze(libprep):
            for seqrun_fastq in self._get_runid_and_fastq_files_for_seqrun(libprep.name, seqrun):
                yield seqrun_fastq

    def _get_runid_and_fastq_files_for_seqrun(self, libprepid, seqrun):
        """
        See `runid_and_fastq_files_for_sample`. This will return the fastq files for the specified libprep and seqrun.

        :param libprepid: the libprep object for which to get the run id and fastq files
        :param seqrun: the seqrun object for which to get the run id and fastq files
        :return: an iterator where each element is a list having the elements [identifier, fastq file R1,
        fastq file R2 (if available)]
        """
        # the runfolder is represented with a Runfolder object
        runfolder = Runfolder(self.sample_seqrun_path(libprepid, seqrun.name))
        # iterate over the fastq file pairs belonging to the seqrun, filter out files with index reads
        sample_fastq_objects = map(
            lambda f: SampleFastq(os.path.join(runfolder.path, f)),
            filter(lambda fq: not is_index_file(fq), seqrun.fastq_files))
        for sample_fastq_file_pair in SampleFastq.sample_fastq_file_pair(sample_fastq_objects):
            runid = "{}.{}.{}".format(
                runfolder.flowcell_id, sample_fastq_file_pair[0].lane_number, sample_fastq_file_pair[0].sample_number)
            yield [runid] + map(lambda x: x.path, sample_fastq_file_pair)

    def runid_and_fastq_files_from_csv(self):
        """
        Get an ordered representation of the fastq files belonging to this sample from the tsv file. Additionally,
        the identifier constructed from FCID.LANE.SAMPLE_NUMBER is included.

        The fastq files are returned according to the order in the tsv file.

        :return: an iterator where each element is a list having the elements [identifier, fastq file R1,
        fastq file R2 (if available)]
        """
        for runid_and_fastq_file_paths in self.analysis_object.runid_and_fastq_files_from_tsv_file(
                self.sample_analysis_tsv_file()):
            yield runid_and_fastq_file_paths
