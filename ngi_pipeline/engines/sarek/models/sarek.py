import csv
import os

from ngi_pipeline.engines.sarek.database import CharonConnector, TrackingConnector
from ngi_pipeline.engines.sarek.exceptions import BestPracticeAnalysisNotRecognized, SampleNotValidForAnalysisError
from ngi_pipeline.engines.sarek.models.resources import ReferenceGenome
from ngi_pipeline.engines.sarek.models.sample import SarekAnalysisSample
from ngi_pipeline.engines.sarek.models.workflow import NextflowStep, SarekMainStep
from ngi_pipeline.engines.sarek.parsers import ParserIntegrator
from ngi_pipeline.engines.sarek.process import ProcessConnector, ProcessRunning, ProcessExitStatusSuccessful, \
    ProcessExitStatusFailed
from ngi_pipeline.utils.filesystem import safe_makedir


class SarekAnalysis(object):
    """
    Base class for the SarekAnalysis engine. This class contains the necessary methods for configuring and launching
    an analysis with the Sarek engine. However, some methods are not implemented (they are "abstract") and are
    expected to be implemented in subclasses, providing interfaces to the specialized analysis modes (e.g. Germline or
    Somatic).
    """

    DEFAULT_CONFIG = {
        "nextflow": {
            "profile": "uppmax",
            "command": "nextflow",
            "subcommand": "run"},
        "sarek": {
            "command": os.path.join(
                "/lupus",
                "ngi",
                "production",
                "latest",
                "sw",
                "sarek",
                "workflow")}
    }

    def __init__(
            self,
            reference_genome,
            config,
            log,
            charon_connector=None,
            tracking_connector=None,
            process_connector=None):
        """
        Create an instance of SarekAnalysis.

        :param reference_genome: a type of ReferenceGenome indicating the reference genome to use
        :param config: a dict object with configuration options
        :param log: a log handle to use for logging
        :param charon_connector: a CharonConnector instance to use for the database connection. If not specified, a new
        connector will be created
        :param tracking_connector: a TrackingConnector instance to use for connections to the local tracking database.
        If not specified, a new connector will be created
        :param process_connector: a ProcessConnector instance to use for starting the analysis. If not specified, a new
        connector for local execution will be created
        """
        self.reference_genome = reference_genome
        self.config = config
        self.log = log
        merged_configs = self.configure_analysis(opts={"sarek": {"genome": self.reference_genome}})
        self.sarek_config = merged_configs["sarek"]
        self.nextflow_config = merged_configs["nextflow"]
        self.charon_connector = charon_connector or CharonConnector(self.config, self.log)
        self.tracking_connector = tracking_connector or TrackingConnector(self.config, self.log)
        self.process_connector = process_connector or ProcessConnector(cwd=os.curdir)

    def __repr__(self):
        # returns the name of the instance type, e.g. "SarekAnalysis" or "SarekGermlineAnalysis"
        return type(self).__name__

    def configure_analysis(self, config=None, opts=None):
        """
        Put together the process config dict based on the default parameters in the class, updated with any options
        passed as well as the corresponding section (e.g. "sarek", "nextflow") in a supplied config

        :param config: a config dict. If specified, any content stored under the "sarek" key will be included in the
        returned dict
        :param opts: additional options can be specified in the call and will be included in the returned dict
        :return: a config dict based on the default values and updated with the passed parameters
        """
        config = config or self.config
        opts = opts or dict()
        merged_configs = dict()
        for section in ["sarek", "nextflow"]:
            merged_configs[section] = self.merge_configs(
                self.DEFAULT_CONFIG.get(section, dict()),
                config.get(section, dict()),
                **opts.get(section, dict())
            )

        # make sure to configure the correct genomes_base parameter if it's not set
        if self.reference_genome is not None:
            merged_configs["sarek"]["genomes_base"] = merged_configs["sarek"].get(
                "genomes_base",
                self.reference_genome.get_genomes_base_path(merged_configs["sarek"]))
        # let's unset the genomes_base_paths item since we don't want it to show up on the command line
        try:
            del(merged_configs["sarek"]["genomes_base_paths"])
        except KeyError:
            pass
        return merged_configs

    @staticmethod
    def merge_configs(default_config, global_config, **local_config):
        """
        Merge multiple configs together along with passed keyword arguments (local config). The order of precedence is
        local_config > global_config > default_config

        :param default_config: a dict having arguments as key/value pairs. Keys overlapping with global_config or
        local_config will be overridden
        :param global_config: a dict having arguments as key/value pairs. Keys overlapping with local_config will be
        overridden
        :param local_config: additional keyword arguments will be added as key/value pairs. Keys overlapping with the
        default or global configs will override those.
        :return: a config dict based on the default values and updated with the passed config and parameters
        """
        config = default_config.copy()
        config.update(global_config)
        config.update(local_config)
        return config

    @staticmethod
    def get_analysis_type_for_workflow(workflow):
        """
        Gets the type of the SarekAnalysis instance to use for a supplied workflow name.

        :param workflow: name of the workflow
        :return: a SarekAnalysis type appropriate for the workflow
        :raises BestPracticeAnalysisNotRecognized: if no suitable analysis type was found for the workflow
        """
        if workflow == "SarekAnalysis":
            return SarekAnalysis
        if workflow == "SarekGermlineAnalysis":
            return SarekGermlineAnalysis
        raise BestPracticeAnalysisNotRecognized(workflow)

    @staticmethod
    def get_analysis_instance_for_workflow(
            workflow,
            config,
            log,
            reference_genome=None,
            charon_connector=None,
            tracking_connector=None,
            process_connector=None):
        """
        Factory method returning a SarekAnalysis subclass instance corresponding to the best practice analysis specified
        by the supplied workflow name.

        :param workflow: name of the workflow
        :param config: a config dict
        :param log: a log handle
        :param reference_genome: an instance of ReferenceGenome. If omitted, it will be None
        :param charon_connector: a connector instance to the charon database. If None, the default connector will be
        used
        :param tracking_connector: a TrackingConnector instance to use for connections to the local tracking database.
        If not specified, a new connector will be created
        :param process_connector: a ProcessConnector instance to use for starting the analysis. If not specified, a new
        connector for local execution will be created
        :return: an instance of a SarekAnalysis subclass
        """
        instance_type = SarekAnalysis.get_analysis_type_for_workflow(workflow)
        return instance_type(
            reference_genome,
            config,
            log,
            charon_connector=charon_connector,
            tracking_connector=tracking_connector,
            process_connector=process_connector)

    @staticmethod
    def get_analysis_instance_for_project(
            projectid,
            config,
            log,
            charon_connector=None,
            tracking_connector=None,
            process_connector=None):
        """
        Factory method returning a SarekAnalysis subclass instance corresponding to the best practice analysis specified
        for the supplied project.

        :param projectid: the projectid of the project to get an analysis instance for
        :param config: a config dict
        :param log: a log handle
        :param charon_connector: a connector instance to the charon database. If None, the default connector will be
        used
        :param tracking_connector: a TrackingConnector instance to use for connections to the local tracking database.
        If not specified, a new connector will be created
        :param process_connector: a ProcessConnector instance to use for starting the analysis. If not specified, a new
        connector for local execution will be created
        :return: an instance of a SarekAnalysis subclass
        """

        charon_connector = charon_connector or CharonConnector(config, log)
        tracking_connector = tracking_connector or TrackingConnector(config, log)
        process_connector = process_connector or ProcessConnector(cwd=os.curdir)

        # fetch the best practice analysis pipeline and reference specified in Charon
        analysis_pipeline = charon_connector.analysis_pipeline(projectid)
        analysis_type = charon_connector.best_practice_analysis(projectid)
        analysis_reference = charon_connector.analysis_reference(projectid)
        reference_genome = ReferenceGenome.get_instance(analysis_reference)

        # we can only run Sarek analyses in this module
        if analysis_pipeline.lower() != "sarek":
            raise BestPracticeAnalysisNotRecognized(analysis_pipeline)

        # currently, we use the same analysis pipeline for targeted and wgs data
        if analysis_type.lower() in ["exome_germline", "wgs_germline"]:
            return SarekGermlineAnalysis(
                reference_genome,
                config,
                log,
                charon_connector,
                tracking_connector,
                process_connector)
        elif analysis_type in ["exome_somatic", "wgs_somatic"]:
            raise NotImplementedError(
                "best-practice.analysis for {} is not implemented".format(analysis_type))
        raise BestPracticeAnalysisNotRecognized(analysis_type)

    def status_should_be_started(
            self,
            status,
            restart_failed_jobs=False,
            restart_finished_jobs=False,
            restart_running_jobs=False):
        """
        Takes a status string (e.g. the analysis_status or alignment_status as stored in Charon) and decides whether
        analysis should be started based on the status, taking the value of the restart flags into account.

        :param status: the status string as stored in Charon (e.g. "UNDER_ANALYSIS", "NOT RUNNING" etc.)
        :param restart_failed_jobs: if True, jobs marked as failed are ok to start (default is False)
        :param restart_finished_jobs: if True, jobs marked as finished are ok to start (default is False)
        :param restart_running_jobs: if True, jobs marked as running are ok to start (default is False)
        :return: True if the analysis is ok to start or False otherwise
        """
        def _charon_status_list_from_process_status(process_status):
            analysis_status = self.charon_connector.analysis_status_from_process_status(process_status)
            alignment_status = self.charon_connector.alignment_status_from_analysis_status(analysis_status)
            return analysis_status, alignment_status

        if status in _charon_status_list_from_process_status(ProcessRunning):
            return restart_running_jobs
        if status in _charon_status_list_from_process_status(ProcessExitStatusSuccessful):
            return restart_finished_jobs
        if status in _charon_status_list_from_process_status(ProcessExitStatusFailed):
            return restart_failed_jobs
        return True

    def analyze_project(self, analysis_object, batch_analysis=True):
        """
        Start the analysis of the samples in the supplied NGIAnalysis object.

        :param analysis_object: a NGIAnalysis object containing the project and smaples to be
        analyzed as well as details for the analysis
        :param batch_analysis: boolean indicating whether analysis should be started as a single
        batch that includes all samples or for each sample individually
        :return: None
        """
        analysis_samples = []
        for sample_object in analysis_object.project:
            try:
                analysis_samples.append(
                    self.analyze_sample(
                        sample_object=sample_object,
                        analysis_object=analysis_object))
            except SampleNotValidForAnalysisError as e:
                analysis_object.log.error(e)

        self.start_analysis(
            analysis_samples=analysis_samples,
            batch_analysis=batch_analysis)

    def analyze_sample(self, sample_object, analysis_object):
        """
        Convert the supplied NGISample and NGIAnalysis object to a SarekAnalysisSample object.

        The status of the sample will be checked against the restart options in the analysis object.

        :raises: a SampleNotValidForAnalysisError if the sample is not eligible for analysis based
        on its status and the analysis options in the NGIAnalysis object
        :param sample_object: a NGISample object representing the sample to start analysis for
        :param analysis_object: a NGIAnalysis object containing the details for the analysis
        :return: a SarekAnalysisSample representing the sample object and analysis options
        """

        analysis_sample = SarekAnalysisSample(
            analysis_object.project,
            sample_object,
            self,
            restart_options={
                "restart_failed_jobs": analysis_object.restart_failed_jobs,
                "restart_finished_jobs": analysis_object.restart_finished_jobs,
                "restart_running_jobs": analysis_object.restart_running_jobs})

        if not self.sample_should_be_started(
                analysis_sample.projectid, analysis_sample.sampleid, analysis_sample.restart_options):
            raise SampleNotValidForAnalysisError(
                analysis_sample.projectid, analysis_sample.sampleid, "nothing to analyze")

        return analysis_sample

    def start_analysis(self, analysis_samples, batch_analysis=False):
        """
        Start the analysis for the supplied list of SarekAnalysisSample objects. If batch_analysis
        is True, all samples will be started together at the project level, in one batch.
        Otherwise, the samples will be started and handled individually. Regardless of the batch
        mode, if analysis is successfully started, the analysis will be recorded for each sample in
        the local tracking database.

        :param analysis_samples: a list of SarekAnalysisSample objects representing the samples
        and corresponding analysis parameters to start analysis for
        :param batch_analysis: boolean indicating whether analysis should be started as a single
        batch that includes all samples or for each sample individually
        :return: None
        """
        def _start_job(cmd, workdir, exit_code_path, job_name):
            pid = self.process_connector.execute_process(
                cmd,
                working_dir=workdir,
                exit_code_path=exit_code_path,
                job_name=job_name)
            self.log.info(
                "launched '{}', with {}, pid: {}".format(
                    cmd, type(self.process_connector), pid))
            return pid

        def _start_batch():
            self.create_project_tsv_file(analysis_samples)
            return _start_job(
                self.command_line(analysis_samples[0], batch_analysis=batch_analysis),
                analysis_samples[0].project_analysis_path(),
                analysis_samples[0].project_analysis_exit_code_path(),
                "{}-{}".format(
                    analysis_samples[0].projectid,
                    str(self))
            )

        def _start_single_sample(analysis_sample):
            self.create_tsv_file(analysis_sample)
            return _start_job(
                self.command_line(analysis_sample, batch_analysis=batch_analysis),
                analysis_sample.sample_analysis_path(),
                analysis_sample.sample_analysis_exit_code_path(),
                "{}-{}-{}".format(
                    analysis_sample.projectid,
                    analysis_sample.sampleid,
                    str(self)))

        pid = _start_batch() if batch_analysis else None
        for analysis_sample in analysis_samples:
            pid = _start_single_sample(analysis_sample) if not batch_analysis else pid

            # record the analysis details in the local tracking database
            self.tracking_connector.record_process_sample(
                analysis_sample.projectid,
                analysis_sample.sampleid,
                analysis_sample.project_base_path,
                str(self),
                "sarek",
                pid,
                type(self.process_connector))

    def sample_should_be_started(self,
                                 projectid,
                                 sampleid,
                                 restart_options):
        """
        Decides whether the analysis for a sample should be started based on the analysis status recorded in Charon
        and taking the value of the restart flags into account.

        :param projectid: the project id for the sample
        :param sampleid: the sample id
        :param restart_options: a dict with the restart options to take into account when deciding whether to start the
        sample
        :return: True if the analysis for the sample is ok to start or False otherwise
        """
        analysis_status = self.charon_connector.sample_analysis_status(projectid, sampleid)
        should_be_started = self.status_should_be_started(analysis_status, **restart_options)
        self.log.info(
            "{} - {}: sample analysis status is '{}' -> sample will{} be included".format(
                projectid,
                sampleid,
                analysis_status,
                "" if should_be_started else " NOT"))
        return should_be_started

    def libprep_should_be_started(self,
                                  projectid,
                                  sampleid,
                                  libprepid,
                                  start_failed_libpreps=False):
        """
        Decides whether the analysis for a libprep should be started based on the QC status recorded in Charon
        and taking the value of the start flags into account.

        :param projectid: the project id for the sample
        :param sampleid: the sample id
        :param libprepid: the libprep id
        :param start_failed_libpreps: if True, failed libpreps will be included in the analysis (default is False)
        :return: True if the analysis for the libprep is ok to start or False otherwise
        """
        libprep_qc_status = self.charon_connector.libprep_qc_status(projectid, sampleid, libprepid)
        should_be_started = libprep_qc_status != "FAILED" or start_failed_libpreps
        self.log.info(
            "{} - {} - {}: libprep QC is '{}' -> libprep will{} be included".format(
                projectid,
                sampleid,
                libprepid,
                libprep_qc_status,
                "" if should_be_started else " NOT"))
        return should_be_started

    def seqrun_should_be_started(self,
                                 projectid,
                                 sampleid,
                                 libprepid,
                                 seqrunid,
                                 restart_options):
        """
        Decides whether the analysis for a seqrun should be started based on the alignment status recorded in Charon
        and taking the value of the restart flags into account.

        :param projectid: the project id for the sample
        :param sampleid: the sample id
        :param libprepid: the libprep id
        :param seqrunid: the seqrun id
        :param restart_options: a dict with the restart options to take into account when deciding whether to start the
        seqrun
        :return: True if the analysis for the seqrun is ok to start or False otherwise
        """
        seqrun_alignment_status = self.charon_connector.seqrun_alignment_status(
            projectid, sampleid, libprepid, seqrunid)
        should_be_started = self.status_should_be_started(seqrun_alignment_status, **restart_options)
        self.log.info(
            "{} - {} - {} - {}: seqrun alignment status is '{}' -> seqrun will{} be included".format(
                projectid,
                sampleid,
                libprepid,
                seqrunid,
                seqrun_alignment_status,
                "" if should_be_started else " NOT"))
        return should_be_started

    def processing_steps(self, analysis_sample, batch_analysis=False):
        """
        Configure and get a list of the processing steps included in the analysis. Subclasses may call this and add
        additional steps as needed.

        :param analysis_sample: the SarekAnalysisSample to analyze
        :return: a list of the processing steps included in the analysis
        """

        return [
            NextflowStep(
                self.nextflow_config.get("command", "nextflow"),
                self.nextflow_config.get("subcommand", "run"),
                **{k: v for k, v in self.nextflow_config.items() if k != "command" and k != "subcommand"})]

    def command_line(self, analysis_sample, batch_analysis=False):
        raise NotImplementedError("command_line should be implemented in the subclasses")

    def generate_tsv_file_contents(self, analysis_sample):
        raise NotImplementedError("creation of sample tsv file contents should be implemented by subclasses")

    def collect_analysis_metrics(self, analysis_sample):
        raise NotImplementedError("collection of analysis results should be implemented by subclasses")

    def cleanup(self, analysis_sample):
        self.process_connector.cleanup(analysis_sample.sample_analysis_work_dir())

    def _create_tsv_file(self, analysis_sample):
        rows = self.generate_tsv_file_contents(analysis_sample)
        if not rows:
            raise SampleNotValidForAnalysisError(
                analysis_sample.projectid,
                analysis_sample.sampleid,
                "no libpreps or seqruns to analyze")
        return rows

    def create_tsv_file(self, analysis_sample):
        """
        Create a tsv file containing the information needed by Sarek for starting the analysis. Will decide the path to
        the tsv file based on the sample information. If the path does not exist, it will be created.

        :raises: a SampleNotValidForAnalysisError if no libpreps or seqruns for the sample were eligible for analysis
        :param analysis_sample: a SarekAnalysisSample object representing the sample to create the tsv file for
        :return: the path to the created tsv file
        """
        rows = self._create_tsv_file(analysis_sample)
        tsv_file = analysis_sample.sample_analysis_tsv_file()
        return self.write_tsv_file(rows, tsv_file)

    def create_project_tsv_file(self, analysis_samples):
        rows = []
        tsv_file = None
        for analysis_sample in analysis_samples:
            if not rows:
                tsv_file = analysis_sample.project_analysis_tsv_file()
            rows.extend(self._create_tsv_file(analysis_sample))
        return self.write_tsv_file(rows, tsv_file)

    @classmethod
    def write_tsv_file(cls, tsv_rows, tsv_file):
        safe_makedir(os.path.dirname(tsv_file))
        with open(tsv_file, "w") as fh:
            writer = csv.writer(fh, dialect=csv.excel_tab)
            writer.writerows(tsv_rows)
        return tsv_file

    @staticmethod
    def _project_paths(project_base_path, projectid, subroot=None):
        try:
            return os.path.join(project_base_path, subroot, projectid)
        except AttributeError:
            return os.path.join(project_base_path, projectid)

    @classmethod
    def _sample_paths(cls, project_base_path, projectid, sampleid, subroot=None):
        return os.path.join(cls._project_paths(project_base_path, projectid, subroot), sampleid)

    @classmethod
    def sample_data_path(cls, *args):
        return cls._sample_paths(*args, subroot="DATA")

    @classmethod
    def project_analysis_path(cls, *args):
        return os.path.join(cls._project_paths(*args, subroot="ANALYSIS"), cls.__name__)

    @classmethod
    def sample_analysis_path(cls, *args):
        return os.path.join(cls._sample_paths(*args, subroot="ANALYSIS"), cls.__name__)

    @classmethod
    def _project_analysis_file(cls, project_base_path, projectid, extension):
        return os.path.join(
            cls.project_analysis_path(project_base_path, projectid),
            "{}-{}.{}".format(
                projectid,
                cls.__name__,
                extension))

    @classmethod
    def _sample_analysis_file(cls, project_base_path, projectid, sampleid, extension):
        return os.path.join(
            cls.sample_analysis_path(project_base_path, projectid, sampleid),
            "{}-{}-{}.{}".format(
                projectid,
                sampleid,
                cls.__name__,
                extension))

    @classmethod
    def project_analysis_exit_code_path(cls, *args):
        return cls._project_analysis_file(*args, extension="exit_code")

    @classmethod
    def sample_analysis_exit_code_path(cls, *args):
        return cls._sample_analysis_file(*args, extension="exit_code")

    @classmethod
    def project_analysis_tsv_file(cls, *args):
        return cls._project_analysis_file(*args, extension="tsv")

    @classmethod
    def sample_analysis_tsv_file(cls, *args):
        return cls._sample_analysis_file(*args, extension="tsv")

    @classmethod
    def project_analysis_work_dir(cls, *args):
        return os.path.join(cls.project_analysis_path(*args), "work")

    @classmethod
    def sample_analysis_work_dir(cls, *args):
        return os.path.join(cls.sample_analysis_path(*args), "work")

    @classmethod
    def project_analysis_results_dir(cls, *args):
        return os.path.join(cls.project_analysis_path(*args), "results")

    @classmethod
    def sample_analysis_results_dir(cls, *args):
        return os.path.join(cls.sample_analysis_path(*args), "results")


class SarekGermlineAnalysis(SarekAnalysis):
    """
    Class representing the Sarek Germline analysis mode. This inherits the SarekAnalysis class but any mode-specific
    methods and configurations are overriding the base class equivalents.
    """

    def processing_steps(self, analysis_sample, batch_analysis=False):
        """
        Configure and get a list of the processing steps included in the analysis.

        :param analysis_sample: the SarekAnalysisSample to analyze
        :return: a list of the processing steps included in the analysis
        """
        outdir = analysis_sample.project_analysis_results_dir() \
            if batch_analysis else analysis_sample.sample_analysis_results_dir()
        tsv_file = analysis_sample.project_analysis_tsv_file() \
            if batch_analysis else analysis_sample.sample_analysis_tsv_file()
        local_sarek_config = {
            "outdir": outdir}
        local_sarek_config.update({k: v for k, v in self.sarek_config.items() if k != "command"})
        processing_steps = super(
            SarekGermlineAnalysis,
            self).processing_steps(analysis_sample, batch_analysis=batch_analysis)
        processing_steps.append(
            SarekMainStep(
                self.sarek_config.get("command"),
                input=tsv_file,
                **local_sarek_config))
        return processing_steps

    def command_line(self, analysis_sample, batch_analysis=False):
        """
        Creates the command line for launching the sample analysis and returns it as a string. Works by chaining the
        command line from each of the workflow steps in the analysis workflow.

        :param analysis_sample: the SarekAnalysisSample to analyze
        :return: the command line as a string
        """
        # each analysis step is represented by a SarekWorkflowStep instance
        # create the command line by chaining the command lines from each processing step
        return " ".join(
            [step.command_line() for step in self.processing_steps(analysis_sample, batch_analysis=batch_analysis)])

    def generate_tsv_file_contents(self, analysis_sample):
        """
        Create the contents of the tsv file used by Sarek for the analysis. This will check the libpreps and seqruns
        and decide whether to include them in the analysis based on QC flag and alignment status, respectively.

        :param analysis_sample: the SarekAnalysisSample to analyze
        :return: a list of lists representing tsv entries where the outer list is the rows and each element is a list of
        the fields that should be tab-separated in the tsv file
        """
        rows = []
        patientid = analysis_sample.sampleid
        gender = "ZZ"
        status = 0
        sampleid = patientid
        for sample_fastq in analysis_sample.runid_and_fastq_files_for_sample():
            rows.append([patientid, gender, status, sampleid] + sample_fastq)
        return rows

    @staticmethod
    def runid_and_fastq_files_from_tsv_file(tsv_file):
        """
        Get the identifier and path to the fastq files listed in a tsv file.

        :param tsv_file: the path to the tsv file
        :return: an iterator where each element is a list having the elements [identifier, fastq file R1,
        fastq file R2 (if available)]
        """
        with open(tsv_file) as fh:
            reader = csv.reader(fh, dialect=csv.excel_tab)
            for sample in reader:
                yield sample[4:]

    def collect_analysis_metrics(self, analysis_sample):
        """
        Parse and return the analysis metrics from the finished analysis.

        :param analysis_sample: the SarekAnalysisSample to analyze
        :return: a dict with the analysis metric names as keys and a list of corresponding values for each metric
        """
        results_parser = ParserIntegrator()
        for processing_step in self.processing_steps(analysis_sample):
            for parser_type, results_file in processing_step.report_files(analysis_sample):
                results_parser.add_parser(parser_type(results_file))
        return {
            metric: results_parser.query_parsers("get_{}".format(metric))[0]
            for metric in ["percent_duplication", "autosomal_coverage", "total_reads"]}

