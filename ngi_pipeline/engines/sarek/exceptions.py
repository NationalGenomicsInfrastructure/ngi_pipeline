

class SarekException(Exception):
    pass


class BestPracticeAnalysisNotRecognized(SarekException):

    def __init__(self, best_practice_analysis):
        super(BestPracticeAnalysisNotRecognized, self).__init__(
            "best-practice-analysis '{}' not recognized".format(best_practice_analysis))
        self.best_practice_analysis = best_practice_analysis


class ReferenceGenomeNotRecognized(Exception):

    def __init__(self, genome_name):
        super(ReferenceGenomeNotRecognized, self).__init__(
            "reference genome '{}' not recognized".format(genome_name))
        self.genome_name = genome_name


class SampleNotValidForAnalysisError(Exception):

    def __init__(self, projectid, sampleid, reason=None):
        super(SampleNotValidForAnalysisError, self).__init__(
            "requirements for starting analysis not fulfilled for sample '{}' in project '{}'{}".format(
                sampleid, projectid, ": {}".format(reason.__repr__()) if reason else ""))
        self.projectid = projectid
        self.sampleid = sampleid
        self.reason = reason


class DatabaseProjectException(Exception):

    MESSAGE = "project database exception"

    def __init__(self, projectid, message=None, reason=None):
        super(DatabaseProjectException, self).__init__(
            "{}{} for project '{}'".format(
                message or self.MESSAGE,
                " ({})".format(reason) if reason else "",
                projectid))
        self.projectid = projectid
        self.message = message or self.MESSAGE
        self.reason = reason


class DatabaseSampleException(DatabaseProjectException):

    MESSAGE = "sample database exception"

    def __init__(self, projectid, sampleid, message=None, reason=None):
        super(DatabaseSampleException, self).__init__(
            projectid,
            message="{} for sample '{}'".format(message or self.MESSAGE, sampleid),
            reason=reason)
        self.sampleid = sampleid


class DatabaseSeqrunException(DatabaseSampleException):

    MESSAGE = "seqrun database exception"

    def __init__(self, projectid, sampleid, libprepid, seqrunid, message=None, reason=None):
        super(DatabaseSeqrunException, self).__init__(
            projectid,
            sampleid,
            "{} for seqrun '{}' in libprep '{}'".format(message or self.MESSAGE, seqrunid, libprepid),
            reason)
        self.libprepid = libprepid
        self.seqrunid = seqrunid


class BestPracticeAnalysisNotSpecifiedError(DatabaseProjectException):

    MESSAGE = "best-practice-analysis could not be found"


class AnalysisPipelineNotSpecifiedError(DatabaseProjectException):

    MESSAGE = "analysis pipeline could not be found"


class AnalysisReferenceNotSpecifiedError(DatabaseProjectException):

    MESSAGE = "analysis reference genome could not be found"


class SampleAnalysisStatusNotFoundError(DatabaseSampleException):

    MESSAGE = "sample analysis status not found"


class SampleLookupError(DatabaseSampleException):

    MESSAGE = "error fetching sample property from Charon"


class SampleUpdateError(DatabaseSampleException):

    MESSAGE = "sample attribute could not be set"


class SeqrunUpdateError(DatabaseSeqrunException):

    MESSAGE = "seqrun attribute could not be set"


class SampleAnalysisStatusNotSetError(DatabaseSampleException):

    def __init__(self, projectid, sampleid, status, reason=None):
        super(SampleAnalysisStatusNotSetError, self).__init__(
            projectid,
            sampleid,
            message="sample analysis status '{}' could not be set".format(status),
            reason=reason)
        self.status = status


class SampleAlignmentStatusNotSetError(DatabaseSeqrunException):

    def __init__(self, projectid, sampleid, libprepid, seqrunid, status, reason=None):
        super(SampleAlignmentStatusNotSetError, self).__init__(
            projectid,
            sampleid,
            libprepid,
            seqrunid,
            message="seqrun alignment status '{}' could not be set".format(status),
            reason=reason)
        self.status = status


class SlurmStatusNotRecognizedError(Exception):

    def __init__(self, jobid, job_state):
        super(SlurmStatusNotRecognizedError, self).__init__(
            "SLURM job state '{}' for job id {} is not recognized".format(job_state, jobid))
        self.jobid = jobid
        self.job_state = job_state


class AnalysisStatusForProcessStatusNotFoundError(Exception):

    def __init__(self, process_status):
        super(AnalysisStatusForProcessStatusNotFoundError, self).__init__(
            "Charon analysis status corresponding to process status {} not found".format(process_status.__name__))
        self.process_status = process_status


class AlignmentStatusForAnalysisStatusNotFoundError(Exception):

    def __init__(self, analysis_status):
        super(AlignmentStatusForAnalysisStatusNotFoundError, self).__init__(
            "Charon alignment status corresponding to analysis status {} not found".format(analysis_status))
        self.analysis_status = analysis_status


class ParserException(Exception):

    def __init__(self, instance, message):
        super(ParserException, self).__init__(
            "{} raised an exception: {}".format(type(instance).__name__, message)
        )
        self.instance = instance


class ParserMetricNotFoundException(ParserException):

    def __init__(self, instance, metric):
        super(ParserMetricNotFoundException, self).__init__(
            instance,
            "'{}' not retrieable by parser type".format(metric))
        self.metric = metric
