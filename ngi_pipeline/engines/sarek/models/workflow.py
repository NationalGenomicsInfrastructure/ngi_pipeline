import os
from string import Template

from ngi_pipeline.engines.sarek.exceptions import ParserException
from ngi_pipeline.engines.sarek.parsers import QualiMapParser, PicardMarkDuplicatesParser


class WorkflowStep(object):
    """
    The WorkflowStepMixin is a basic utility class that provides functionality needed by workflow steps.
    """
    def __init__(self, command, hyphen="--", **kwargs):
        # create a dict with parameters based on the passed key=value arguments
        # expand any parameters passed as list items into a ","-separated string
        self.command = command
        self.hyphen = hyphen
        self.parameters = dict()
        self.parameters = {k: v if type(v) is not list else ",".join(v) for k, v in kwargs.items()}

    def _append_argument(self, base_string, name, hyphen="--"):
        """
        Append an argument with a placeholder for the value to the supplied string in a format suitable for the
        string.Template constructor. If no value exists for the argument name among this workflow step's config
        parameters, the supplied string is returned untouched.

        Example: step._append_argument("echo", "hello", "") should return "echo hello ${hello}", provided the step
        instance has a "hello" key in the step.sarek_args dict.

        :param base_string: the string to append an argument to
        :param name: the argument name to add a placeholder for
        :param hyphen: the hyphen style to prefix the argument name with (default "--")
        :return: the supplied string with an appended argument name and placeholder
        """
        # NOTE: a numeric value of 0 will be excluded (as will a boolean value of False)!
        if not self.parameters.get(name):
            return base_string
        return "{0} {2}{1} ${{{1}}}".format(base_string, name, hyphen)

    def command_line(self):
        """
        Generate the command line for launching a analysis workflow step based on the parameters. The command line will
        be built using the arguments passed to the step's constructor and returned as a string.

        :return: the command line for the workflow step as a string
        """
        template_string = "${command}"
        for argument_name in self.parameters.keys():
            template_string = self._append_argument(template_string, argument_name, hyphen=self.hyphen)
        command_line = Template(template_string).substitute(
            command=self.command,
            **self.parameters)
        return command_line

    @classmethod
    def report_files(cls, analysis_sample):
        return []


class NextflowStep(WorkflowStep):
    """
    The Nextflow command is implemented as a subclass of workflow step as well.
    """

    def __init__(self, command, subcommand, **kwargs):
        """
        Create a NextlowStep instance

        :param command: the command used to invoke Nextflow
        :param subcommand: the subcommand to pass to Nextflow (e.g. run)
        :param kwargs: additional Nextflow parameters to be specified on the command line
        """
        super(NextflowStep, self).__init__("{} {}".format(command, subcommand), hyphen="-", **kwargs)


class SarekWorkflowStep(WorkflowStep):
    """
    The SarekWorkflowStep class represents an analysis step in the Sarek workflow. Primarily, it provides a method for
    creating the step-specific command line.
    """

    available_tools = []

    def __init__(self, command, **kwargs):
        """
        Create a SarekWorkflowStep instance according to the passed parameters.

        :param command: the command used to invoke sarek (i.e. the path to the relevant nextflow script)
        :param kwargs: additional Sarek parameters to be included on the command line
        """
        super(SarekWorkflowStep, self).__init__(command, hyphen="--", **kwargs)

    def sarek_step(self):
        raise NotImplementedError("The Sarek workflow step definition for {} has not been defined".format(type(self)))


class SarekMainStep(SarekWorkflowStep):
    """
    Create a class instance representing the main Sarek workflow step.
    """

    def sarek_step(self):
        return "main.nf"

    @classmethod
    def report_files(cls, analysis_sample):
        """
        Get a list of the report files resulting from this processing step and the associated parsers.

        :param analysis_sample: the SarekAnalysisSample that was analyzed
        :return: a list of tuples where the first element is a parser class instance and the second is the path to the
        result file that the parser instance should parse
        """
        report_dir = os.path.join(
            analysis_sample.sample_analysis_results_dir(),
            "Reports",
            analysis_sample.sampleid)
        # MarkDuplicates output files may be named differently depending on if the pipeline was started with a single
        # fastq file pair or multiple file pairs
        markdups_dir = os.path.join(report_dir, "MarkDuplicates")
        metric_files = filter(lambda f: f.endswith(".metrics"), os.listdir(markdups_dir))
        if not metric_files:
            raise ParserException(cls, "no metrics file for MarkDuplicates found for sample {} in {}".format(
                analysis_sample.sampleid, markdups_dir))
        markdups_metrics_file = metric_files.pop()
        if metric_files:
            raise ParserException(cls, "multiple metrics files for MarkDuplicates found for sample {} in {}".format(
                analysis_sample.sampleid, markdups_dir))
        return [
            [
                QualiMapParser,
                os.path.join(report_dir, "bamQC", "{}.recal".format(analysis_sample.sampleid), "genome_results.txt")],
            [
                PicardMarkDuplicatesParser,
                os.path.join(markdups_dir, markdups_metrics_file)]]
