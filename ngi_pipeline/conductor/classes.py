
import importlib
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.charon import recurse_status_for_sample

class NGIAnalysis(object):
    def __init__(self, project, restart_failed_jobs=None,
                    restart_finished_jobs=False, restart_running_jobs=False,
                    keep_existing_data=False, no_qc=False, exec_mode="sbatch",
                    quiet=False, manual=False, config=None, config_file_path=None,
                    generate_bqsr_bam=False, log=None, sample=None):
        self.project=project
        self.sample=sample
        self.restart_failed_jobs=restart_failed_jobs
        self.restart_finished_jobs=restart_finished_jobs
        self.restart_running_jobs=restart_running_jobs
        self.keep_existing_data=keep_existing_data 
        self.no_qc=no_qc 
        self.exec_mode=exec_mode
        self.quiet=quiet
        self.manual=manual
        self.config=config
        self.config_file_path=config_file_path
        self.generate_bqsr_bam=generate_bqsr_bam
        self.log=log

        if not log:
            self.log=minimal_logger(__name__)

        self.engine=self.get_engine()

    def get_engine(self):
        try:
            return get_engine_for_bp(self.project, self.config, self.config_file_path)
        except (RuntimeError, CharonError) as e:
            self.LOG.error('Cannot identify engine for project {} : {}'.format(self.project, e))
            return None


class NGIObject(object):
    def __init__(self, name, dirname, subitem_type):
        self.being_analyzed=False
        self.name = name
        self.dirname = dirname
        self._subitems = {}
        self._subitem_type = subitem_type

    def _add_subitem(self, name, dirname):
        # Only add a new item if the same item doesn't already exist
        try:
            subitem = self._subitems[name]
        except KeyError:
            subitem = self._subitems[name] = self._subitem_type(name, dirname)
        return subitem

    def __iter__(self):
        return iter(self._subitems.values())

    def __unicode__(self):
        return self.name

    def __str__(self):
        return self.__unicode__()

    def __repr__(self):
        return "{}: \"{}\"".format(type(self), self.name)


## TODO consider changing the default __repr__ and __str__ to project_id
class NGIProject(NGIObject):
    def __init__(self, name, dirname, project_id, base_path):
        self.base_path = base_path
        super(NGIProject, self).__init__(name, dirname, subitem_type=NGISample)
        self.samples = self._subitems
        self.add_sample = self._add_subitem
        self.project_id = project_id
        self.command_lines = []


class NGISample(NGIObject):
    def __init__(self, *args, **kwargs):
        super(NGISample, self).__init__(subitem_type=NGILibraryPrep, *args, **kwargs)
        self.libpreps = self._subitems
        self.add_libprep = self._add_subitem


class NGILibraryPrep(NGIObject):
    def __init__(self, *args, **kwargs):
        super(NGILibraryPrep, self).__init__(subitem_type=NGISeqRun, *args, **kwargs)
        self.seqruns = self._subitems
        self.add_seqrun = self._add_subitem


class NGISeqRun(NGIObject):
    def __init__(self, *args, **kwargs):
        super(NGISeqRun, self).__init__(subitem_type=None, *args, **kwargs)
        self.fastq_files = self._subitems = []
        ## Not working
        #delattr(self, "_add_subitem")

    def __iter__(self):
        return iter(self._subitems)

    def add_fastq_files(self, fastq):
        if type(fastq) == list:
            self._subitems.extend(fastq)
        elif type(fastq) == str or type(fastq) == unicode:
            self._subitems.append(str(fastq))
        else:
            raise TypeError("Fastq files must be passed as a list or a string: " \
                            "got \"{}\"".format(fastq))

@with_ngi_config
def get_engine_for_bp(project, config=None, config_file_path=None):
    """returns a analysis engine module for the given project.

    :param NGIProject project: The project to get the engine from.
    """
    charon_session = CharonSession()
    try:
        best_practice_analysis = charon_session.project_get(project.project_id)["best_practice_analysis"]
        if not best_practice_analysis:
            raise KeyError("For once in my life ever can't you just fill in the forms properly")
    except KeyError:
        error_msg = ('No best practice analysis specified in Charon for '
                     'project "{}". Using "whole_genome_reseq"'.format(project))
        LOG.error(error_msg)
        best_practice_analysis = "whole_genome_reseq"
    try:
        analysis_module = load_engine_module(best_practice_analysis, config)
    except RuntimeError as e:
        raise RuntimeError('Project "{}": {}'.format(project, e))
    else:
        return analysis_module

def load_engine_module(best_practice_analysis, config):
    try:
        analysis_engine_module_name = config["analysis"]["best_practice_analysis"][best_practice_analysis]["analysis_engine"]
    except KeyError:
        error_msg = ('No analysis engine for best practice analysis "{}" '
                     'specified in configuration file.'.format(best_practice_analysis))
        raise RuntimeError(error_msg)
    try:
        analysis_module = importlib.import_module(analysis_engine_module_name)
    except ImportError as e:
        error_msg = ('best practice analysis "{}": couldn\'t import '
                     'module "{}": {}'.format(best_practice_analysis,
                                              analysis_engine_module_name, e))
        raise RuntimeError(error_msg)
    else:
        return analysis_module


