from ngi_pipeline.database.classes import CharonSession

from ngi_pipeline.log.loggers import minimal_logger

LOG = minimal_logger(__name__)

def get_project_id_from_name(project_name):
    """Given the project name ("Y.Mom_14_01") return the project ID ("P123")

    :param str project_name: The human-friendly name of the project (e.g. "J.Doe_14_01")

    :returns: The alphanumeric database-friendly name of the project (e.g. "P123")
    :rtype: str

    :raises RuntimeError: If there is some problem relating to the GET (HTTP Return code != 200)
    :raises ValueError: If the project has no project id in the database or if the project does not exist in Charon
    """
    charon_session = CharonSession()

    try:
        project_id = charon_session.project_get(project_name)
    except CharonError as e:
        if e.return_code == 404:
            raise ValueError('Project "{}" missing from database: {}'.format(project_name, e))
        else:
            raise RuntimeError(e)
    try:
        return project_id['projectid']
    except KeyError:
        raise ValueError('Couldn\'t retrive project id for project "{}"; '
                         'this project\'s database entry has no "projectid" value.'.format(project))


def rebuild_project_obj_from_Charon(analysis_top_dir, project_name, project_id):
    project_dir = os.path.join(analysis_top_dir, "DATA", project_name)
    project_obj = NGIProject(name=project_name, dirname=project_name,
                                     project_id=project_id,
                                     base_path=analysis_top_dir)
    #I use the DB to build the object
    #get the samples
    charon_session = CharonSession()
    url = charon_session.construct_charon_url("samples", project_id)
    samples_response = charon_session.get(url)
    if samples_response.status_code != 200:
        error_msg = ('Error accessing database: could not get samples for projects: {}'.format(project_id,
                            project_response.reason))
        LOG.error(error_msg)
        raise RuntimeError(error_msg)
    #now I have all the samples
    samples_dict = samples_response.json()["samples"]
    for sample in samples_dict:
        sample_id = sample["sampleid"]
        sample_dir = os.path.join(project_dir, sample_id)
        sample_obj = project_obj.add_sample(name=sample_id, dirname=sample_id)
        #now get lib preps
        url = charon_session.construct_charon_url("libpreps", project_id, sample_id)
        libpreps_response = charon_session.get(url)
        if libpreps_response.status_code != 200:
            error_msg = ('Error accessing database: could not get lib preps for sample {}: {}'.format(sample_id,
                            project_response.reason))
            LOG.error(error_msg)
            raise RuntimeError(error_msg)
        libpreps_dict = libpreps_response.json()["libpreps"]
        for libprep in libpreps_dict:
            libprep_id = libprep["libprepid"]
            libprep_object = sample_obj.add_libprep(name=libprep_id,
                                                        dirname=libprep_id)
            url = charon_session.construct_charon_url("seqruns", project_id, sample_id, libprep_id)
            seqruns_response = charon_session.get(url)
            if seqruns_response.status_code != 200:
                error_msg = ('Error accessing database: could not get lib preps for sample {}: {}'.format(sample_id,
                            seqruns_response.reason))
                LOG.error(error_msg)
                raise RuntimeError(error_msg)
            seqruns_dict = seqruns_response.json()["seqruns"]
            for seqrun in seqruns_dict:
                runid = seqrun["runid"]
                #140528_D00415_0049_BC423WACXX   --> 140528_BC423WACXX
                parse_FC = re.compile("(\d{6})_(.*)_(.*)_(.*)")
                fc_short_run_id = "{}_{}".format(parse_FC.match(runid).group(1), parse_FC.match(runid).group(4))
                seqrun_object = libprep_object.add_seqrun(name=fc_short_run_id,
                                                          dirname=fc_short_run_id)
    return project_obj


def get_workflow_for_project(project_id):
    """Get the workflow that should be run for this project from the database.

    :param str project_id: The id_name of the project P\d*

    :returns: The names of the workflow that should be run.
    :rtype: str
    :raises ValueError: If the project cannot be found in the database or if the project lacks a pipeline/workflow value
    :raises RuntimeError: If there is some other database problem
    """
    charon_session = CharonSession()
    try:
        project_dict = charon_session.project_get(project_id)
    except CharonError as e:
        if e.status_code == 404:
            raise ValueError('Project "{}" not found: {}'.format(project_id, e))
        else:
            error_msg = ('Error accessing database: could not get project {}: {}'.format(project_id, e))
            raise RuntimeError(error_msg)
    if "pipeline" not in project_dict:
        error_msg = ('Project {} has no associated pipeline/workflow to execute'.format(project_id))
        raise RuntimeError(error_msg)
    return project_dict["pipeline"]
