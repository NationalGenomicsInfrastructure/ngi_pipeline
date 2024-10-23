from ngi_pipeline.database.classes import CharonSession, CharonError

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
        if e.status_code == 404:
            new_e = ValueError(
                'Project "{}" missing from database: {}'.format(project_name, e)
            )
            new_e.status_code = 404
            raise new_e
        else:
            raise
    try:
        return project_id["projectid"]
    except KeyError:
        raise ValueError(
            'Couldn\'t retrieve project id for project "{}"; '
            'this project\'s database entry has no "projectid" value.'.format(
                project_name
            )
        )
