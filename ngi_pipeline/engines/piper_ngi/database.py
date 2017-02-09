"""sqlalchemy interface to a (currently) sqlite database for tracking running processes"""

import os
import contextlib

from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


LOG = minimal_logger(__name__)

# Declare the base class
Base = declarative_base()
Session = sessionmaker()


@contextlib.contextmanager
@with_ngi_config
def get_db_session(database_path=None, config=None, config_file_path=None):
    """Return a session connection to the database."""
    if not database_path:
        database_path = config['database']['record_tracking_db_path']
    database_abspath = os.path.abspath(database_path)
    if not os.path.exists(database_abspath):
        LOG.info('Creating local job tracking database "{}"'.format(database_path))
        # This needs to be called somehow even if the engine already exists
        # possibly this should be module-level
        try:
            engine = create_database_populate_schema(database_abspath)
        except OperationalError as e:
            raise RuntimeError("Could not create database at {}: {}".format(database_abspath, e))
    else:
        LOG.debug('Local job tracking database at "{}" already exists; '
                  'connecting.'.format(database_abspath))
        engine = _init_engine(database_abspath)
    # Bind the Session to the engine
    Session.configure(bind=engine)
    # Instantiate
    session = Session()
    try:
        yield session
    finally:
        session.close()


def _init_engine(database_path):
    """Create the engine connection."""
    database_abspath = os.path.abspath(database_path)
    return create_engine('sqlite:///{}'.format(database_abspath))
    #return create_engine('sqlite:///:memory:', echo=True)


def create_database_populate_schema(location):
    """Create the database and populate it with the schema."""
    engine = _init_engine(location)
    # Create the folder if necessary
    if not os.path.exists( os.path.dirname(location) ):
        try:
            os.makedirs( os.path.dirname(location)) 
        except (OSError):
            LOG.info('Failed to create database directory at "{}", continuing without local database'.format(os.path.dirname(location)))
            pass 
    # Create the tables & sqlite file
    Base.metadata.create_all(engine)
    return engine


class SampleAnalysis(Base):
    __tablename__ = 'sampleanalysis'

    project_id = Column(String(50), primary_key=True)
    project_name = Column(String(50))
    project_base_path = Column(String(100))
    sample_id = Column(String(50), primary_key=True)
    workflow = Column(String(50), primary_key=True)
    engine = Column(String(50))
    analysis_dir = Column(String(100))
    # Only one of these is ever used
    process_id = Column(Integer)
    slurm_job_id = Column(Integer)

    def __repr__(self):
        return ("<SampleRunAnalysis({project_id}/{sample_id}: job id "
                "{job_id}, engine {engine}, "
                "workflow {workflow})>".format(project_id=self.project_id,
                                                           sample_id=self.sample_id,
                                                           job_id=(self.slurm_job_id or self.process_id),
                                                           engine=self.engine,
                                                           workflow=self.workflow))
