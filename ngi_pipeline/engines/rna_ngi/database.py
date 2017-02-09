from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from ngi_pipeline.utils.classes import with_ngi_config

import contextlib

Base = declarative_base()

@with_ngi_config
def get_engine(config=None, config_file_path=None):
    """generates a SQLAlchemy engine for PostGres with the CONF currently used
    :returns: the SQLAlchemy engine"""
    uri=None
    try:
        uri="sqlite:///{db}".format(db=config['database']['record_tracking_db_path'])
    except KeyError as e:
        raise Exception("The configuration file seems to be missing a required parameter. Please read the README.md. Missing key : {}".format(e.message))
    return create_engine(uri)

@contextlib.contextmanager
def get_session():
    """Generates a SQLAlchemy session based on the CONF
    :returns: the SQLAlchemy session
    """
    engine=get_engine()
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    try:
        yield session
    finally:
        session.close()


class ProjectAnalysis(Base):
    __tablename__ = 'projectanalysis'

    project_id = Column(String(50), primary_key=True)
    project_name = Column(String(50))
    project_base_path = Column(String(100))
    workflow = Column(String(50))
    engine = Column(String(50))
    analysis_dir = Column(String(100))
    job_id = Column(Integer, primary_key=True)
    run_mode = Column(String(50))

    def __repr__(self):
        return ("<ProjectAnalysis({project_id}/: job id "
            "{job_id}, engine {engine}, "
            "workflow {workflow})>".format(project_id=self.project_id,
            job_id=(self.job_id),
            engine=self.engine,
            workflow=self.workflow))
