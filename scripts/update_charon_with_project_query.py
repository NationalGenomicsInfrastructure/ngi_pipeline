#!/bin/env python

import argparse
import csv
import os
from functools import reduce

from ngi_pipeline.engines.sarek import local_process_tracking
from ngi_pipeline.engines.sarek.database import TrackingConnector
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config

LOG = minimal_logger(__name__, debug=True)


class DiskTrackingSession(object):
    """
    This is an object to replace the SQLAlchemy object injected into the TrackingConnector, in order to replace the
    database connections
    """
    def __init__(self, analyses=None):
        self.analyses = analyses or list()

    def add(self, db_obj):
        self.analyses.append(db_obj)

    def all(self, *args, **kwargs):
        for analysis in self.analyses:
            yield analysis

    def commit(self, *args, **kwargs):
        pass

    def delete(self, *args, **kwargs):
        pass

    def filter(self, *args, **kwargs):
        return self

    def query(self, *args, **kwargs):
        return self


def locate_tsv(dirpath):
    if os.path.basename(dirpath) == "SarekGermlineAnalysis":
        return [
            os.path.join(dirpath, f)
            for f in filter(lambda x: x.endswith(".tsv"), os.listdir(dirpath))]
    try:
        return reduce(lambda x, y: x + y,
                      map(lambda d: locate_tsv(d),
                          filter(lambda x: os.path.isdir(x),
                                 map(lambda f: os.path.join(dirpath, f),
                                     os.listdir(dirpath)))))
    except TypeError:
        return []


def samples_from_tsv(tsvfile):
    with open(tsvfile) as fh:
        reader = csv.reader(fh, dialect=csv.excel_tab)
        return list(set([sample[0] for sample in reader]))


def update_charon_with_sample(
        db_session, project_base_path, project_id, sample_id, limit_to_sample):

    # skip if we are only to add a specified sample and this is not it
    if limit_to_sample is not None and limit_to_sample != sample_id:
        return

    db_session.add(
        TrackingConnector._SampleAnalysis(
            project_id=project_id,
            project_name=project_id,
            sample_id=sample_id,
            project_base_path=project_base_path,
            workflow="SarekGermlineAnalysis",
            engine="sarek",
            process_id=999999)
    )


@with_ngi_config
def update_charon_with_project(project, sample=None, config=None, config_file_path=None):
    project_base_path = os.path.join(
        config["analysis"]["base_root"],
        config["analysis"]["upps_root"],
        config["analysis"]["top_dir"])

    project_analysis_dir = os.path.join(
        project_base_path,
        "ANALYSIS",
        project)

    db_session = DiskTrackingSession()

    for tsvfile in locate_tsv(project_analysis_dir):
        sample_ids = samples_from_tsv(tsvfile)
        for sample_id in sample_ids:
            update_charon_with_sample(
                db_session,
                project_base_path,
                project,
                sample_id,
                sample)

    tracking_connector = TrackingConnector(
        config,
        LOG,
        tracking_session=db_session)
    local_process_tracking.update_charon_with_local_jobs_status(
        config=config,
        log=LOG,
        tracking_connector=tracking_connector)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update Charon with Sarek analysis status and analysis results for a project independent of the "
                    "local processing db")

    parser.add_argument("-p", "--project", required=True)
    parser.add_argument("-s", "--sample", required=False)
    parser.add_argument("-c", "--config", required=False)

    args = parser.parse_args()
    update_charon_with_project(args.project, sample=args.sample, config_file_path=args.config)
