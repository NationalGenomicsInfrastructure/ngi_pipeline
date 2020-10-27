import collections
import os
import shlex
import shutil
import subprocess
import tempfile
import unittest

#from ngi_pipeline.conductor.flowcell import process_demultiplexed_flowcells
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.config import load_yaml_config, locate_ngi_config
from ngi_pipeline.utils.filesystem import locate_project, recreate_project_from_filesystem
from ngi_pipeline.utils.pyutils import update_dict

LOG = minimal_logger(__name__)

class AutomatedAnalysisTest(unittest.TestCase):
    ## Not used / WIP
    def _install_test_files(self, workflow_dict):
        """Download required sequence and reference files."""
        url = workflow_dict.get("remote_url")
        local_parent_dir = workflow_dict.get("local_parent_dir")
        if url and local_parent_dir:
            dest_dir = os.path.join(local_parent_dir,
                                    os.path.basename(url).replace(".tar.gz", ""))
            if not os.path.exists(dest_dir):
                self._download_to_dir(url, dest_dir)

    ## Not used / WIP
    def _download_to_dir(self, url, dest_dir):
        tmp_dir = tempfile.mkdtemp()
        output_dir = os.path.join(tmp_dir, os.path.basename(url))
        cl = "wget -P {} {}".format(tmp_dir, url)
        subprocess.check_call(shlex.split(cl))
        if not os.path.exists(os.path.dirname(dest_dir)):
            os.makedirs(os.path.dirname(dest_dir))
        if output_dir.endswith(".tar.gz") or output_dir.endswith(".tar.gzip"):
            cl = "tar -C {} -xzvf {}".format(os.path.dirname(dest_dir), output_dir)
            subprocess.check_call(shlex.split(cl))
        else:
            shutil.move(output_dir, dest_dir)
        shutil.rmtree(tmp_dir)

    def test_workflows(self):
        config_file_path = locate_ngi_config()
        config = load_yaml_config(config_file_path)

        for workflow_name, workflow_dict in config.get("test_data", {}).get("workflows", {}).iteritems():
            # Load and rewrite config file as needed
            customize_config_dict = workflow_dict.get("customize_config")
            if customize_config_dict:
                config = update_dict(config, customize_config_dict)

            #self._install_test_files(workflow_dict)
            LOG.info('Starting test analysis pipeline for workflow "{}"'.format(workflow_name))
            try:
                local_files = workflow_dict["local_files"]
            except KeyError:
                raise ValueError("Required paths to input files for testing do not"
                                 "exist in config file (test_data.workflows."
                                 "{}.local_files); cannot proceed.".format(workflow_name))
            try:
                flowcell_path = local_files["flowcell"]
            except KeyError:
                raise ValueError("Path to flowcell is required and not specified "
                                 "in configuration file (test_data.workflows."
                                 "{}.local_files.flowcell); cannot proceed.".format(workflow_name))
            try:
                test_project = workflow_dict["test_project"]
                test_proj_id = test_project["project_id"]
                test_proj_name = test_project["project_name"]
                test_proj_bpa = test_project["bpa"]
            except KeyError as e:
                raise ValueError("Test project information is missing from config "
                                 "file (under test_data.workflows.{}.test_project "
                                 "({}); cannot proceed.".format(workflow_name, e.msg))
            charon_session = CharonSession(config=config)
            try:
                charon_session.project_delete(projectid=test_proj_id)
            except CharonError:
                pass
            charon_session.project_create(projectid=test_proj_id, name=test_proj_name,
                                          status="OPEN", best_practice_analysis=test_proj_bpa)

            #process_demultiplexed_flowcells([flowcell_path], fallback_libprep="A",
            #                                config=config)
