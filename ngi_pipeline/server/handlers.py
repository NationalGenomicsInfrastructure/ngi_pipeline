"""Provide ability to run bcbio-nextgen workflows.
"""
import collections
import os
import StringIO
import sys
import uuid

import tornado.gen
import tornado.web
import yaml

from ngi_pipeline.server import background


##################################
#          Handlers              #
##################################
class NGIHandler(tornado.web.RequestHandler):
    """ Generic Handler for calling NGI pipeline methods.
    """
    def run_ngi_pipeline(self, args, callback=None):
        """ Runs ngi_pipeline_start.py with the given args
        """
        run_id = str(uuid.uuid1())
        def set_done(status, stdout, stderr, has_timed_out):
            self.application.runmonitor.set_status(run_id, "finished" if status == 0 else "failed")
        self._run_local(args, set_done)
        self.application.runmonitor.set_status(run_id, "running")
        if callback:
            callback(run_id)
        else:
            return run_id

    def _run_local(self, args, callback):
        cmd = [os.path.join(os.path.dirname(sys.executable), "ngi_pipeline_start.py")] + args
        p = background.Subprocess(callback, timeout=-1, args=[str(x) for x in cmd])
        p.start()


class FlowcellHandler(NGIHandler):
    """ Handler to manage flowcell processing

    GET /flowcell_analysis/(fc_dir)?path=path_to_run
    """
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self, fc_dir):
        base_path = self.get_argument('path', None)
        if base_path:
            fc_dir = os.path.join(base_path, fc_dir)
        args = ['analyze', 'flowcell', fc_dir]
        run_id = yield tornado.gen.Task(self.run_ngi_pipeline, args)
        self.write(run_id)
        self.finish()


class TestHandler(tornado.web.RequestHandler):
    """ Execute a 'sleep' command
    """
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self, secs):
        run_id = str(uuid.uuid1())
        def set_done(status, stdout, stderr, has_timed_out):
            self.application.runmonitor.set_status(run_id, "finished" if status == 0 else "failed")
        cmd = ['sleep', secs]
        p = background.Subprocess(set_done, timeout=-1, args=[str(x) for x in cmd])
        p.start()
        self.application.runmonitor.set_status(run_id, "running")

        self.write(run_id)


class TestFlowcellHandler(tornado.web.RequestHandler):

    def get(self, runfolder_name):
        if not runfolder_name:
            raise AttributeError("Need to provide a runfolder name")
        self.write("Test started: {}".format(runfolder_name))
        self.finish()


class StatusHandler(tornado.web.RequestHandler):
    def get(self):
        run_id = self.get_argument("run_id", None)
        if run_id is None:
            status = "server-up"
        else:
            status = self.application.runmonitor.get_status(run_id)
        self.write(status)
        self.finish()
