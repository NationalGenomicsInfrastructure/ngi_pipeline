"""Top level functionality for running a ngi_pipeline web server allowing remote jobs.
"""
import tornado.web
import tornado.ioloop

from ngi_pipeline.server import handlers

def start(port):
    """Run server with provided command line arguments.
    """
    application = tornado.web.Application([(r"/flowcell_analysis/([^/]*)$", handlers.FlowcellHandler),
                                           (r"/status", handlers.StatusHandler),
                                           (r"/test/([0-9]*)$", handlers.TestHandler),
                                           (r"/test_flowcell_analysis/([^/]*)$", handlers.TestFlowcellHandler)
                                           ])
    application.runmonitor = RunMonitor()
    application.listen(port)
    tornado.ioloop.IOLoop.instance().start()

class RunMonitor:
    """Track current runs and provide status.
    """
    def __init__(self):
        self._running = {}

    def set_status(self, run_id, status):
        self._running[run_id] = status

    def get_status(self, run_id):
        return self._running.get(run_id, "not-running")
