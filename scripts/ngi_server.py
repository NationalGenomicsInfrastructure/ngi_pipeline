#!/usr/bin/env python
"""Start a nextgen analysis server that handles processing from a distributed task queue.

This reads configuration details and then starts a Celery server that will handle
requests that are passed via an external message queue. This
allows distributed processing of analysis sections with fewer assumptions about the
system architecture.
"""
import argparse

from subprocess import check_call

from ngi_pipeline.distributed.celery import app


def run_server(task_module=None):
    """ Launches the server
    """
    cl = ["celery"]
    tasks = task_module if task_module else "ngi_pipeline.distributed.celery.tasks"
    cl += ["-A", tasks, "worker", "-n", "ngi_server", "--loglevel=info"]
    check_call(cl)


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--tasks", dest="task_module", action="store",
                      default=None, help="Task module to import")
    args = parser.parse_args()
    run_server(args.task_module)
