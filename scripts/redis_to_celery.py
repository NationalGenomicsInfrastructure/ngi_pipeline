import argparse

import json
import redis

import ngi_pipeline.distributed.celery.tasks as tasks
from ngi_pipeline.distributed.celery.tasks import *

DESCRIPTION="""
 This script will pick up messages from a Redis database and execute the corresponding
 Celery task, defined in the tasks module. The format of the messages should be:

 {'task_name': name_of_the_task, 'args': {dict_of_taks_arguments}}

 The purpose of this script is to ease Uppsala the task of starting tasks from
 their Scala applications.
"""

if __name__=="__main__":

    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('--key', help='Redis database key', type=str, default='uppsala')
    args = parser.parse_args()
    rc = redis.StrictRedis(host='localhost', port=6379, db=0)

    # Build a dictionary of available tasks
    tasks_names = [task_name for task_name in dir(tasks)]
    # You may be loading objects and modules together with tasks that are not
    # on your locals()
    tasks_dict = {}
    for task in tasks_names:
        try:
            tasks_dict[task] = locals()[task]
        except KeyError:
            pass

    while rc.llen(args.key):
        redis_task = json.loads(rc.lpop(args.key))
        celery_task = tasks_dict[redis_task['task_name']]
        celery_task.delay(**redis_task['args'])

