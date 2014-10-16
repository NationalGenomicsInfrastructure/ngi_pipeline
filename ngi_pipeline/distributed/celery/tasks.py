""" Module to define Celery tasks for NGI pipeline
"""

from ngi_pipeline.distributed.celery import app

# Test tasks
@app.task()
def add_two(a=0, b=0):
    """ Sum a and b
    :param a: First term to sum
    :param b: Second term to sum
    :returns: int -- Sum of a and b
    """
    return a+b

@app.task()
def divide_by_0():
    """ Task that will fail performing a division by 0
    """
    return 1/0.

