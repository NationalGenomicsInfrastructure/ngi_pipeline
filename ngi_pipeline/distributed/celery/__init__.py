""" Celery module for NGI-pipeline
"""
from celery import Celery

app = Celery('ngi_celery_server',
             broker='redis://localhost:6379/0',
             backend='redis://localhost:6379/0')
