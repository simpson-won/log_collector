"""
log_collector_v3_sub
"""
from celery import Celery

celery_app = Celery('log_tasks',
                    broker='redis://localhost:6379//',
                    include=['lib.aws_logs',
                             'lib.mongo_logs'])
