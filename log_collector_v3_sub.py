"""
log_collector_v3_sub
"""
from celery import Celery
from config import BROKER_ADDRESS

celery_app = Celery('log_tasks',
                    broker=BROKER_ADDRESS,
                    include=['lib.aws_logs',
                             'lib.mongo_logs'])
