import time
import signal
from lib.pri_signal import sig_init
from lib.pid import write_pid
from lib.log_trace import trace_log
from service import redis_client
from service.redis_svc import queue_pop
from lib.args import get_args
from log import logger
from celery import Celery

run_mode = 'publisher'


from lib.pid import write_pid
from lib.mongo_logs import data_parse_process
from lib.aws_logs import data_parse_process

write_pid(file_path=f"/var/run/log_collector_v3_sub.pid")

celery_app = Celery('log_tasks', broker='redis://localhost:6379//')
