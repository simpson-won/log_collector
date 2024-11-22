"""main for vm"""
import json
from log import logger
from lib.mongo_log_parse import log_c_process
from config import mongodb_host, mongodb_name

from log_collector_v3_sub import celery_app


def data_parse_process(data):
    """Main function for msg"""
    try:
        if isinstance(data, (bytes, str)):
            if isinstance(data, bytes):
                data = data.decode('utf-8')
            if data.startswith('{'):
                body = json.loads(str(data))
                if body["c"] in log_c_process:
                    log_c_process[body["c"]](body, [mongodb_name, mongodb_host], True)
    except Exception as e:
        logger.error('data_parse_process: Exception\n\t\t%s', e)


@celery_app.task
def celery_task(data):
    data_parse_process(data=data)
