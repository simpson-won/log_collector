"""
aws_logs
"""
import json
from log import logger
from lib.mongo_log_parse import log_c_process
from service import s3_object_get, s3_object_delete
from log_collector_v3_sub import celery_app


def record_process(record):
    if 's3' in record:
        record_s3 = record['s3']
        bucket_name = record_s3['bucket']['name']
        object_key = record_s3['object']['key']
        key_item = object_key.split("/")
        s3_object = s3_object_get(bucket=bucket_name, key=object_key)
        body_bytes_list = s3_object["Body"].read().decode('utf-8').split("\n")
        for body_bytes in body_bytes_list:
            body = json.loads(body_bytes)
            if "msg" in body and body["msg"] in log_c_process.keys():
                log_c_process[body["c"]](body, [key_item[2], key_item[3]], True)
        s3_object_delete(bucket=bucket_name, key=object_key)


def parse_event_msg(event_msg):
    """parse_event_msg"""
    event_msg = event_msg.replace("\'", "\"")
    event = json.loads(event_msg)
    logger.info(f'event = {event}')
    record_process(event)


def data_parse_process(data):
    """data_oarse_process"""
    try:
        if isinstance(data, [str, bytes]):
            if type(data) == bytes:
                data = data.decode('utf-8')
            if data.startswith('{'):
                parse_event_msg(data)
    except Exception as e:
        logger.error('data_parse_process: Exception\n\t\t%s', e)


@celery_app.task
def celery_task(data):
    """celery task"""
    data_parse_process(data)
