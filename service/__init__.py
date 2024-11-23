"""service 초기화"""
from service.redis_svc import redis_init
from service.aws_svc import s3_object_get, s3_object_delete, s3_bucket_list

redis_client = redis_init()
