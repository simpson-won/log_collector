"""service 초기화"""
from service.redis_svc import redis_init
from lib.mysql import db_init
from config import db_port, db_user, db_db, db_passwd, db_write_host, db_read_host
from service.aws_svc import s3_object_get, s3_object_delete, s3_bucket_list

redis_client = redis_init()
db_write_handle = db_init(db_host=db_write_host,
                          db_user=db_user,
                          db_passwd=db_passwd,
                          db_db=db_db,
                          db_port=db_port)

db_read_handle = db_init(db_host=db_read_host,
                         db_user=db_user,
                         db_passwd=db_passwd,
                         db_db=db_db,
                         db_port=db_port)
