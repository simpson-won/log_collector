from service.redis_svc import redis_init
from lib.mysql import db_init
from config import db_host, db_port, db_user, db_db, db_passwd
from service.aws_svc import s3_object_get, s3_object_delete, s3_bucket_list

redis_client = redis_init()
db_handle = db_init(db_host=db_host,
                    db_user=db_user,
                    db_passwd=db_passwd,
                    db_db=db_db,
                    db_port=db_port)
