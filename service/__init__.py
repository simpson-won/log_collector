from service.redis import recv_from_redis, send_to_redis, redis_init, queue_pop, queue_push
from lib.mysql import db_init, db_fint, select_datas, insert_datas, insert_data
from config import db_host, db_port, db_user, db_db, db_passwd

redis_client = redis_init()
db_handle = db_init(db_host=db_host, db_user=db_user, db_passwd=db_passwd, db_db=db_db, db_port=db_port)
