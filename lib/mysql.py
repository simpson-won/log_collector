"""mysql utility library"""
import os
import threading
import time
import pymysql
import schedule
from log import logger
from config import db_port, db_user, db_passwd, db_write_host, db_read_host, db_db

CHARSET = 'utf8'
DATA_COUNT = 0
db_schedule_thread = None


db_lock = threading.Lock()


def check_handle_alive(handle):
    logger.info(f'check_handle_alive : start [{os.getpid()}, {handle}]')
    with db_lock:
        handle.ping(reconnect=True)
    logger.info(f'check_handle_alive : end [{os.getpid()}, {handle}]')


def schedule_loop():
    while True:
        schedule.run_pending()
        time.sleep(1)


def db_init(host: str, user: str, passwd: str, db: str, port: int = 3306):
    """db 연결 함수"""
    global db_schedule_thread
    logger.info(f'db_init: start [{os.getpid()}]')
    conn = pymysql.connect(host=host, user=user, port=port,
                           password=passwd, db=db, charset=CHARSET)
    schedule.every(1).minutes.do(check_handle_alive, conn)
    if db_schedule_thread is None:
        logger.info(f'db_init: create schedule_loop [{os.getpid()}]')
        db_schedule_thread = threading.Thread(target=schedule_loop)
        db_schedule_thread.start()
    logger.info(f'db_init: end [{os.getpid()}]')
    return conn


db_write_handle = db_init(host=db_write_host,
                          user=db_user,
                          passwd=db_passwd,
                          db=db_db,
                          port=db_port)


db_read_handle = db_init(host=db_read_host,
                         user=db_user,
                         passwd=db_passwd,
                         db=db_db,
                         port=db_port)


def db_fint(conn):
    """db 연결 해제"""
    if conn is not None:
        with db_lock:
            conn.close()


def select_datas(conn, table: str = "", where: str = None, order: str = None) -> []:
    """
    select data

    args:
      conn = connection
      query = input query
        ex ) query : "select * from finance.stock_list;"
    """
    with db_lock:
        cur = conn.cursor()
        datas = []
        query = f'select * from {table}'
        if where is not None and len(where) > 6:
            query = query + f' where {where}'
        if order is not None and len(order) > 8:
            query = query + " " + order
        logger.info(f'select_datas: query={query}')
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            datas.append(row)
        cur.close()
        return datas


def insert_datas(conn, cursor=None, table="", values=None, auto_commit=True):
    """
    insert data

    args:
      conn = connection
      table = string, table name
      values = string, values
    """
    with db_lock:
        if cursor is None:
            cur = conn.cursor()
        else:
            cur = cursor
        for value in values:
            query = f'insert ignore into {table} values({str(value)})'
            cur.execute(query)
        if auto_commit:
            conn.commit()


def insert_data(conn, cursor=None, table="", value=None, auto_commit=True):
    """insert data only one row"""
    global DATA_COUNT
    with db_lock:
        try:
            if cursor is None:
                cur = conn.cursor()
            else:
                cur = cursor
            query = f'insert ignore into {table} values({str(value)})'
            logger.info(f'query={query}')
            cur.execute(query)
            if auto_commit and DATA_COUNT > 1:
                conn.commit()
                DATA_COUNT = 0
            else:
                DATA_COUNT += 1
        except Exception as e:
            logger.info('insert_data: %s', e)
