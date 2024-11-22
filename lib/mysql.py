"""mysql utility library"""
import pymysql
from log import logger

CHARSET = 'utf8'


DATA_COUNT = 0


def db_init(db_host: str, db_user: str, db_passwd: str, db_db: str, db_port: int = 3306):
    """db 연결 함수"""
    conn = pymysql.connect(host=db_host, user=db_user, port=db_port,
                           password=db_passwd, db=db_db, charset=CHARSET)
    return conn


def db_fint(conn):
    """db 연결 해제"""
    if conn is not None:
        conn.close()


def select_datas(conn, table: str = "", where: str = None, order: str = None) -> []:
    """
    select data

    args:
      conn = connection
      query = input query
        ex ) query : "select * from finance.stock_list;"
    """
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
    # logger.info(f'select_datas: rows={rows}')
    return datas


def insert_datas(conn, cursor=None, table="", values=None, auto_commit=True):
    """
    insert data

    args:
      conn = connection
      table = string, table name
      values = string, values
    """
    if cursor is None:
        cur = conn.cursor()
    else:
        cur = cursor
    for value in values:
        query = f'insert ignore into {table} values({str(value)})'
        # logger.info(f'query={query}')
        cur.execute(query)
    if auto_commit:
        conn.commit()


def insert_data(conn, cursor=None, table="", value=None, auto_commit=True):
    """insert data only one row"""
    global DATA_COUNT
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
