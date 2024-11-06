"""user_access 테이블에 대한 조회/입력 기능"""
from log import logger
from model.user_access import UserAccess, table_name
from service import db_handle
from lib.mysql import select_datas, insert_data


def select_value(cursor=None, value: UserAccess = None):
    """모든 정보 조회"""
    value_list = []
    if cursor is None:
        t_cursor = db_handle.cursor()
    else:
        t_cursor = cursor
    values = select_datas(db_handle, cursor=t_cursor, table=table_name, where=value.where_all())
    for val in values:
        value_list.append(val)
    if cursor is None:
        t_cursor.close()
    return value_list


def select_user_client_by_ctx_db_dbs(cursor=None, ctx="", db="", database_name="") -> ():
    """ctx와 database_name 정보로 사용자 정보 조회"""
    where_clause = f"ctx=\"{ctx}\" and database_name=\"{database_name}\""
    order_clause = "order by id desc limit 1"
    if cursor is None:
        t_cursor = db_handle.cursor()
    else:
        t_cursor = cursor
    value = select_datas(db_handle,
                         cursor=t_cursor,
                         table=table_name,
                         where=where_clause,
                         order=order_clause)
    if cursor is None:
        t_cursor.close()
    if value is not None and len(value) >= 1:
        return value[0][5], value[0][1]
    logger.info("Not found user. [%s, %s]", ctx, database_name)
    return None, None


def select_user_by_ctx_db_client(cursor=None,
                                 ctx="",
                                 db="",
                                 client="",
                                 database_name="",
                                 host=""):
    """ctx와 client 정보로 사용자 정보 조회 {db}, {host}"""
    where_clause = f"ctx=\"{ctx}\" and client=\"{client}\" " +\
                   "and database_name=\"{database_name}\" " +\
                   "and host=\"{host}\""
    order_clause = "order by id desc"
    if cursor is None:
        t_cursor = db_handle.cursor()
    else:
        t_cursor = cursor
    values = select_datas(db_handle,
                          cursor=t_cursor,
                          table=table_name,
                          where=where_clause,
                          order=order_clause)
    if cursor is None:
        t_cursor.close()
    if len(values) > 0 and len(values[0]) >= 5:
        return values[0][5]
    return "Unknown"


def insert_value(values, cursor=None, auto_commit=True, real_write=True):
    """사용자 정보 입력"""
    if len(values) != 7:
        return
    if cursor is None:
        t_cursor = db_handle.cursor()
    else:
        t_cursor = cursor
    user_access = UserAccess.create(*values)
    if real_write:
        insert_data(conn=db_handle,
                    cursor=t_cursor,
                    table=table_name,
                    value=user_access,
                    auto_commit=auto_commit)
    if cursor is None:
        t_cursor.close()
