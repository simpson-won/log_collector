"""user_access 테이블에 대한 조회/입력 기능"""
from log import logger
from model.user_access import UserAccess, table_name


def select_value(handle=None, value: UserAccess = None):
    """모든 정보 조회"""
    from lib.mysql import db_read_handle, select_datas
    value_list = []
    if handle is None:
        cur_handle = db_read_handle
    else:
        cur_handle = handle
    values = select_datas(cur_handle, table=table_name, where=value.where_all())
    for val in values:
        value_list.append(val)
    return value_list


def select_user_client_by_ctx_db_dbs(handle=None, ctx="", database_name="") -> ():
    """ctx와 database_name 정보로 사용자 정보 조회"""
    from lib.mysql import db_read_handle, select_datas
    where_clause = f"ctx=\"{ctx}\" and database_name=\"{database_name}\""
    order_clause = "order by id desc limit 1"
    if handle is None:
        cur_handle = db_read_handle
    else:
        cur_handle = handle
    value = select_datas(cur_handle,
                         table=table_name,
                         where=where_clause,
                         order=order_clause)
    if value is not None and len(value) >= 1:
        return value[0][5], value[0][1]
    logger.info("Not found user. [%s, %s]", ctx, database_name)
    return None, None


def select_user_by_ctx_db_client(handle=None, ctx="", client=""):
    """ctx와 client 정보로 사용자 정보 조회 {db}, {host}"""
    from lib.mysql import db_read_handle, select_datas
    where_clause = f"ctx=\"{ctx}\" and client=\"{client}\""
    order_clause = "order by id desc"
    if handle is None:
        cur_handle = db_read_handle
    else:
        cur_handle = handle
    values = select_datas(cur_handle,
                          table=table_name,
                          where=where_clause,
                          order=order_clause)
    if len(values) > 0 and len(values[0]) >= 5:
        return values[0][5]
    return "Unknown"


def insert_value(values, cursor=None, auto_commit=True, real_write=True):
    """사용자 정보 입력"""
    from lib.mysql import db_write_handle, insert_data
    if len(values) != 7:
        return
    if cursor is None:
        t_cursor = db_write_handle.cursor()
    else:
        t_cursor = cursor
    user_access = UserAccess.create(*values)
    if real_write:
        insert_data(conn=db_write_handle,
                    cursor=t_cursor,
                    table=table_name,
                    value=user_access,
                    auto_commit=auto_commit)
    if cursor is None:
        t_cursor.close()
