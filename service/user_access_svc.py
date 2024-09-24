from model.user_access import UserAccess, table_name
from service import db_handle, select_datas, insert_data
from log import logger


def select_value(cursor=None, value: UserAccess = None):
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


def select_user_by_ctx_db_client(cursor=None, ctx="", db="", client=""):
    where_clause = f"ctx=\"{ctx}\" and db=\"{db}\" and client=\"{client}\""
    order_cluse = "order by id desc"
    
    logger.info(f'select_user_by_ctx_db_client : cursor={cursor} ctx={ctx} db={db} client={client}')
    
    if cursor is None:
        t_cursor = db_handle.cursor()
    else:
        t_cursor = cursor
    
    values = select_datas(db_handle, cursor=t_cursor, table=table_name, where=where_clause, order=order_cluse)
    
    if cursor is None:
        t_cursor.close()

    logger.info(f'select_user_by_ctx_db_client: values={values}, {len(values)}')

    if len(values) > 0 and len(values[0])==6:
        return values[0][5]
    return "Unknown"


def insert_value(values, cursor=None, auto_commit=True):
    # date, ctx, cmd, client, user, db,
    logger.info(f'insert_ac_value: values={values}')

    if len(values) != 6:
        return
    
    if cursor is None:
        t_cursor = db_handle.cursor()
    else:
        t_cursor = cursor
    
    user_access = UserAccess.create(*values)

    ret = select_value(cursor=cursor, value=user_access)

    logger.info(f'insert_ac_value: user_access={user_access}')
    
    if len(ret) == 0:
        insert_data(conn=db_handle, cursor=cursor, table=table_name, value=user_access, auto_commit=auto_commit)
    
    if cursor is None:
        t_cursor.close()
