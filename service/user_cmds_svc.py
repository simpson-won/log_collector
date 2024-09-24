from model.user_command import UserCommand, table_name
from service import db_handle, select_datas, insert_data
from service.user_access_svc import select_user_by_ctx_db_client
from log import logger


def select_market_value(cursor=None, value: UserCommand = None):
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


def insert_value(values: list, cursor=None, auto_commit=True):
    # date, ctx, cmd, client, user, db,
    logger.info(f'insert_ac_value: values = {values}, cursor={cursor} auto_commit={auto_commit}')
    if len(values) != 6:
        return
    
    if cursor is None:
        t_cursor = db_handle.cursor()
    else:
        t_cursor = cursor
    
    # [client, cmd, ctx, date, db, table]
    user = select_user_by_ctx_db_client(cursor=t_cursor, client=values[0], db=values[4], ctx=values[2])
    logger.info(f'insert_ac_value:  user_access={user}, {type(user)}')
    values.append(user)

    user_cmd = UserCommand.create(*values)

    ret = select_market_value(cursor=t_cursor, value=user_cmd)
    
    if len(ret) == 0:
        insert_data(conn=db_handle, cursor=t_cursor, table=table_name, value=user_cmd, auto_commit=auto_commit)
    
    if cursor is None:
        t_cursor.close()
