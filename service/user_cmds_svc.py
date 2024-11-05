from model.user_command import UserCommand, table_name
from service import db_handle
from lib.mysql import select_datas, insert_data
from service.user_access_svc import select_user_by_ctx_db_client, select_user_client_by_ctx_db_dbs
import logging


def select_value(cursor=None, value: UserCommand = None):
    value_list = []
    if cursor is None:
        t_cursor = db_handle.cursor()
    else:
        t_cursor = cursor
    values = select_datas(db_handle,
                          cursor=t_cursor,
                          table=table_name,
                          where=value.where_all())
    for val in values:
        value_list.append(val)
    if cursor is None:
        t_cursor.close()
    return value_list


def insert_value(values: list, cursor=None, auto_commit=True, filter_str="", database_name="", host="", real_write=True):
    if len(values) != 6:
        return
    if cursor is None:
        t_cursor = db_handle.cursor()
    else:
        t_cursor = cursor
    user = select_user_by_ctx_db_client(cursor=t_cursor,
                                        client=values[0],
                                        db=values[4],
                                        ctx=values[2],
                                        database_name=database_name,
                                        host=host)
    values.append(user)
    values.append(filter_str)
    values.append(database_name)
    values.append(host)
    user_cmd = UserCommand.create(*values)
    if real_write:
        insert_data(conn=db_handle,
                    cursor=t_cursor,
                    table=table_name,
                    value=user_cmd,
                    auto_commit=auto_commit)
    else:
        logging.info(f"table = {table_name} data = {user_cmd}")

    if cursor is None:
        t_cursor.close()


def insert_value_1(values: list, cursor=None, auto_commit=True, filter_str="", database_name="", host="", real_write=True):
    if len(values) != 5:
        return
    if cursor is None:
        t_cursor = db_handle.cursor()
    else:
        t_cursor = cursor
    user, client = select_user_client_by_ctx_db_dbs(cursor=t_cursor,
                                                    db=values[3],
                                                    ctx=values[1],
                                                    database_name=database_name)
    values.insert(0, client)
    values.append(user)
    values.append(filter_str)
    values.append(database_name)
    values.append(host)
    user_cmd = UserCommand.create(*values)
    if real_write:
        insert_data(conn=db_handle,
                    cursor=t_cursor,
                    table=table_name,
                    value=user_cmd,
                    auto_commit=auto_commit)
    else:
        logging.info(f"table = {table_name} data = {user_cmd}")

    if cursor is None:
        t_cursor.close()