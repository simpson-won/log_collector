"""service library for user_cmds"""
from model.user_command import UserCommand, table_name
from lib.mysql import db_write_handle, db_read_handle
from lib.mysql import select_datas, insert_data
from service.user_access_svc import select_user_by_ctx_db_client, select_user_client_by_ctx_db_dbs
import logging


def select_value(cursor=None, value: UserCommand = None):
    """select value from user_cmds"""
    value_list = []
    values = select_datas(db_read_handle,
                          table=table_name,
                          where=value.where_all())
    for val in values:
        value_list.append(val)
    return value_list


def insert_value(values: list, cursor=None,
                 auto_commit=True, filter_str="",
                 database_name="", host="",
                 real_write=True):
    """insert single value to user_cmds"""
    if len(values) != 6:
        return
    if cursor is None:
        t_cursor = db_write_handle.cursor()
    else:
        t_cursor = cursor
    user = select_user_by_ctx_db_client(handle=db_read_handle,
                                        client=values[0],
                                        ctx=values[2])
    if user == "Unknown":
        user = select_user_by_ctx_db_client(handle=db_write_handle,
                                            client=values[0],
                                            ctx=values[2])
    values.append(user)
    values.append(filter_str)
    values.append(database_name)
    values.append(host)
    user_cmd = UserCommand.create(*values)
    if real_write:
        insert_data(conn=db_write_handle,
                    cursor=t_cursor,
                    table=table_name,
                    value=user_cmd,
                    auto_commit=auto_commit)
    else:
        logging.info("table = %s data = %s", table_name, user_cmd)

    if cursor is None:
        t_cursor.close()


def insert_value_1(values: list, cursor=None,
                   auto_commit=True, filter_str="",
                   database_name="", host="",
                   real_write=True):
    """insert value"""
    if len(values) != 5:
        return
    if cursor is None:
        t_cursor = db_write_handle.cursor()
    else:
        t_cursor = cursor
    user, client = select_user_client_by_ctx_db_dbs(handle=db_read_handle,
                                                    ctx=values[1],
                                                    database_name=database_name)
    if user is None:
        user, client = select_user_client_by_ctx_db_dbs(handle=db_write_handle,
                                                        ctx=values[1],
                                                        database_name=database_name)
    if user is None:
        user = "Unknown"
    if client is None:
        client = "Unknown"
    values.insert(0, client)
    values.append(user)
    values.append(filter_str)
    values.append(database_name)
    values.append(host)
    user_cmd = UserCommand.create(*values)
    if real_write:
        insert_data(conn=db_write_handle,
                    cursor=t_cursor,
                    table=table_name,
                    value=user_cmd,
                    auto_commit=auto_commit)
    else:
        logging.info("table = %s data = %s", table_name, user_cmd)
    if cursor is None:
        t_cursor.close()
