import pymysql

charset = 'utf8'


# db connect
def db_init(db_host: str, db_user: str, db_passwd: str, db_db: str, db_port: int = 3306):
    conn = pymysql.connect(host=db_host, user=db_user, password=db_passwd, db=db_db, charset=charset)
    return conn


# close connection
def db_fint(conn):
    if conn != None:
        conn.close()


"""
select data

args:
  conn = connection
  query = input query
    ex ) query : "select * from finance.stock_list;"
"""
def select_datas(conn, cursor=None, table: str="", where: str = None, order: str = None) -> []:
    if cursor == None:
        cur = conn.cursor()
    else:
        cur = cursor
    datas = []
    query = f'select * from {table}'
    if where is not None and len(where) > 6:
        query = query + f' where {where}'

    if order is not None and len(order) > 8:
        query = query + " " + order
    print(f'select_datas: query={query}')
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        datas.append(row)
    print(f'select_datas: rows={rows}')
    return datas


"""
insert data

args:
  conn = connection
  table = string, table name
  values = string, values
"""
def insert_datas(conn, cursor=None, table="", values=None, auto_commit=True):
    if cursor == None:
        cur = conn.cursor()
    else:
        cur = cursor
    for value in values:
        query = f'insert ignore into {table} values({str(value)})'
        #print(f'query={query}')
        cur.execute(query)
    if auto_commit:
        conn.commit()


def insert_data(conn, cursor=None, table="", value=None, auto_commit=True):
    try:
        if cursor == None:
            cur = conn.cursor()
        else:
            cur = cursor
        query = f'insert ignore into {table} values({str(value)})'
        #print(f'query={query}')
        cur.execute(query)
        if auto_commit:
            conn.commit()
    except Exception as e:
        print(e)
