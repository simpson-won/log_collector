"""
mysql> describe user_access;
+--------+--------------+------+-----+---------+----------------+
| Field  | Type         | Null | Key | Default | Extra          |
+--------+--------------+------+-----+---------+----------------+
| id     | bigint(20)   | NO   | PRI | NULL    | auto_increment |
| client | varchar(255) | YES  |     | NULL    |                |
| ctx    | varchar(255) | YES  |     | NULL    |                |
| date   | datetime(6)  | YES  |     | NULL    |                |
| db     | varchar(255) | YES  |     | NULL    |                |
| user   | varchar(255) | YES  |     | NULL    |                |
+--------+--------------+------+-----+---------+----------------+
"""
from datetime import datetime

class_name = "UserAccess"
table_name = "user_access"


class UserAccess:
    id: int
    client: str
    ctx: str
    updated_at: datetime
    db: str
    user: str
    database_name: str
    host: str
    
    def __init__(self,
                 id: int = 0,
                 client: str = "",
                 ctx: str = "",
                 updated_at: datetime = datetime.now(),
                 db: str = "",
                 user: str = "",
                 database_name: str = "dev",
                 host: str = "localhost"):
        self.id = id
        self.client = client
        self.ctx = ctx
        self.updated_at = updated_at
        self.db = db
        self.user = user
        self.database_name = database_name
        self.host = host
    
    def where_all(self):
        return f'updated_at=\"{self.updated_at}\" and ' \
            + f'ctx=\"{self.ctx}\" and ' \
            + f'user=\"{self.user}\" and ' \
            + f'client=\"{self.client}\" and ' \
            + f'db=\"{self.db}\" and ' \
            + f'database_name=\"{self.database_name}\" and '\
            + f'host=\"{self.host}\"'
    
    def __str__(self) -> str:
        return f'\"{self.id}\", \"{self.client}\", \"{self.ctx}\", ' +\
            '\"{self.updated_at}\", \"{self.db}\", \"{self.user}\", ' +\
            '\"{self.database_name}\", \"{self.host}\"'
    
    def __eq__(self, other):
        if self.updated_at == other.updated_at and self.ctx == other.ctx \
                and self.user == other.user and self.client == other.client \
                and self.db == other.db and self.database_name == other.database_name \
                and self.host == other.host:
            return True
        return False
    
    @staticmethod
    def create(client="", ctx="", updated_at=datetime.now(),
               db="", user="", database_name="dev", host="localhost"):
        # date, ctx, cmd, client, user, db,
        return UserAccess(
            client=client,
            ctx=ctx,
            updated_at=updated_at,
            db=db,
            user=user,
            database_name=database_name,
            host=host
        )
