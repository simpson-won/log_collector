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
    date: datetime
    db: str
    user: str
    
    def __init__(self,
                 id: int = 0,
                 client: str = "",
                 ctx: str = "",
                 date: datetime = datetime.now(),
                 db: str = "",
                 user: str = ""):
        self.id = id
        self.client = client
        self.ctx = ctx
        self.date = date
        self.db = db
        self.user = user
    
    def where_all(self):
        return f'date=\"{self.date}\" and ' \
            + 'ctx=\"{self.ctx}\" and ' \
            + 'user=\"{self.user}\" and ' \
            + 'client=\"{self.client}\" and ' \
            + 'db=\"{self.db}\"'
    
    def __str__(self) -> str:
        return f'\"{self.id}\", \"{self.client}\", \"{self.ctx}\", \"{self.date}\", \"{self.db}\", \"{self.user}\"'
    
    def __eq__(self, other):
        if self.date == other.date and self.ctx == other.ctx \
                and self.user == other.user and self.client == other.client and self.db == other.db:
            return True
        else:
            False
    
    @classmethod
    def create(cls, client="", ctx="", updated=datetime.now(), db="", user=""):
        return UserAccess(
            client=client,
            ctx=ctx,
            date=updated,
            db=db,
            user=user
        )
