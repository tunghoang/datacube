from sqlalchemy import create_engine, delete
from os import environ

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer, String, Boolean
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship

from typing import List
from sqlalchemy.schema import MetaData
from utils import *

USERDB_CONN_STR = f'postgresql://{environ.get("DB_USERNAME")}:{environ.get("DB_PASSWORD")}@{environ.get("DB_HOSTNAME")}/indra_extra'
__extradb_conn = None

def extradb_conn():
    global __extradb_conn
    print(USERDB_CONN_STR)
    if __extradb_conn == None:
        __extradb_conn = create_engine(USERDB_CONN_STR).connect()
    return __extradb_conn

Base = declarative_base()

class User(Base):
    __tablename__ = "user_account"
    id = Column(Integer, primary_key=True)
    email = Column(String(100), unique=True)
    fullname = Column(String)
    status = Column(Boolean, unique=False, default=True)

    password = Column(String, nullable=False)

    def __repr__(self):
        return f"User(id={self.id!r}, email={self.email!r}, fullname={self.fullname!r})"

def extra_create_all():
    Base.metadata.create_all(extradb_conn())

def extra_authenticate(user, password):
    sql = f'''
SELECT email as "email", fullname as "fullname", password as "password" from user_account WHERE email = '{user}'
'''
    cursor = extradb_conn().execute(sql)
    records = [r._asdict() for r in cursor]
    if len(records) == 0: 
        return None, -1
    
    userRecord = records[0]
    cursor.close()
    if userRecord['password'] == password:
        return userRecord, 0
    return userRecord, -2

def extra_list_users(limit: int, offset=int):
    sql = f'''
SELECT id as "key",  email as "email", fullname as "fullname" from user_account limit {limit} offset {offset}
'''
    cursor = extradb_conn().execute(sql)
    users = cursor.fetchall()
    cursor.close()
    return users

def extra_new_user(email: str, fullname: str, password_hash: str):
    sql1 = f'''INSERT INTO user_account (email, fullname, password) values ('{email}', '{fullname}', '{password_hash}')'''
    extradb_conn().execute(sql1)

def extra_delete_users(userIds: List[int]):
    stm = delete(User).where(User.id.in_(userIds))
    print(stm)
    extradb_conn().execute(stm)

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument("--insert-user", action="store_true")
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--email")
    parser.add_argument("--fullname")
    parser.add_argument("--password")
    args = parser.parse_args()
    if args.init:
        extra_create_all()
    elif args.insert_user and args.email and args.fullname and args.password:
        extra_new_user(args.email, args.fullname, hash_password(args.password))
    else:
        parser.print_help()
