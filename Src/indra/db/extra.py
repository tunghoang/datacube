from sqlalchemy import create_engine, delete, text
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

class Admin(Base):
    __tablename__ = "user_admin"
    id = Column(Integer, primary_key=True)
    id_user = Column(Integer, ForeignKey('user_account.id'))
    def __repr__(self):
        return f"isAdmin(id={self.id!r}, id_user={self.id_user!r})"

def extra_create_all():
    Base.metadata.create_all(extradb_conn())

def extra_authenticate(user, password):
    sql = f'''
SELECT email as "email", fullname as "fullname", password as "password", uad.id as "admin_id"
from user_account uac left join user_admin uad on uac.id=uad.id_user
WHERE email = '{user}'
'''
    cursor = extradb_conn().execute(text(sql))
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
SELECT uac.id as "key",  email as "email", fullname as "fullname", uad.id as "admin_id"
from user_account uac left join user_admin uad 
on uac.id = uad.id_user
limit {limit} offset {offset}
'''
    cursor = extradb_conn().execute(text(sql))
    users = cursor.fetchall()
    cursor.close()
    return users

def extra_new_user(email: str, fullname: str, password_hash: str):
    sql1 = f'''INSERT INTO user_account (email, fullname, password) values ('{email}', '{fullname}', '{password_hash}')'''
    extradb_conn().execute(text(sql1))

def extra_delete_users(userIds: List[int]):
    stm = delete(User).where(User.id.in_(userIds))
    print(stm)
    extradb_conn().execute(stm)

def extra_search_location(query: str):
    sql = f'''
SELECT gid as "gid", 
    gid_1 as "gid_1", 
    gid_2 as "gid_2", 
    gid_3 as "gid_3", 
    name_1 as "name_1", 
    name_2 as "name_2", 
    name_3 as "name_3"
    from administrative_unit 
WHERE LOWER(concat(name_3, name_2, name_1,varname_1, varname_2, varname_3)) like '%{query.lower()}%' limit 100;'''
    print(sql)
    conn = extradb_conn()
    print('conn', conn)
    cursor = conn.execute(text(sql))

    print('cursor', cursor)

    locations = cursor.fetchall()
    cursor.close()
    return locations

def extra_get_location(level, gid = None):
    sql = f'''
SELECT gid as "gid", 
    gid_1 as "gid_1", 
    gid_2 as "gid_2", 
    gid_3 as "gid_3", 
    name_1 as "name_1", 
    name_2 as "name_2", 
    name_3 as "name_3"
    from administrative_unit 
WHERE 1=1 '''
    if level < 3:
        sql += f'AND gid_{level + 1} is NULL '
    elif level == 3:
        sql += f'AND gid_{level} is NOT NULL '
    if gid: 
        sql += f'''AND gid_{level - 1} = '{gid}' '''
    print(sql)
    conn = extradb_conn()
    print('conn', conn)
    cursor = conn.execute(text(sql))

    locations = cursor.fetchall()
    cursor.close()
    return locations
    
def extra_get_feature(level, gid):
    inner_sql = f'''SELECT * FROM administrative_unit WHERE gid_{level} = '{gid}' '''
    if level < 3:
        inner_sql += f'''AND gid_{level + 1} IS NULL'''

    sql = f'''SELECT jsonb_build_object(
    'type',       'Feature',
    'id',         gid,
    'geometry',   ST_AsGeoJSON(the_geom)::jsonb,
    'properties', to_jsonb(row) - 'gid' - 'geom'
) FROM ( {inner_sql} ) row'''
    
    print(sql)
    conn = extradb_conn()
    cursor = conn.execute(text(sql))

    features = cursor.fetchall()
    cursor.close()
    print(features)
    return features[0]

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
