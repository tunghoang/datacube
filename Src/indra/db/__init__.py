from sqlalchemy import create_engine
from os import environ

CONN_STR = f'postgresql://{environ.get("DB_USERNAME")}:{environ.get("DB_PASSWORD")}@{environ.get("DB_HOSTNAME")}/{environ.get("DB_DATABASE")}'
__db_conn = None

def db_conn():
    global __db_conn
    print(CONN_STR)
    if __db_conn == None:
        __db_conn = create_engine(CONN_STR).connect()
    return __db_conn

def db_get_datetime_limits(product:str):
    sql = f"""select 
                max((metadata::jsonb->>'properties')::jsonb->>'datetime') as "max", 
                min((metadata::jsonb->>'properties')::jsonb->>'datetime') as "min"
                from agdc.dataset 
                where (metadata::jsonb->>'product')::jsonb->>'name' = '{product}';"""
    result = None
    cursor = db_conn().execute(sql)
    records = cursor.fetchall()
    result = records[0]
    cursor.close()
    
    return result
