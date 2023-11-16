from snowflake.connector import connect
from odp_db import db

class db_snowflake(db):
    def __init__(self, *args, **kwargs):
        db.conn = connect(**kwargs)
        db.cursor = db.conn.cursor()
        db.cursor.fast_executemany = True

    def prepare(self, tb_name, t_fields):
        flds1 = ['REQID VARCHAR(30) NOT NULL', 'PAKID VARCHAR(30) NOT NULL', 'RECORD INTEGER NOT NULL']
        flds2 = ['REQID', 'PAKID', 'RECORD']
        for col in t_fields:
            t = ''
            if col['TYPE'] in ('CLNT', 'LANG', 'CHAR', 'CUKY', 'UNIT', 'DATS', 'TIMS', 'NUMC'):
                assert col['LENGTH'] == col['OUTPUTLENG']
                t = 'VARCHAR(%d)' % int(col['LENGTH'])
            elif col['TYPE'] in ('QUAN', 'CURR', 'DEC'):
                t = 'DECIMAL(%d,%d)' % (int(col['LENGTH']), int(col['DECIMALS']))
            elif col['TYPE'] in ('INT4'):
                t = 'INTEGER'
            else:
                print(col)
                assert 0 == 1
            flds1.append('%s %s not null' % (col['NAME'], t))
            flds2.append(col['NAME'])
        flds1.append('constraint PK primary key (REQID, PAKID, RECORD)')
        db.sql_ddl = 'create table if not exists %s(%s);' % (tb_name, ','.join(flds1))
        # db.sql_ddl = 'create or replace table %s(%s);' % (tb_name, ','.join(flds1))
        db.sql_insert = 'insert into %s(%s) values(%s);' % (tb_name, ','.join(flds2), ','.join(['%s'] * len(flds2)))
