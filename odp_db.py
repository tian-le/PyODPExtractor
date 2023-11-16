import time

class db:
    conn = None
    cursor = None
    sql_ddl = None
    sql_insert = None
    tt = 0

    def create_table(self):
        if not self.sql_ddl:
            raise Exception('Run prepare first!')
        self.cursor.execute(self.sql_ddl)

    def insert_data(self, rows):
        if not self.sql_insert:
            raise Exception('Run prepare first!')
        print('db call begin')
        t = time.time()
        self.cursor.executemany(self.sql_insert, rows)
        print('db call end')
        print(time.time() - t)
        self.tt += time.time() - t
