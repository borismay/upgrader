from sqlalchemy import create_engine
import pandas as pd
import time
import config as cfg
import logging


# GRANT ALL PRIVILEGES ON testdb.* To 'testuser'@'24.6.58.95' IDENTIFIED BY '**';

class mysql_engine:
    def __init__(self, username, password, host, db_name):
        db_log_file_name = 'db.log'
        db_handler_log_level = logging.ERROR
        db_logger_log_level = logging.ERROR

        db_handler = logging.FileHandler(db_log_file_name)
        db_handler.setLevel(db_handler_log_level)
        # db_handler.setFormatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')

        db_logger = logging.getLogger('sqlalchemy')
        db_logger.addHandler(db_handler)
        db_logger.setLevel(db_logger_log_level)
        self.engine = create_engine("mysql://%s:%s@%s/%s" % (username, password, host, db_name), echo=False)

    def get_tables(self):
        sql = 'show tables'
        return pd.read_sql(sql, self.engine)


####################################################
class aws_db(mysql_engine):
    def __init__(self):
        username = 'testuser'
        password = 'kwaY8HPqZ6'
        host = '54.68.129.46'
        db_name = 'testdb'
        mysql_engine.__init__(self, username, password, host, db_name)

class local_db(mysql_engine):
    def __init__(self):
        username = 'testuser'
        password = 'kwaY8HPqZ6'
        host = 'localhost'
        self.db_name = 'testdb'
        mysql_engine.__init__(self, username, password, host, self.db_name)

####################################################
if __name__ == '__main__':
    engine = local_db().engine
    data = pd.DataFrame(2*[1, 2, 3, 4, 563, 345, 346])
    data.to_sql('test_table', engine, if_exists='append', index=False)