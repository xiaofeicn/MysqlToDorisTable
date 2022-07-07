# encoding=utf8
from MysqlHelper import MysqlHelper
from db_config import doris_config, mysql_config


class MysqlToDoris(object):
    def __init__(self, mysql_config, doris_config, regexps):
        self.mysql_db = MysqlHelper(mysql_config)
        self.doris_db = MysqlHelper(doris_config)
        self.regexps = str(regexps).split(",")

    def mysql_doris(self):

        sync_tables = self.doris_db.select(
            "select member_id,sync_table from sync_member.sync_table")
        database_name_dup = set()
        for sync_table in sync_tables:
            database_name = sync_table[0]
            database_name_dup.add(database_name)
        for database_name in database_name_dup:
            crete_sql = '''CREATE DATABASE IF NOT EXISTS {}'''.format(database_name)
            print(crete_sql)
            self.doris_db.execute_sql(crete_sql)
        print("-" * 20)
        print("需同步企业库: {} 个".format(len(database_name_dup)))
        print("需同步企业表: {} 个".format(len(sync_tables)))
        print("-" * 20)
        for sync_table in sync_tables:
            database_name = sync_table[0]
            table_name = sync_table[1]
            # print(database_name, table_name)
            sql = '''select COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,COLUMN_TYPE,COLUMN_KEY,COLUMN_COMMENT from information_schema.columns where table_schema ='{}' and table_name ='{}' '''.format(
                database_name, table_name)
            table_desc = self.mysql_db.select(sql)
            AGGR_TYPE = ""

            # 主键
            PRI = []
            # 索引
            MUL = []
            COLUMN_List = []
            COLUMN_List_HEADE = []
            for col in table_desc:
                COLUMN_NAME = str(col[0])
                DATA_TYPE = str(col[1])
                COLUMN_TYPE = str(col[3]).replace('unsigned', '')
                if COLUMN_TYPE == 'text' or COLUMN_TYPE == 'longtext':
                    COLUMN_TYPE = 'string'
                elif DATA_TYPE == 'varchar':
                    CHARACTER_MAXIMUM_LENGTH = int(col[2])
                    TYPE_LEN = CHARACTER_MAXIMUM_LENGTH * 3
                    COLUMN_TYPE = 'varchar({})'.format(int(TYPE_LEN) * 3)
                elif DATA_TYPE.startswith("float"):
                    COLUMN_TYPE = "float"
                elif DATA_TYPE.startswith("varbinary"):
                    COLUMN_TYPE = 'string'
                COLUMN_KEY = col[4]
                COLUMN_COMMENT = str(col[5]).replace("'", "\"")
                if COLUMN_COMMENT is None:
                    COLUMN_COMMENT = ''
                if COLUMN_KEY == 'PRI':
                    PRI.append('''`{}`'''.format(str(COLUMN_NAME)))
                elif COLUMN_KEY == 'MUL':
                    MUL.append(COLUMN_NAME)
                if COLUMN_KEY != 'PRI':
                    AGGR_TYPE = "REPLACE"
                COL_ = ''' `{}` {} COMMENT '{}' '''.format(COLUMN_NAME, COLUMN_TYPE, COLUMN_COMMENT)
                if COLUMN_NAME == 'id' or COLUMN_NAME == 'aid':
                    COLUMN_List.insert(0, col)
                elif COLUMN_NAME == 'create_time':
                    COLUMN_List.insert(1, col)
                elif COLUMN_NAME == 'update_time':

                    COLUMN_List.insert(2, col)
                else:
                    COLUMN_List.append(COL_)
            cols = ','.join(COLUMN_List)
            pri_keys = ''
            head = 'CREATE TABLE IF NOT EXISTS {}.{} ('.format(database_name, table_name)
            if len(PRI) > 0:
                pri_keys = ','.join(PRI)
            COL = ''' ) UNIQUE KEY({})  DISTRIBUTED BY HASH({}) BUCKETS 10  PROPERTIES ("replication_allocation" = "tag.location.default: 1") '''.format(
                pri_keys, pri_keys)
            desc_ = head + cols + COL
            self.doris_db.execute_sql(desc_)


if __name__ == '__main__':
    v = MysqlToDoris(mysql_config, doris_config, 'nn')
    v.mysql_doris()
