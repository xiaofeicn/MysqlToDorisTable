# -*- mode: python ; coding: utf-8 -*-
import sys

from MysqlHelper import MysqlHelper
from db_config import doris_config, mysql_config
import json
from os import system

mysql_db = MysqlHelper(mysql_config)
doris_db = MysqlHelper(doris_config)
database = sys.argv[1]
json_arr = []
no_free_table_num = 0
no_free_table_members = []

database_name = "ttk_member_" + str(database)
try:
    tables = mysql_db.select(
        '''select table_name from information_schema.TABLES where TABLE_SCHEMA='{}' '''.format(database_name))
    for table in tables:
        tab = table[0]
        json_arr.append({"member_id": database_name, "sync_table": tab})
except Exception as e:
    no_free_table_members.append(database_name)
    no_free_table_num += 1
    print(e)
print("*"*30)
print('''* 共有 {} 个企业无法同步，原因: 暂无自定义表'''.format(len(no_free_table_members)))
print(''' 企业如下''')
for member in no_free_table_members:
    print(member)
print("*"*30)
with open("format_json.json", 'w+') as write_f:
    write_f.write(json.dumps(json_arr, indent=2, ensure_ascii=False))
system(
    '''curl -v --location-trusted -u root: -H "format: json" -H "strip_outer_array: true" -T format_json.json http://doris-fe01:8030/api/sync_member/sync_table/_stream_load?''')
