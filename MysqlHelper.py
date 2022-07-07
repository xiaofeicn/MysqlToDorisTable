# -*- coding: utf-8 -*-

import pymysql


class MysqlHelper:
    def __init__(self, config):
        self.host = config["host"]
        self.port = config["port"]
        self.user = config["user"]
        self.password = config["password"]
        self.db = config["db"]
        self.charset = config["charset"]
        self.con = None
        self.cursor = None

    def create_con(self):
        """
        创建连接 , charset='utf8'
        """
        try:
            self.con = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                       database=self.db)
            self.cursor = self.con.cursor()
            return True
        except Exception as e:
            print(e)
            return False

    def close_con(self):
        """
        关闭链接
        """
        if self.cursor:
            self.cursor.close()
        if self.con:
            self.con.close()

    # sql执行
    def execute_sql(self, sql):
        """
        执行插入/更新/删除语句
        """
        try:
            self.create_con()
            # print(sql)
            self.cursor.execute(sql)
            self.con.commit()
        except Exception as e:
            print(sql)
            print(e)
        finally:
            self.close_con()

    def select(self, sql, *args):
        """
        执行查询语句
        """
        try:
            self.create_con()
            # print(sql)
            self.cursor.execute(sql, args)
            res = self.cursor.fetchall()
            return res
        except Exception as e:
            print(e)
            return False
        finally:
            self.close_con()

    def execute_commit(self, sql):
        """
        执行数据库sql语句，针对更新,删除,事务等操作失败时回滚
        """
        try:
            self.cursor.execute(sql)
            self.con.commit()
        except Exception as e:
            self.con.rollback()
            print(e)
            return False
        finally:
            self.close_con()
