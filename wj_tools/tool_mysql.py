#!coding:utf-8

import pymysql
import pymysql.cursors


class TheDB:
    m_connect: None
    m_cursor: None

    def __init__(self):  # def __init__(self, realpart, imagpart):
        # self.r = realpart
        # self.i = imagpart
        return

    def connect(self, host='127.0.0.1', port=3306, user='root', passwd='thbl123', db='echart'):
        # 连接数据库
        self.m_connect = pymysql.Connect(
            host=host,
            port=port,
            user=user,
            passwd=passwd,
            db=db,
            charset='utf8'
        )
        # 获取游标
        self.m_cursor = self.m_connect.cursor()

    def execute(self, cmdSQL):

        try:
            self.m_cursor.execute(cmdSQL)  # 添加数据
        except Exception as e:
            self.m_connect.rollback()  # 事务回滚
            print('事务处理失败', e)
        else:
            self.m_connect.commit()  # 事务提交
#            print('事务处理成功', self.m_cursor.rowcount)

    def close(self):
        self.m_cursor.close()
        self.m_connect.close()
        self.m_cursor = None
        self.m_connect = None

