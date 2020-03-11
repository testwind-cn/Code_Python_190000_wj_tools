#!coding:utf-8

import xlrd
import xlwt

import pathlib
import datetime
from hdfs.client import Client
from impala.dbapi import connect
from py_tools.file_check import MyHdfsFile
from py_tools.hdfsclient import MyClient  # hdfs
# hive
# data path config file
from py_tools.str_tool import StrTool

from py_tools.tool_mysql import TheDB

# https://xlrd.readthedocs.io/en/latest/api.html?highlight=Cell#xlrd.sheet.Cell

"""
data_type_D0009 = [
    0, 2, 0, 0, 1, 0, 1, 0, 1, 1,
    0, 0, 0, 1, 1, 3, 3, 2, 2, 2,
    0, 0, 2, 2, 0, 1, 0, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 0, 2, 0, 2, 0, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 1,
    1, 2
    ]

s_sql = '''INSERT INTO d0009 ( 
col_01,col_02,col_03,col_04,col_05,col_06,col_07,col_08,col_09,col_10,
col_11,col_12,col_13,col_14,col_15,col_16,col_17,col_18,col_19,col_20,
col_21,col_22,col_23,col_24,col_25,col_26,col_27,col_28,col_29,col_30,
col_31,col_32,col_33,col_34,col_35,col_36,col_37,col_38,col_39,col_40,
col_41,col_42,col_43,col_44,col_45,col_46,col_47,data_dt
)
VALUES
( {} )
'''
"""


class FileLoad:
    __workbook1 = None
    __sheet_names = []

    __p_local_file = ""
    __p_first_row = -1
    __p_total_col = 0
    __p_ctl_col = -1
    __p_data_type = []
    __p_data_pos = []

    def __init__(self):  # def __init__(self, realpart, imagpart):
        # self.r = realpart
        # self.i = imagpart
        return

    def __init__(self, p_file_name1: str, p_first_row: int, p_total_col: int, p_ctl_col: int, p_data_type: list):
        self.__p_local_file = p_file_name1
        self.__p_first_row = p_first_row
        self.__p_total_col = p_total_col
        self.__p_ctl_col = p_ctl_col
        self.__p_data_type = p_data_type
        return

    def __init__(self, p_file_name1: str, p_first_row: int, p_total_col: int, p_ctl_col: int, p_data_type: list, p_data_pos: list = []):
        self.__p_local_file = p_file_name1
        self.__p_first_row = p_first_row
        self.__p_total_col = p_total_col
        self.__p_ctl_col = p_ctl_col
        self.__p_data_type = p_data_type
        self.__p_data_pos = p_data_pos
        return

    def open_file(self, p_file_name1: str, p_first_row: int, p_total_col: int, p_ctl_col: int, p_data_type: list, p_data_pos: list = []):
        """

        :param p_file_name1:
        :param p_first_row: p_first_row=4， 表示首行在第五行; p_first_row=0， 表示首行在第一行
        :param p_total_col:
        :param p_ctl_col: # 第2列是合计,就是 col==1
        :param p_data_type:
        :param p_data_pos:
        :return:
        """
        self.__p_local_file = p_file_name1
        self.__p_first_row = p_first_row
        self.__p_total_col = p_total_col
        self.__p_ctl_col = p_ctl_col
        self.__p_data_type = p_data_type
        self.__p_data_pos = p_data_pos
        return

    def get_rows_excel(self):
        try:
            self.__workbook1 = xlrd.open_workbook(self.__p_local_file)
            self.__sheet_names = self.__workbook1.sheet_names()
        except Exception as e1:
            # raise StopIteration
            # 两个效果一样
            return

        for sheet_name in self.__sheet_names:
            sheet1 = self.__workbook1.sheet_by_name(sheet_name)
            # print(sheet_name)

            # rows = sheet1.row_values(1)  # 获取第2行内容
            # print(rows)

            cur_row = 0
            ctl_cell = "DATA"

            style1 = xlwt.XFStyle()
            style1.num_format_str = 'yyyy-mm-dd'

            while len("{}".format(ctl_cell).strip()) > 0 and "{}".format(ctl_cell).strip() != u'合计':
                ctl_cell = "DATA"
                ret_data_array = []

                for col in range(0, self.__p_total_col):
                    try:
                        cell_value = sheet1.cell_value(cur_row + self.__p_first_row, col)
                    except Exception as e1:
                        cell_value = ""

                    # 第2列是合计,就是 col==1
                    if col == self.__p_ctl_col:
                        ctl_cell = cell_value
                        if len("{}".format(ctl_cell).strip()) <= 0 or "{}".format(ctl_cell).strip() == u'合计':
                            break

                    if self.__p_data_type[col] == 3:  # 日期
                        # 如果 Excel 里面本来就是日期，那取到的是整数，要用下面的来转换
                        # cell_value = self.__c_date(cell_value)
                        pass
                    elif self.__p_data_type[col] == 2:  # 数字
                        cell_value = "{}".format(cell_value)
                        cell_value = cell_value.replace(',', '')
                        # data_value = float(cell_value)
                    elif self.__p_data_type[col] == 1:  # 字符串
                        pass

                    cell_value = "{}".format(cell_value)
                    cell_value.replace("\t", "    ")
                    ret_data_array.append(cell_value)

                if len("{}".format(ctl_cell).strip()) > 0 and "{}".format(ctl_cell).strip() != u'合计':
                    yield ret_data_array

                cur_row += 1

    def get_rows_fix_w_text(self):
        try:
            f_file = open(self.__p_local_file)  # 返回一个文件对象
        except Exception as e1:
            # raise StopIteration
            # 两个效果一样
            return

        line = f_file.readline()
        cur_row = 0

        while len(line) > 0:
            if cur_row >= self.__p_first_row:
                byte_line = line.encode('gb18030')
                ret_data_array = []

                for col in range(0, self.__p_total_col):
                    byte_col = byte_line[self.__p_data_pos[col]: self.__p_data_pos[col + 1]]
                    cell_value = byte_col.decode('gb18030')
                    cell_value = cell_value.strip()

                    if self.__p_data_type[col] == 3:  # 日期
                        # 如果 Excel 里面本来就是日期，那取到的是整数，要用下面的来转换
                        # cell_value = self.c_date(cell_value)
                        pass
                    elif self.__p_data_type[col] == 2:  # 数字
                        cell_value = "{}".format(cell_value)
                        cell_value = cell_value.replace(',', '')
                    elif self.__p_data_type[col] == 1:  # 字符串
                        pass

                    cell_value.replace("\t", "    ")
                    ret_data_array.append(cell_value)
                # run

                yield ret_data_array

            line = f_file.readline()
            cur_row += 1

    def get_sql(self, p_data_type: list, data_array: list, p_thedate: str):
        buf_sql = ""
        cur_col = 0
        for col in range(0, self.__p_total_col):
            if p_data_type[col] > 0:
                if cur_col > 0:
                    buf_sql += ','
                cur_col += 1

            if p_data_type[col] == 3:  # 日期
                # 如果 Excel 里面本来就是日期，那取到的是整数，要用下面的来转换
                # cell_value = self.__c_date(cell_value)
                try:
                    data_value = datetime.datetime.strptime(data_array[col], "%Y-%m-%d").date()
                    buf_sql += "str_to_date('{}','%Y-%m-%d')".format(data_value)
                except Exception as e1:
                    buf_sql += "NULL"
            elif p_data_type[col] == 2:  # 数字
                try:
                    data_value = "{}".format(data_array[col])
                    data_value = data_value.replace(',', '')
                    data_value = float(data_value)
                    buf_sql += "{}".format(data_value)
                except Exception as e1:
                    buf_sql += "NULL"
            elif p_data_type[col] == 1:  # 字符串
                try:
                    buf_sql += "'{}'".format(data_array[col])
                except Exception as e1:
                    buf_sql += "NULL"

        if len(p_thedate) == 8:
            buf_sql += ",str_to_date('{}','%Y%m%d')".format(p_thedate)
        elif len(p_thedate) == 10:
            buf_sql += ",str_to_date('{}','%Y-%m-%d')".format(p_thedate)

        buf_sql = "({})".format(buf_sql)

        return buf_sql
        # the_db.execute(real_sql)

    def __c_date(self, dates):  # 定义转化日期戳的函数,dates为日期戳
        delta = datetime.timedelta(days=dates)
        today = datetime.datetime.strptime('1899-12-30', '%Y-%m-%d') + delta  # 将1899-12-30转化为可以计算的时间格式并加上要转化的日期戳
        return datetime.datetime.strftime(today, '%Y-%m-%d')  # 制定输出日期的格式

    def LoadHive_Mysql(self, p_thedate, file_type: int, p_the_db: TheDB = None, p_table: str = "", p_sql: str = "", p_sql2: str = "", p_delete_hive=False):
        """


        :param p_thedate:
        :param file_type:  0 excel, 1 fix width text
        :param p_the_db:
        :param p_table:
        :param p_sql:
        :param p_sql2:
        :param p_delete_hive:  是否删除HIVE表
        :return:
        """
        if self.__p_local_file is None or len(self.__p_local_file) == 0:
            return

        if self.__p_data_type is None or len(self.__p_data_type) == 0:
            return

        if self.__p_total_col is None or self.__p_total_col <= 0:
            return

        p_path = pathlib.PurePath(self.__p_local_file).parent
        if len(p_thedate) > 0:
            date_08 = StrTool.get_the_date_str(p_thedate, 0, 8)
            name = pathlib.PurePath(self.__p_local_file).name + "_{}.tsv".format(date_08)
        else:
            name = pathlib.PurePath(self.__p_local_file).name + ".tsv"

        local_file_2 = str(p_path.joinpath(name))

        buf_sql = ""
        file_2 = None
        first_row = True

        if file_type == 0:
            all_rows = self.get_rows_excel()
        else:
            all_rows = self.get_rows_fix_w_text()

        for row in all_rows:
            print(row)
            if len(p_table) > 0 and file_2 is None:
                file_2 = open(local_file_2, 'w', encoding="utf-8")
            the_str = ""
            if first_row:
                first_row = False
                # 第一行，删除当日数据
                if p_the_db is not None and p_sql2 is not None and len(p_sql2) > 0:
                    real_sql = p_sql2.format(p_thedate)
                    p_the_db.execute(real_sql)
            else:
                buf_sql += ",\n"
                the_str += "\n"
            first_col = True
            for i in range(0, self.__p_total_col):
                if i < len(self.__p_data_type) and i < len(row) and self.__p_data_type[i] > 0:
                    if first_col:
                        first_col = False
                    else:
                        the_str += "\t"
                    the_str += row[i]

            if len(p_table) > 0 and file_2 is not None:
                file_2.write(the_str)

            if p_the_db is not None and p_sql is not None and len(p_sql) > 0:
                real_sql = self.get_sql(self.__p_data_type, row, p_thedate)
                print(real_sql)
                buf_sql += real_sql

        print("end")

        if p_the_db is not None and p_sql is not None and len(p_sql) > 0:
            real_sql = p_sql.format(buf_sql)
            p_the_db.execute(real_sql)
            print(real_sql)

        if len(p_table)>0 and file_2 is not None:
            file_2.close()
            self.run_hdfs(local_file_2, p_thedate)
            self.run_hive(local_file_2, p_thedate, table=p_table, delete_hive=p_delete_hive)
            try:
                pathlib.Path(local_file_2).unlink()
                # 删除 filename
            except Exception as e2:
                pass

    def __get_hdfs_file_name(self, local_file: str, the_date: str):
        # "/data/posflow/thbl_rpt" 在 hdfs 上的路径
        hdfs_path = pathlib.PurePosixPath("/data/posflow/thbl_rpt")

        if len(the_date) > 0:
            hdfs_path = hdfs_path.joinpath(the_date)

        hdfs_path = hdfs_path.joinpath(pathlib.PurePath(local_file).name)

        hdfs_file = str(hdfs_path)
        return hdfs_file

    def run_hdfs(self, local_file: str, the_date: str):
        a_client = MyClient("http://10.91.1.100:50070")  # "http://10.2.201.197:50070"
        hdfs_file = self.__get_hdfs_file_name(local_file, the_date)
        MyHdfsFile.safe_make_dir(a_client, hdfs_file)
        # a_client.newupload(to_file2, to_file1, encoding='utf-8')
        the_file = a_client.status(hdfs_file, strict=False)
        if the_file is None:
            a_client.upload(hdfs_file, local_file)
            a_client.set_permission(hdfs_file, 777)
        # a_client.set_owner(thePath,owner='hdfs',group='supergroup')
        elif the_file['type'].lower() == 'file':  # 'directory'
            a_client.set_permission(hdfs_file, 777)

    def run_hive(self, local_file: str, the_date: str, table: str, delete_hive = False):
        a_client = Client("http://10.91.1.100:50070")  # "http://10.2.201.197:50070"
        conn = connect(host="10.91.1.100", port=10000, auth_mechanism="PLAIN", user="root")
        cur = conn.cursor()

        print("Start\n")

        hdfs_file = self.__get_hdfs_file_name(local_file, the_date)

        table_name = table  # "allinpal_rpt.thbl_rpt_d0009"

        if len(the_date) > 0:
            sql = 'LOAD DATA INPATH \'{}\' OVERWRITE INTO TABLE {} PARTITION ( data_dt=\'{}\' )'.format(hdfs_file, table_name, the_date)
        else:
            sql = 'LOAD DATA INPATH \'{}\' OVERWRITE INTO TABLE {} '.format(hdfs_file, table_name)

        if MyHdfsFile.isfile(a_client, hdfs_file):
            if delete_hive:
                cur.execute('TRUNCATE TABLE {} '.format(table_name))
                print("OK " + 'TRUNCATE TABLE {} '.format(table_name) + "\n")
            print("OK" + "  " + sql + "\n")
            cur.execute(sql)  # , async=True)

        cur.close()
        conn.close()

