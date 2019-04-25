#!coding:utf-8

import xlrd
import xlwt
import datetime

from tool_mysql import TheDB

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
 {} ;
'''

s_sql2 = "DELETE from d0009 WHERE data_dt = str_to_date('{}','%Y%m%d')"

"""


class LoadText:
    def __init__(self):  # def __init__(self, realpart, imagpart):
        # self.r = realpart
        # self.i = imagpart
        return

    def c_date(self, dates):  # 定义转化日期戳的函数,dates为日期戳
        delta = datetime.timedelta(days=dates)
        today = datetime.datetime.strptime('1899-12-30', '%Y-%m-%d') + delta  # 将1899-12-30转化为可以计算的时间格式并加上要转化的日期戳
        return datetime.datetime.strftime(today, '%Y-%m-%d')  # 制定输出日期的格式

    def load_text_by_file(self, p_the_db: TheDB, p_file_name1: str, p_first_row: int, p_total_col: int, p_sql: str, p_sql2: str, p_data_type: list, the_date):
        # 连接数据库
        # p_first_row = 4 表示第5行是首行数据，从0开始编号。
        # p_first_row = 0 表示第1行是首行数据，从0开始编号。

        the_db = p_the_db

        workbook1 = xlrd.open_workbook(p_file_name1)

        sheet_names = workbook1.sheet_names()

        real_sql = p_sql2.format(the_date)
        the_db.execute(real_sql)

        for sheet_name in sheet_names:
            sheet1 = workbook1.sheet_by_name(sheet_name)
            # print(sheet_name)

            rows = sheet1.row_values(1)  # 获取第2行内容
            # print(rows)

            rows = sheet1.row_values(3)  # 获取第四行内容
            # print(rows)

            rows = sheet1.row_values(4)  # 获取第5行内容
            # print(rows)

            row_n = 0
            row_id = "DATA"

            style1 = xlwt.XFStyle()
            style1.num_format_str = 'yyyy-mm-dd'

            big_buf_sql = ""

            while len(row_id.strip()) > 0 and row_id.strip() != u'合计':

                row_id = "DATA"
                buf_sql = ""
                first_col = 0

                for col in range(0, p_total_col):
                    cell_value = sheet1.cell_value(row_n + p_first_row, col)

                    # 第2列是合计
                    if col == 1:
                        row_id = cell_value
                        if len(row_id.strip()) <= 0 or row_id.strip() == u'合计':
                            break

                    if p_data_type[col] > 0:
                        if first_col > 0:
                            buf_sql += ','
                        first_col += 1

                    if p_data_type[col] == 3:  # 日期
                        # 如果 Excel 里面本来就是日期，那取到的是整数，要用下面的来转换
                        # cell_value = self.c_date(cell_value)
                        cell_value = datetime.datetime.strptime(cell_value, "%Y-%m-%d").date()
                        buf_sql += "str_to_date('{}','%Y-%m-%d')".format(cell_value)
                    elif p_data_type[col] == 2:  # 数字
                        cell_value = "{}".format(cell_value)
                        cell_value = cell_value.replace(',', '')
                        cell_value = float(cell_value)
                        buf_sql += "{}".format(cell_value)
                    elif p_data_type[col] == 1:  # 字符串
                        buf_sql += "'{}'".format(cell_value)

                if len(row_id.strip()) > 0 and row_id.strip() != u'合计':
                    buf_sql += ",str_to_date('{}','%Y%m%d')".format(the_date)

                    if row_n > 0:
                        big_buf_sql += ','

                    big_buf_sql += "({})".format(buf_sql)

                row_n += 1

            real_sql = p_sql.format(big_buf_sql)
            # run

            the_db.execute(real_sql)

    def load_text_by_row(self, p_the_db: TheDB, p_file_name1: str, p_first_row: int, p_total_col: int, p_sql: str, p_sql2: str, p_data_type: list, the_date, p_data_pos):
        # 连接数据库
        # p_first_row = 4 表示第5行是首行数据，从0开始编号。
        # p_first_row = 0 表示第1行是首行数据，从0开始编号。

        the_db = p_the_db

        real_sql = p_sql2.format(the_date)
        the_db.execute(real_sql)

        f_file = open(p_file_name1)  # 返回一个文件对象
        line = f_file.readline()
        row_n = 0

        while len(line) > 0:

            if row_n >= p_first_row:
                byte_line = line.encode('gb18030')
                buf_sql = ""
                first_col = 0

                for col in range(0, p_total_col):
                    byte_col = byte_line[p_data_pos[col]: p_data_pos[col + 1]]
                    cell_value = byte_col.decode('gb18030')
                    cell_value = cell_value.strip()

                    if p_data_type[col] > 0:
                        if first_col > 0:
                            buf_sql += ','
                        first_col += 1

                    if p_data_type[col] == 3:  # 日期
                        # 如果 Excel 里面本来就是日期，那取到的是整数，要用下面的来转换
                        # cell_value = self.c_date(cell_value)
                        cell_value = datetime.datetime.strptime(cell_value, "%Y-%m-%d").date()
                        buf_sql += "str_to_date('{}','%Y-%m-%d')".format(cell_value)
                    elif p_data_type[col] == 2:  # 数字
                        cell_value = cell_value.replace(',', '')
                        cell_value = float(cell_value)
                        buf_sql += "{}".format(cell_value)
                    elif p_data_type[col] == 1:  # 字符串
                        buf_sql += "'{}'".format(cell_value)

                buf_sql += ",str_to_date('{}','%Y%m%d')".format(the_date)
                buf_sql = "({})".format(buf_sql)
                real_sql = p_sql.format(buf_sql)
                # run

                the_db.execute(real_sql)

            line = f_file.readline()
            row_n += 1

        f_file.close()


"""
def load_excel_D0009(p_date):  # : str = u'D0009LoanSurplusRpt_1_修改.xls'
    # u'D0009LoanSurplusRpt_1_修改_{}.xls'
    # file_name1 = os.path.join(os.getcwd(), u'D0009LoanSurplusRpt_1_修改_{}.xls'.format(p_date))

    file_name1 = os.path.join(os.getcwd(), 'data_tmp', p_date, 'D0009LoanSurplusRpt_1.xls')

    a = LoadExcel()
    a.load_excel(p_file_name1=file_name1, p_title_row=3, p_total_col=62, p_sql=s_sql, p_data_type=data_type_D0009, the_date=p_date)


if __name__ == "__main__":
    date = "20190308"
    load_excel_D0009(date)
"""