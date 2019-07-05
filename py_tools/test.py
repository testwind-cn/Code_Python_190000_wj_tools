import os
from py_tools.str_tool import StrTool
from py_tools.tool_load_excel_notype import LoadExcel


data_type_D0009 = [
    0, 2, 0, 0, 1, 0, 1, 0, 1, 1,
    0, 0, 0, 1, 1, 3, 3, 2, 2, 2,
    0, 0, 2, 2, 0, 1, 0, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 0, 2, 0, 2, 0, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 1,
    1, 2
    ]

s_sql = """INSERT INTO d0009 ( 
col_01,col_02,col_03,col_04,col_05,col_06,col_07,col_08,col_09,col_10,
col_11,col_12,col_13,col_14,col_15,col_16,col_17,col_18,col_19,col_20,
col_21,col_22,col_23,col_24,col_25,col_26,col_27,col_28,col_29,col_30,
col_31,col_32,col_33,col_34,col_35,col_36,col_37,col_38,col_39,col_40,
col_41,col_42,col_43,col_44,col_45,col_46,col_47,data_dt
)
VALUES
{} ;
"""
s_sql2 = "DELETE from d0009 WHERE data_dt = str_to_date('{}','%Y%m%d')"


file_name1 = os.path.join(os.getcwd(), '..', '..', '..', 'data_tmp', '20190418', 'sss.xls')

# abc = LoadExcel(p_file_name1="E:\\git-workspace\\Code_Database\\Code_Database_180829_Thjk_DB\\data_tmp\\20190301\\D0009LoanSurplusRpt_1.xls", p_first_row=4, p_total_col=62, p_ctl_col=1, p_data_type=data_type_D0009)

abc = LoadExcel(p_file_name1="C:\\Users\\wangjun\\Desktop\\Code\\Code_Database_180829_Thjk_DB\\data_tmp\\20190411\\D0009LoanSurplusRpt_1.xls", p_first_row=4, p_total_col=62, p_ctl_col=1, p_data_type=data_type_D0009)


abc.LoadHive_Mysql("2019-03-01", s_sql)

a = StrTool()
print( a.get_the_date_str())