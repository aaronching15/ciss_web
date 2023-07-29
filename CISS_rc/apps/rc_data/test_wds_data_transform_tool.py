# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

''' 
last 20201027 | since 20200301
todo：# TODO:一下未梳理完成

功能：对WDS下载的数据进行转换操作
1，根据"OPDATE"方式更新的一段时间的行，用key_column关键列更新细分csv文件
2，



notes: 
'''
#################################################################################
### Initialization 
### Initialization 
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )
sys.path.append(path_ciss_rc + "db\\analysis_indicators\\" )
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )
sys.path.append(os.getcwd()[:2] + "\\ciss_web\\CISS_rc\\db\\db_assets\\" )

import pandas as pd 
import numpy as np 
import math
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

file_path_admin = "C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\apps\\rc_data\\"
path_data_adj = "D:\\db_wind\\data_adj\\"
path_data_wds = "D:\\db_wind\\data_wds\\"

###
from data_io import data_io 
data_io_1 = data_io()
col_str = "Unnamed"

### 导入wds数据转换模块
from transform_wind_wds import transform_wds
transform_wds1 = transform_wds()
### Print all modules 
transform_wds1.print_info()

# from get_wind_wds import wind_wds
# wind_wds1 = wind_wds()
# wind_wds1.print_info()

### datetime
import time 
import datetime as dt 

#################################################################################

count  =0 
while count < 100 :
    print("Type of data method:")
    print("1,文件批量转换名称,给定具体前缀和后缀名称 ")
    print("2,将一次性下载的opdater日期区间表格或全历史表格，生成prime_key交易日文件 ")
    print("3,查询股票池：cs ")
    get_wds_type = input("Type in choice 1,2,3...")

#################################################################################
### 1,文件批量转换名称,给定具体前缀和后缀名称
# from WDS_TRADE_DT_20000102.csv to WDS_TRADE_DT_20000102_ALL.csv
    if get_wds_type == "1" :
        table_name = input("Type in table name ")
        path_wds = "D:\db_wind\data_wds\\"
        path_table = path_wds + table_name + "\\"
        file_list = os.listdir( path_table )
        
        ### case 1,识别出 WDS_TRADE_DT_20000102.csv 的文件
        # file_list =[temp_str for temp_str in file_list if "_ALL" not in temp_str  ]
        # file_list =[temp_str for temp_str in file_list if "WDS_TRADE_DT_" in temp_str  ]

        ### case 2 识别出包含 "WDS_" 和 ".0" 的文件
        file_list =[temp_str for temp_str in file_list if "WDS_"  in temp_str  ]
        file_list =[temp_str for temp_str in file_list if ".0" in temp_str  ]

        count_sub=0 
        for temp_file_name in file_list :
            ### case 1
            # new_file_name = temp_file_name[:-4]+"_ALL"+ temp_file_name[-4:]
            
            ### case 2 
            str_list = temp_file_name.split(".0")
            new_file_name = str_list[0] +str_list[1]
            # 先删除该文件
            if os.path.exists( path_table+new_file_name ) :
                os.remove(path_table+new_file_name )

            ### 文件改名
            os.rename(path_table+temp_file_name,path_table+new_file_name )
            count_sub=count_sub+1
            print(count_sub, path_table+new_file_name )  


#################################################################################
### 2,将一次性下载的opdater日期区间表格或全历史表格，生成prime_key交易日文件
# notes:由于wds里绝大部分row数据对应的"OPDATE"都是2017年以后，这说明wind不定期会对历史数据进行调整。
# "AShareStrangeTradedetail" 还要算 >=20150917
    if get_wds_type == "2" :
        obj_in = {}
        obj_in["dict"] = {}
        obj_in["dict"]["table_name"] = input("Type in table name,such as  AShareIndusRating ")
        obj_in["dict"]["prime_key"] = input("Type in prime_key,such as RATING_DT")
        obj_in["dict"]["file_name_opdate"] =  input("文件名，例如WDS_full_table_full_table_ALL.csv or WDS_EST_DT_20000101_20051231.csv ")

        ### 导入已下载好csv的opdate数据文件：读取 opdate数据
        file_name = obj_in["dict"]["file_name_opdate"]
        try :
            obj_in["wds_df"] = pd.read_csv( path_data_wds +obj_in["dict"]["table_name"]+"\\"+ file_name )
        except :
            obj_in["wds_df"] = pd.read_csv( path_data_wds +obj_in["dict"]["table_name"]+"\\"+ file_name,encoding="gbk" )
        # print( obj_in["df_opdate"].head().T  )
        # obj_in = wind_wds1.manage_opdata_to_anndates(obj_in)
        obj_in = transform_wds1.manage_opdata_to_anndates(obj_in)
    

    count = count + 1 

#################################################################################
### 3,查询股票池：cs
    if get_wds_type == "3" :
        code_stock = input("Type in stock code,such as 600036... ")
        ### todo
        path= "C:\\zd_zxjtzq\\rc_reports_cs\\"
        file_name = "股票池最新状态查询_latest.xls"
        df0= pd.read_excel(path + file_name ,sheet_name="数据" , encoding="gbk")
        df0=df0.iloc[:,:10]
        df0.columns=df0.iloc[0,:]
        
        df_sub = df0 [ df0["股票代码"]== code_stock ]
        print("================================================================= \n" ,df_sub.T ) 

asd


# TODO:一下未梳理完成

#################################################################################
## 3,更新最新A股信息，计算个股历史行业变动和最新行业分类:将3种行业分类代码和中文值赋给对应的股票
# output:df_ind_code_stock_last.csv;df_ind_code_stock_io.csv
# cal_stock_indclass() 会先运行 cal_df_ind()
# notes:1,这个需要运行比较久的时间；2,cal_df_ind 需要对沪深300指数保存最新的全历史日期数据

object_wds = transform_wds1.cal_stock_indclass("list")
asd

#################################################################################
### 按一定的命名规则对文件夹内满足条件的文件改名：
# table_name = "ChinaMutualFundStockPortfolio"
# table_name = "AShareEODPrices"
# table_name = "AShareEODDerivativeIndicator"
# table_name = "AIndexEODPrices"
# table_name = "ChinaMutualFundNAV"
# result = transform_wds1.rename_folder(table_name, para_dict={} )

# asd


#################################################################################
### 给定指数最新成分和历史成分调整，计算指数历史成分
# raw data from rc_标普500成分股及的行业权重变化_1960_2015.xlsx
# file_path = "C:\\zd_zxjtzq\\rc_reports_cs\\指数研究\\美股指数研究\\"

# Nasdaq 1997-2019
# file_index_consti_end = "nasdaq100_20200310.csv"
# file_consti_io = "nasdaq100_in_out_1997_2020.csv"

# SP500 1994-2019
# file_index_consti_end = "sp500_20200310.csv"
# file_consti_io = "sp500_in_out_1994_2020.csv"

# index_constituents_hist = transform_wds1.cal_index_constituents_adjustment(file_index_consti_end,file_consti_io,file_path)

# asd

#################################################################################
### 对于给定交易日至最新交易日，若有新股票无个券csv文件则新建该文件
# notes:第一次统计下载个股数据到20190830，200309下载了190830至200309期间的新个股；
# 但是，对于已有的股票，并不会更新价格数据。
''''''
# table_name = "AShareEODPrices"
# date_start ="20190830" # pre 20190830-20200210 
# df_output = transform_wds1.update_newcode_from_date(date_start ,table_name  )

# table_name = "AShareEODDerivativeIndicator"
# date_start ="20190830" # pre 20190830-20200210 
# df_output = transform_wds1.update_newcode_from_date(date_start ,table_name  )

# table_name = "AIndexEODPrices"
# date_start ="20190830" # pre 20190830-20200210 
# df_output = transform_wds1.update_newcode_from_date(date_start ,table_name  )

# asd

#################################################################################
### 按最新交易日行情对个股日行情内的交易日进行更新
''''''
# table_name = "AShareEODPrices"
# df_output = transform_wds1.update_date_pass_code(table_name)

# table_name = "AShareEODDerivativeIndicator"
# df_output = transform_wds1.update_date_pass_code(table_name)

# table_name = "AIndexEODPrices"
# df_output = transform_wds1.update_date_pass_code(table_name)

# asd

#################################################################################
### 给定交易日，计算当日所有股票行业分类，基于已经计算好了的数据
# 个股行业分类数据截至20200107，需要对20200107亿后上市的个股构建行业分类数据，同时对现有股票匹配新的行业分类数据

# code_list = []
# date = "20200102"
# if_all_codes="1"
# ### if_all_codes="1" means directly import all code list from inside
# object_ind = transform_wds1.get_ind_date(code_list,date ,if_all_codes) 

# asd



#################################################################################
### 获取所有文件名称 |需要对每张表进行匹配分析
### ANY Table 增加任意表格列名称 | changeed both for trading_date until 201912,codes until 201909
'''
notes：每个表格的关键词都不一样，需要先下载一个标准化的表格获取列名称
"AShareDescription",
"AShareEODDerivativeIndicator",
'''
# from get_wind_wds import wind_wds
# wind_wds1 = wind_wds()
# ### Print all modules 
# wind_wds1.print_info()

# table_name = "AShareDescription"
# print("table_name  ", table_name ) 
# file_path = "C:\\db_wind\\"+ table_name +"\\"
# file_list = os.listdir( file_path )
# ### 剔除已经转换过的文件
# file_list =[temp_str for temp_str in file_list if "_adj" not in temp_str  ]
# file_list =[temp_str for temp_str in file_list if "S_INFO_WINDCODE" not in temp_str  ]
# print( "file-list", file_list[:5] )

# ### 读取表的列信息， 来自于已有同样列名的文件

# temp_cols= wind_wds1.get_table_columns()

# ### 设置不同关键词csv文件的string长度判断值
# para_keyword={}
# result = transform_wds1.add_columns2table(table_name,temp_cols,file_list,file_path ,para_keyword ) 

# ASD



### Assign CN name to df if output file is needed 
# temp_df.columns = list_cols_CN

#################################################################################
### 按一定的命名规则对文件夹内满足条件的文件改名：
# table_name = "ChinaMutualFundStockPortfolio"
# table_name = "AShareEODPrices"
# table_name = "AShareEODDerivativeIndicator"
# table_name = "AIndexEODPrices"
# table_name = "ChinaMutualFundNAV"
# result = transform_wds1.rename_folder(table_name, para_dict={} )

# asd


#################################################################################
### 对于给定交易日至最新交易日，若有新股票无个券csv文件则新建该文件
# notes:第一次统计下载个股数据到20190830，200309下载了190830至200309期间的新个股；
# 但是，对于已有的股票，并不会更新价格数据。
''''''
# table_name = "AShareEODPrices"
# date_start ="20190830" # pre 20190830-20200210 
# df_output = transform_wds1.update_newcode_from_date(date_start ,table_name  )

# table_name = "AShareEODDerivativeIndicator"
# date_start ="20190830" # pre 20190830-20200210 
# df_output = transform_wds1.update_newcode_from_date(date_start ,table_name  )

# table_name = "AIndexEODPrices"
# date_start ="20190830" # pre 20190830-20200210 
# df_output = transform_wds1.update_newcode_from_date(date_start ,table_name  )

# asd




#################################################################################
### 获取所有文件名称 |需要对每张表进行匹配分析
### ANY Table 增加任意表格列名称 | changeed both for trading_date until 201912,codes until 201909
'''
notes：每个表格的关键词都不一样，需要先下载一个标准化的表格获取列名称
"AShareDescription",
"AShareEODDerivativeIndicator",
'''
# from get_wind_wds import wind_wds
# wind_wds1 = wind_wds()
# ### Print all modules 
# wind_wds1.print_info()

# table_name = "AShareDescription"
# print("table_name  ", table_name ) 
# file_path = "C:\\db_wind\\"+ table_name +"\\"
# file_list = os.listdir( file_path )
# ### 剔除已经转换过的文件
# file_list =[temp_str for temp_str in file_list if "_adj" not in temp_str  ]
# file_list =[temp_str for temp_str in file_list if "S_INFO_WINDCODE" not in temp_str  ]
# print( "file-list", file_list[:5] )

# ### 读取表的列信息， 来自于已有同样列名的文件

# temp_cols= wind_wds1.get_table_columns()

# ### 设置不同关键词csv文件的string长度判断值
# para_keyword={}
# result = transform_wds1.add_columns2table(table_name,temp_cols,file_list,file_path ,para_keyword ) 

# ASD