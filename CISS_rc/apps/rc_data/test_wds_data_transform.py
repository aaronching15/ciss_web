# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

''' 
todo 

功能：
1,每周更新股票行业分类
2,
last 201109 | since 190101
------ 
'''
#################################################################################
### Initialization 
import os 
# 获取当前目录 os.getcwd() =: G:\zd_zxjtzq\ciss_web
import sys
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )
sys.path.append(os.getcwd()[:2] + "\\ciss_web\\CISS_rc\\db\\db_assets\\" )

import pandas as pd 
import numpy as np 

if os.path.exists( "C:\\zd_zxjtzq\\ciss_web\\" ) :
    file_path_admin = "C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\apps\\rc_data\\"
else :
    file_path_admin = "C:\\ciss_web\\CISS_rc\\apps\\rc_data\\"




### 导入wds数据转换模块
from transform_wind_wds import transform_wds
transform_wds1 = transform_wds()
### Print all modules 
transform_wds1.print_info()

import datetime as dt
import time 

#################################################################################
### 为了在无数据库连接的情况下不影响行业分类计算
from get_wind_wds import wind_wds
wind_wds1 = wind_wds()
wind_wds1.print_info()

#################################################################################
### 计算个股历史行业变动和最新行业分类:将3种行业分类代码和中文值赋给对应的股票
# 个股行业分类数据截至20200107，需要对20200107亿后上市的个股构建行业分类数据，同时对现有股票匹配新的行业分类数据
'''
涉及到的全量表格：AShareDescription；AShareIndustriesClass；"AShareIndustriesClassCITICS"，"AShareSWIndustriesClass"

'''

for temp_table in ["AShareDescription","AShareIndustriesClass","AShareIndustriesClassCITICS","AShareSWIndustriesClass"] :
    data_obj = wind_wds1.get_table_full_input( temp_table )


obj_wds = transform_wds1.cal_stock_indclass("list")





#################################################################################
### 1,用"OPDATE" 维护一次性下载的表格
'''中国A股TTM与MRQ	AShareTTMAndMRQ ;中国A股财务衍生指标表	AShareFinancialderivative
steps:
1,下载增量数据， wind_wds1.get_table_opdate()
2,读取历史数据
3，将增量数据部分写入历史数据
4，输出保存

notes:时间选取要考虑到前后2个opdate的值之间可能有数据差，因此需要取至少多3~5个交易日
'''

### 确定更新日期：往前推5~10天 || datetime.timedelta(days=1));datetime.timedelta(days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0)
period_dt_update = 10
# "%Y%m%d %H:%M:%S"
# date_time_update = "20200509" 
# temp_dt = dt.datetime.strptime( date_time_update, "%Y%m%d") - dt.timedelta(days= period_dt_update )
date_time_update = dt.datetime.now() - dt.timedelta(days= period_dt_update )

datetime_key = "OPDATE"

### 确定需要整张表保存的股票， 导入一次性下载的列表
file_name =  "log_data_wds_tables_1shot.csv"
df_table_list = pd.read_csv( file_path_admin +file_name  ,encoding="gbk"  )
table_list = list( df_table_list["name_table"] )
print("table_list ", table_list )


for temp_table in table_list :
    ### 确定数据获取的开始时间 datetime_value
    # 获取文件最后修改时间：notes:这里假定已经有了数据文件"WDS_full_table_full_table_ALL.csv"
    file_path = "D:\\db_wind\\data_wds\\"+temp_table +"\\"
    file_name_all = "WDS_full_table_full_table_ALL.csv"

    # dt_update_file = os.path.getmtime(file_path + file_name_all ) 
    # # seconds --> format under time -->string 
    # dt_update_file = time.strftime("%Y%m%d %H:%M:%S", time.localtime(dt_update_file) )
    # dt_update_file =dt.datetime.strptime( dt_update_file, "%Y%m%d %H:%M:%S")

    # # 若文件很久没更新，则选择更早的日期
    # print( date_time_update, dt_update_file  ) 
    # datetime_value =  min( date_time_update,dt_update_file)
    # datetime_value = dt.datetime.strftime( dt_update_file, "%Y%m%d")
    
    #############################################################
    ### 1,下载增量数据， wind_wds1.get_table_opdate()
    # from manage_data_check_anndates ; 
    obj_in = {}
    obj_in["dict"] = {}
    obj_in["dict"]["table_name"] = temp_table
    obj_in["dict"]["datetime_key"] ="OPDATE"
    ### 确定更新日期：往前推5~10天 || datetime.timedelta(days=1));datetime.timedelta(days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0)
    period_dt_update = 10
    # "%Y%m%d %H:%M:%S"
    # date_time_update = "20200509" 
    # temp_dt = dt.datetime.strptime( date_time_update, "%Y%m%d") - dt.timedelta(days= period_dt_update )
    date_time_update = dt.datetime.now() - dt.timedelta(days= period_dt_update ) 
    
    datetime_value =   date_time_update 

    obj_in["dict"]["datetime_value_lb"] =dt.datetime.strftime( datetime_value, "%Y%m%d") 
    obj_in["dict"]["datetime_value_ub"] = "20991231"
    # opdate文件不保存
    obj_in["dict"]["df2csv"] = 0 

    data_obj = wind_wds1.get_table_opdate( obj_in )

    temp_df = data_obj["wds_df"]
    #  self.path_out+ self.folder_name+ '\\'+ table_name+ '\\'
    file_path = data_obj["dict"]["file_path"] 
    # "WDS_"+ datetime_key +"_"+ datetime_value + "_"+datetime_range +   ".csv"
    file_name = data_obj["dict"]["file_name"]

    ### 2,读取历史数据
    file_name_all = "WDS_full_table_full_table_ALL.csv"
    # 判断文件是否存在
    if os.path.exists( file_path + file_name_all ) : 
        try :
            temp_df_all = pd.read_csv(file_path + file_name_all )
        except :
            temp_df_all = pd.read_csv(file_path + file_name_all,encoding="gbk" )
    # notes:temp_df_all和temp_df可能有重合的部分

    ### 3,合并数据
    len1 =  len(temp_df_all.index)
    temp_df_all = temp_df_all.append( temp_df,ignore_index=True  )
    len2 =  len(temp_df_all.index)
    ### 注意：删除某几列有相同数值的row，并保留最新列
    temp_df_all =temp_df_all.drop_duplicates(subset=["OBJECT_ID","OPDATE"],keep="last")
    len3 =  len(temp_df_all.index)
    print(temp_table,"Change of record numbers:", len1,len2,len3 )
    temp_df_all.to_csv(file_path + file_name_all )
    # 这个没法用，万一列顺序变了就会出错： pandas向一个csv文件追加写入数据,mode="a" 


################################################################################
## 2,获取历史交易日和交易月数据：下载最新上证综指和沪深300指数历史日行情文件，主要股票月收益率数据

table_name = "AIndexEODPrices"
prime_key = "S_INFO_WINDCODE"
# prime_key_value = "000300.SH"
# data_obj = wind_wds1.get_table_primekey_input(table_name,prime_key ,prime_key_value )
prime_key_value = "000001.SH"
data_obj = wind_wds1.get_table_primekey_input(table_name,prime_key ,prime_key_value )
temp_df = data_obj["wds_df"]
file_name = "date_list_tradingday.csv"
pd.DataFrame(temp_df["TRADE_DT"].values,columns=["date"]).to_csv( wind_wds1.path_out + "\\data_adj\\" + file_name )

table_name = "AShareMonthlyYield"
prime_key = "S_INFO_WINDCODE"
prime_key_value = "600000.SH"
data_obj = wind_wds1.get_table_primekey_input(table_name,prime_key ,prime_key_value )
prime_key_value = "000001.SZ"
data_obj = wind_wds1.get_table_primekey_input(table_name,prime_key ,prime_key_value )

temp_df = data_obj["wds_df"]
file_name = "date_list_month.csv"
pd.DataFrame(temp_df["TRADE_DT"].values,columns=["date"]).to_csv( wind_wds1.path_out + "\\data_adj\\" + file_name )






asd


