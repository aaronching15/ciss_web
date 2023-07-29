# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
20200514开始新增OPDATE维护方式

功能： 
1,获取历史交易日和交易月数据：下载最新上证综指和沪深300指数历史日行情文件，主要股票月收益率数据
2，{opdate后不那么需要}先下载昨日滚动一致预期数据，因为预期数据第二天才会有完整的 
3，VIP每个交易日自动维护数据下载表格
'''
#################################################################################
### Initialization 
import os,sys
# 这个不能删，需要用来寻找 get_wind_wds
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )
sys.path.append(os.getcwd()[:2] + "\\ciss_web\\CISS_rc\\db\\db_assets\\" )
import pandas as pd 

from get_wind_wds import wind_wds
wind_wds1 = wind_wds()
### Print all modules 
wind_wds1.print_info() 


################################################################################
### 1,获取历史交易日和交易月数据：下载最新上证综指和沪深300指数历史日行情文件，主要股票月收益率数据
### day

table_name = "AIndexEODPrices"
prime_key = "S_INFO_WINDCODE"
# prime_key_value = "000300.SH"
# data_obj = wind_wds1.get_table_primekey_input(table_name,prime_key ,prime_key_value )
prime_key_value = "000001.SH"
data_obj = wind_wds1.get_table_primekey_input(table_name,prime_key ,prime_key_value )
temp_df = data_obj["wds_df"]
file_name = "date_list_tradingday.csv"
df_temp = pd.DataFrame(temp_df["TRADE_DT"].values,columns=["date"])
df_temp =df_temp.sort_values(by="date")
df_temp.to_csv( wind_wds1.path_out + "\\data_adj\\" + file_name )

date_latest =df_temp["date"].to_list()[-1]
date_pre =df_temp["date"].to_list()[-2]

###也更新沪深300指数的数据
prime_key_value = "000300.SH"
data_obj = wind_wds1.get_table_primekey_input(table_name,prime_key ,prime_key_value )

### month
table_name = "AShareMonthlyYield"
prime_key = "S_INFO_WINDCODE"
prime_key_value = "600000.SH"
data_obj = wind_wds1.get_table_primekey_input(table_name,prime_key ,prime_key_value )
prime_key_value = "000001.SZ"
data_obj = wind_wds1.get_table_primekey_input(table_name,prime_key ,prime_key_value )

temp_df = data_obj["wds_df"]
file_name = "date_list_month.csv"
df_temp = pd.DataFrame(temp_df["TRADE_DT"].values,columns=["date"])
df_temp =df_temp.sort_values(by="date")
df_temp.to_csv( wind_wds1.path_out + "\\data_adj\\" + file_name )

### quarter
file_quarter = "date_list_quarter.csv"
# df_temp = pd.read_csv("D:\\db_wind\\data_adj\\"+ "date_list_month.csv")
df_temp["date1"] =df_temp["date"].apply(lambda x : 1 if str(x)[4:6] in ["03","06","09","12"] else 0 )
df_temp = df_temp[ df_temp["date1"]==1 ]
df_temp = df_temp.drop(["date1"],axis=1)

if  os.path.exists( "D:\\db_wind\\" ) :
    df_temp.to_csv("D:\\db_wind\\data_adj\\"+ file_quarter,index=False )
else :
    df_temp.to_csv("F:\\db_wind\\data_adj\\"+ file_quarter,index=False )



# ################################################################################
# ### 2，先下载昨日滚动一致预期数据，因为预期数据第二天才会有完整的 
table_name = "AShareConsensusRollingData"
prime_key = "EST_DT"
prime_key_value = str(int(date_pre))
print( table_name,prime_key ,prime_key_value )
data_obj = wind_wds1.get_table_primekey_input( table_name,prime_key ,prime_key_value )

# 最新日的个股数据
for table_name in ["AShareEODPrices","AShareEODDerivativeIndicator" ]: 
    prime_key = "TRADE_DT"
    prime_key_value = str(int(date_latest ))
    print( table_name,prime_key ,prime_key_value )
    data_obj = wind_wds1.get_table_primekey_input( table_name,prime_key ,prime_key_value )



#################################################################################
### 3，VIP每个交易日自动维护数据下载表格
'''1,下载交易日期，更新交易日rc_WDS_indexdates_20050101_anndate.csv表格  
2,获取最新交易日和需要跟踪的tables，用df_dates更新表格 data_check_anndates.csv 
3,按照 df_data_check_anndates内的更新记录下载表格数据；cell values:1：已有；0：未下载；2：当日无数据
4，用OPDATE更新近15日数据
'''

file_data_check_anndates = "data_check_anndates.csv"
result = wind_wds1.manage_data_check_anndates(file_data_check_anndates )

#################################################################################
### market abcd3d
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "\\db\\" )
sys.path.append(path_ciss_rc + "\\db\\db_assets\\" )
sys.path.append(path_ciss_rc + "\\db\\data_io\\" )

if  os.path.exists( "D:\\db_wind\\" ) :
    path_output = "D:\\CISS_db\\timing_abcd3d\\market_status_group\\"
else :
    path_output = "F:\\CISS_db\\timing_abcd3d\\market_status_group\\"

import pandas as pd 
import numpy as np 
import math
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

from analysis_indicators import analysis_factor
analysis_factor_1 = analysis_factor()

from data_io import data_io
data_io_1 = data_io()
from data_io_pricevol_financial import data_pricevol_financial
data_pricevol_financial_1 = data_pricevol_financial()
data_pricevol_financial_1.print_info()

year="2020"
mmdd_start = input("Type in mmdd start such as yesterday 0528:")
mmdd_end = input("Type in mmdd end such as today 0529:") 
### for stock abcd3d
obj_in={}
obj_in["dict"] ={}
# 如果只计算T日，则输入T-1,T ;如果计算T~N日，则输入T-1,N  ;"20060105"， "20200515"
obj_in["dict"]["date_start"] = year + mmdd_start
obj_in["dict"]["date_end"] = year + mmdd_end
### for marekt abcd3d
obj_date={}
# 如果只计算T日，则输入T-1,T ;如果计算T~N日，则输入T-1,N 
obj_date["date"]=   year +  mmdd_start
obj_date["date_end"]= year+  mmdd_end

########################################################################
### Step 1，数据下载和abcd3d指标计算、导入|| 单个交易日:ADJ_timing_TRADE_DT_20201013_ALL.csv
# notes:至少要前推100个交易日，因此从20060101开始算合适
obj_data = data_pricevol_financial_1.import_data_ashare_change_amt_period( obj_in)
print("Previous dates: ", obj_in["dict"]["date_start"] , obj_in["dict"]["date_end"]    )
########################################################################
### Step 2，市场数据分析:abcd3d全市场个股分组和分行业统计
obj_date = data_io_1.get_trading_days(obj_date) 
# date_list_post  = obj_date["date_list_post"]
date_list = obj_date["date_list_period"]
print( "date_list  " ,date_list  )

obj_ana = {}
obj_ana["dict"] = {}
obj_ana["date_list"] =date_list

obj_ana = analysis_factor_1.market_status_abcd3d_ana( obj_ana) 

print("Path out=",obj_ana["path_output"])
print("list_group_name ",obj_ana["list_group_name"]  )


