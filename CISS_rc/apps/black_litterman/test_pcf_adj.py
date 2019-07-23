# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
需求：
建立ETF组合日常管理的仿真脚本
last 190723 || since 190723

Function:功能：
1, 维护分红送配数据，确定pcf调整的细节。
	1.1，下载中证800、中证1000的
2，


预案登记日：dividends_announce_date
实施公告日：shareregister_date
一般实施公告日早于预案登记日，如7-17预案，7-23股权登记。
因此我们选择orderby=实施公告，可以提前开始准备
w.wset("bonus","orderby=实施公告日;startdate=2019-07-23;enddate=2019-07-23;sectorid=a00103020a000000")

板块对应：
中证800：a00103020a000000 or 1000011893000000
中证1000：1000012163000000
全部a股：

所需关键词：

todo:

Notes:
##############################################
'''

##################################################################
### Initialization
import json
import pandas as pd 
import numpy as np 
import math
import sys
sys.path.append("C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\db\\db_assets\\")
from get_wind import wind_api 
wind_api1 = wind_api()

from etf.engine_etf import ETF_manage
etf_manage0 = ETF_manage()

##################################################################
### 获取近期分红送配数据，存入现有的CSV文件
# get_wind.py

### Read existing file 
# path to save dividend and share_proportion
file_path0 = "D:\\data_Input_Wind\\temp\\"
file_name_800 = "Wind_csi800_bonus.csv"
file_name_1000 = "Wind_csi1000_bonus.csv"

list1= [ ["csi800","csi1000"],["a00103020a000000","1000012163000000"],[file_name_800,file_name_1000] ]
print("list \n", list1 )

### Generate parameter for wind api 
'''
w.wset("bonus","orderby=股权登记日;startdate=2019-07-23;enddate=2019-07-23;sectorid=a00103020a000000")
'''
# date_start = input( "Starting date for 实施公告日: e.g.190724..." )
# date_end   = input( "Ending date for 实施公告日: e.g.190724..." )
date_start = "190723"
date_end   = "190724"

# 190701 to  2019-07-01
import datetime as dt 
date_start =dt.datetime.strftime( dt.datetime.strptime("20"+date_start,"%Y%m%d"),"%Y-%m-%d")
date_end   =dt.datetime.strftime( dt.datetime.strptime("20"+date_end,"%Y%m%d"),"%Y-%m-%d")

### CSI800
para_dict = {}
para_dict["type_bonus"] = "bonus"
para_dict["orderby"]   = "实施公告日"  # "股权登记日"  # 
para_dict["date_start"] =date_start  # "2019-07-01" 
para_dict["date_end"] = date_end
para_dict["fields"] = "wind_code,sec_name,reporting_date,progress,dividendsper_share_pretax,sharedividends_proportion,shareincrease_proportion,share_benchmark,exrights_exdividend_date,dividend_payment_date"

type_bonus= para_dict["type_bonus"]
orderby= para_dict["orderby"] 
date_start= para_dict["date_start"]
date_end= para_dict["date_end"]
fields = para_dict["fields"]

# import WindPy as WP
# WP.w.start()

# for i in [0,1] :

# 	para_dict["sector_name"] = list1[0][i] # "csi800"  
# 	para_dict["sectorid"] = list1[1][i] #"a00103020a000000"
# 	para_dict["file_name_csv"] = list1[2][i] #"a00103020a000000"

# 	sectorid= para_dict["sectorid"]

# 	para = "orderby="+orderby+";startdate="+date_start+";enddate="+date_end
# 	para = para +";sectorid="+sectorid;"field="+fields
# 	print("para \n" + para)

# 	wind_data0 = WP.w.wset(type_bonus,para )

# 	df1 = wind_api1.Wind2df_wset(wind_data0 )

# 	file_name  = para_dict["file_name_csv"]
	
# 	### First time 
# 	# df1.to_csv(file_path0+file_name ) 

# 	### Update case: Append to csv 
# 	### read existing csv 
# 	df0 = pd.read_csv(file_path0+file_name ,encoding="gbk")
# 	df0=df0.append(df1)
# 	df0.to_csv(file_path0+file_name ) 

# 	### save para_dict to json file ,TODO

file_name =  "Wind_csi800_bonus.csv"
df0 = pd.read_csv(file_path0+file_name ,encoding="gbk")


datetime0 = date_end + " 00:00:00"
# "2019-07-23 00:00:00" column最后2列的值是一样的
### return 
register_date =  df0[ df0["shareregister_date" ] == datetime0 ]
print("股权登记日")
# print( register_date.columns)
print( register_date.loc[:,["wind_code","sec_name","scheme_des"] ] ) 

### Import PCF file 
# path_etf = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\apps\\black_litterman\\etf\\"
path_etf = "D:\\CISS_db\\etf\\"
name_etf = "510300"
date_init = "0723"
df_head,df_stocks = etf_manage0.get_pcf_file(date_init,name_etf,path_etf )
df_head.index = df_head.key
print("df_head ", df_head)
print( df_head.loc["TradingDay","value"] )
print("Head of df_stocks \n", df_stocks.head() )

### 寻找 pcf文件中有持仓的股票，


for temp_index in register_date.index :
	wind_code = register_date.loc[temp_index,"wind_code" ]
	register_date.loc[temp_index,"code_raw" ] =wind_code[:6]

df_stocks.index = df_stocks["code"]
print("6666======================")
list_code = list(register_date["code_raw"]) 

df_stocks2 =  df_stocks.loc[list_code,:] 
# axis=0 means delete by rows
df_stocks2 = df_stocks2.dropna( axis=0 )

import numpy as np 
for temp_i in df_stocks2.index :
	temp_code =  df_stocks2.loc[temp_i, "code" ]
	print(temp_code, temp_i)
	temp_i2 = register_date[ register_date["code_raw"] == temp_code ].index[0]
	
	# df_stocks2.loc[temp_i, "scheme_des" ] =0
	
	df_stocks2.loc[temp_i, "scheme_des" ] =register_date.loc[temp_i2, "scheme_des"]

	df_stocks2.loc[temp_i, "cash_per_share" ] =register_date.loc[temp_i2, "dividendsper_share_aftertax"]
	### 计算分红的现金差额数据
	df_stocks2.loc[temp_i, "cash_diff" ] =df_stocks2.loc[temp_i, "cash_per_share" ]*float( df_stocks2.loc[temp_i, "num" ])

	### 送股,float
	df_stocks2.loc[temp_i, "share_div" ] =register_date.loc[temp_i2, "sharedividends_proportion"]
	if not df_stocks2.loc[temp_i, "share_div" ] == np.NaN :
		df_stocks2.loc[temp_i, "num_new" ] = df_stocks2.loc[temp_i, "num"] *(1.0 + df_stocks2.loc[temp_i, "share_div" ] ) 
	### 转增股,float
	df_stocks2.loc[temp_i, "share_increase" ] =register_date.loc[temp_i2, "shareincrease_proportion"]
	if not df_stocks2.loc[temp_i, "share_div" ] == np.NaN :
		df_stocks2.loc[temp_i, "num_new" ] = df_stocks2.loc[temp_i, "num"] *(1.0 + df_stocks2.loc[temp_i, "share_increase" ] )

	df_stocks2.loc[temp_i, "date_announce" ] =register_date.loc[temp_i2, "dividends_announce_date"]
	df_stocks2.loc[temp_i, "date_register" ] =register_date.loc[temp_i2, "shareregister_date"]
	df_stocks2.loc[temp_i, "date_share_pay" ] =register_date.loc[temp_i2, "exrights_exdividend_date"]
	df_stocks2.loc[temp_i, "date_cash_pay" ] =register_date.loc[temp_i2, "dividend_payment_date"]

	### todo，计算调整后的股票数量。 


sad

print(  df_stocks2   )
df_stocks2.to_csv("D:df_stocks_190723.csv")
# todo,对于现金分红的情况，计算分红所得现金；对于股票新增的情况，计算股票的变化
# Qs:


########################################################
# Qs:1,Wind-navigator 有问题
'''
1,获取未来5个交易日的权息变动情况；
2，对下一个交易日将发生的分红送配，在所有pcf中寻找持仓，设计组合调整计划




'''














