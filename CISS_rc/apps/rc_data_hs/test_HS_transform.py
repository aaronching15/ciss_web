# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
todo: 研究HS恒生数据提取功能 || 1分钟不超过120次 
url=https://udata.hs.net/datas/644/onlinePreview
功能：数据转换，例如将基于个股保存的数据改成基于日期
================= 
数据来源：C:\rc_HUARONG\rc_HUARONG\ciss_web\CISS_rc\db\data_io\get_hs_data.py
last   | since 211216
===============================================
'''
#########################################################################
### Part 0, update log of data in json file and necessarily modules\parameters 

import sys,os
# C:\rc_HUARONG\rc_HUARONG\ciss_web\CISS_rc\db\db_assets
sys.path.append( os.getcwd()[:2] +"\\rc_HUARONG\\rc_HUARONG\\ciss_web\\CISS_rc\\db\\db_assets" )
sys.path.append("../..") 
sys.path.append( os.getcwd()[:2] +"\\rc_HUARONG\\rc_HUARONG\\ciss_web\\CISS_rc\\apps\\rc_data" )
sys.path.append( os.getcwd()[:2] +"\\rc_HUARONG\\rc_HUARONG\\ciss_web\\CISS_rc\\db\\data_io" )

# from db.data_io.data_io_tushare import ts_api
from get_hs_data import data_hs
class_data_hs = data_hs()
import pandas as pd 

import time
import datetime as dt  
# 获取当前时间;'%Y%m%d=20211208, '%y%m%d=211208
# temp_time1 =  time.localtime(time.time()) 
# print( time.strftime('%Y%m%d %H%M%S',temp_time1) )  
time1 = dt.datetime.now()
time1_str = dt.datetime.strftime(time1, "%Y%m%d")
print( time1_str )
#########################################################################
### TEST  
'''TODO
1,
2,下载A股基础信息，"stock_Info" ；调整a股起始日
3,根据基金基础信息，调整起始日
4，下载A股，accounting_data
5，港股，
'''

#########################################################################
### 每日维护数据表格  
######################################################################### 
### 1，导入数据目录，更新交易日和股票代码等基础信息
obj_data = class_data_hs.update_log_date_code()
# output:obj_data["date_list"] ,obj_data["date_list_hk"]  

#########################################################################
### 数据转换
#########################################################################
### 2，给定数据表，将基于个股保存的数据改成基于日期保存
### column_date= "report_date"，是表格里用于区分日期的列名
# "accounting_data"，"report_date"，季末日期
# "stock_key_indicator"，"end_date"，季末日期
# "financial_gene_qincome"，report_date，季末日期
# 5.3.4 港股盈利能力, "hk_profit_ability",report_date, na
# 1.2.17 日行情序列, "stock_quote_daily_list", "trading_date"
obj_data["dict"]["table_name"] = "stock_quote_daily_list"
obj_data["dict"]["column_date"] ="trading_date"
# obj_data["dict"]["table_name"] = "financial_gene_qincome"
df_table=  class_data_hs.trans_table_code2date( obj_data ) 


 

asd

################################################
                        

#########################################################################
### 4，港股数据维护
df_table

#######################################
### passed time 
time2 =  dt.datetime.now()
print(dt.datetime.strftime(time1, "%Y%m%d %H%M%S"),time.strftime('%Y%m%d %H%M%S',time2) )  

asd 
 








