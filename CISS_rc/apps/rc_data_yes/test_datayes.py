# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
todo: 研究通联数据提取功能 || 据说1次不超过10万次
notes:通联数据不是365天，而是当年开始算，所以在211221，用201223取不到数据。
url= https://apidoc.datayes.com/app/codeExample
功能：定期检查数据完整性、下载数据。
================= 
数据来源：samplecode_win36.py
last   | since 211220
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
from get_data_yes import data_yes
class_data_yes = data_yes()
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
### 初始化
# from dataapi_win36 import Client
# client = Client()
# client.init('08b8e4bd50638f2ad23b92e6be3ce8180ea08e744825f07ce27f93deb1f75e12')

#########################################################################
### TEST 

#########################################################################
### 每日维护数据表格  
######################################################################### 
### 1，导入数据目录，更新交易日和股票代码等基础信息
obj_data = class_data_yes.update_log_date_code()
# output:obj_data["date_list"] ,obj_data["date_list_hk"]  

########################################################################################
### 2，股票数据维护
### 沪深股票日行情,	getMktEqud,获取沪深股票未复权日行情信息，包含昨收价、开盘价、最高价、最低价、收盘价、成交量、成交金额等字段（含科创板）。
### getMktEqudAdj
### 沪深估值信息	getMktEqudEval
# 股票日资金流向单类明细	getMktEquFlowOrder
#  getMktRankListSales
# 基金
# 基金前复权行情 getMktFunddAdjBf
obj_data["dict"]["table_name"] = "getMktFunddAdjBf"
# obj_data["dict"]["table_name"] = "financial_gene_qincome"
df_table=  class_data_yes.get_table_by_stock( obj_data ) 

# TODO:应该新建一个专属的log，按季度保存每个股票是否已经下载了数据！
 

asd


######################################
### 

### notes:datayes的ticker不接受600036.SH格式，只接受600036或者他自己的代码后缀
url_json= "/api/market/getMktEqud.json?field=&beginDate=&endDate=&secID=&ticker=&tradeDate=20201223"


### notes:用csv的方法会出现返回的bytes变量报错，但json就不会：UnicodeDecodeError: 'utf-8' codec can't decode byte 0xb0
code, result = client.getData(url_json)#调用getData函数获取数据，数据以字符串的形式返回
dict1=json.loads(result)

### 1，result类型是bytes，若转化成dict字典，data_dict=  json.loads(result)
# data_dict.keys() = dict_keys(['retCode', 'retMsg', 'data'])
# 数据：data_dict["data"][0]， 如果有多条记录，就 data_dict["data"][0~N]

#########################################################################
### 
'''TODO
1,导入基础数据；最近1年交易日
2,按交易日下载每日A股行情数据
3,保存至excel文件
'''








































asd



#########################################################################
### 4，港股数据维护 || 仅仅维护港股通股票
### 1.1.8 沪深港通成分股,quarter;shszhk_stock_list()
### 5.1.4 港股股票基本信息，"hk_secu":需要上市日期来确保不浪费下载时间
### 5.3.4 港股盈利能力 hk_profit_ability
### 5.2.1 港股日行情 "hk_daily_quote"  

# from get_yes_data_hk import data_yes_hk
# class_data_yes_hk = data_yes_hk()
# obj_data["dict"]["table_name"] = "hk_daily_quote"  
# df_table=  class_data_yes_hk.get_table_by_hk( obj_data ) 

# asd 


#########################################################################
### 3，基金数据维护
######################################
### 2.1.10 基金分类	,# df_table= fund_type(en_prod_code = "580001.OF")
# notes:publish_date 有问题，很多基金在 211210的最近发布日期210930，
# obj_data["dict"]["table_name"] = "fund_profile"
# df_table=  class_data_yes.get_table_by_fund( obj_data )

######################################
### 2.2.6 基金净值指标 ;记录基金每日单位基金净值，基金每周单位基金净值等资料；
from get_yes_fund import data_yes_fund
class_data_yes_fund = data_yes_fund()
obj_data["dict"]["table_name"] = "fund_net_value"
df_table=  class_data_yes_fund.get_table_by_fund( obj_data )

### TODO：1，获取基金成立日，计算需要下载数据的区间 ；2.1.4 基金概况
### 2，获取基金

asd











 
 


#######################################
### passed time 
time2 =  dt.datetime.now()
print(dt.datetime.strftime(time1, "%Y%m%d %H%M%S"),time.strftime('%Y%m%d %H%M%S',time2) )  

asd 



 










