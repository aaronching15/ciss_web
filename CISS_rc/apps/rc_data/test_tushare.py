# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
todo: 研究tushare数据提取功能 || 1分钟不超过5次！
您每天最多访问该接口20次

=================
notes:1,积分100元以内：Exception: 抱歉，您每分钟最多访问该接口5次，
权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。

数据来源：C:\rc_HUARONG\rc_HUARONG\ciss_web\CISS_rc\db\data_io 
data_io_tushare.py
last   | since 211122
===============================================
'''
#########################################################################
### Part 0, update log of data in json file and necessarily modules\parameters 
import sys,os
# 添加祖父目录
sys.path.append("../..")
sys.path.append("C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\")
sys.path.append( os.getcwd()[:2] +"\\rc_HUARONG\\rc_HUARONG\\ciss_web\\CISS_rc\\apps\\rc_data" )
sys.path.append( os.getcwd()[:2] +"\\rc_HUARONG\\rc_HUARONG\\ciss_web\\CISS_rc\\db\\data_io" )

# from db.data_io.data_io_tushare import ts_api
from data_io_tushare import data_ts
data_ts_1 = data_ts()
import pandas as pd 

import time
temp_time1 = time.strftime('%y%m%d%H%M%S',time.localtime(time.time()))
print( temp_time1 ) 

#########################################################################
### GetWindData:个股历史前复权数据 

#######################################
### 导入历史交易日
path_trading_days = "D:\\db_wind\\data_adj\\"
file_trading_days = "date_list_tradingday.xlsx"
df_days = pd.read_excel(path_trading_days + file_trading_days)
list_date = list( df_days["date"] )
list_date = [i for i in list_date if i > 20060701]
list_date.sort()

#######################################
### 获取基金历史净值数据
obj_data ={}
obj_data["type"] = "nav" # basic 对应基础信息

for temp_date in list_date : 
    print("Working on fund_nav:" ,temp_date  )
    obj_data["temp_date"] =  str(temp_date)  # "20211119"
    obj_data = data_ts_1.get_ts_fund_basic( obj_data)
    ### notes: 1分钟不超过5次，也就是12秒
    time.sleep( 15 )

print( obj_data["df_nav_date"].head()  )

asd 


#######################################
### 基金基础信息



#######################################


#######################################
#######################################


#######################################























