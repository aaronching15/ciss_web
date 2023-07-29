# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
todo:  

功能：下载单只证券历史行情
数据来源： Wind-API 万得量化数据接口
last update   | since  190821
/ 
===============================================
'''
#########################################################################
### Part 0, update log of data in json file and necessarily modules\parameters 
import sys
# 添加祖父目录
sys.path.append("../..")
sys.path.append("C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\")

from db.db_assets.get_wind import wind_api
wind_api_1 = wind_api()

import time
temp_time1 = time.strftime('%y%m%d%H%M%S',time.localtime(time.time()))
print( temp_time1 )

file_path0 = "D:\\data_Input_Wind\\"

#########################################################################
### GetWindData:个股历史前复权数据
code='139263.MI'
date_0='20050101'
date_1=''
items ='open,high,low,close,pct_chg' # 'open,high,low,close,volume,amt,pct_chg'
winddata_1 = wind_api_1.GetWindData(code , date_0 , date_1 , items , 1)


#########################################################################
### Wind2Csv_wsd:WSD获得的个股历史数据保存至csv

wind_api_1.Wind2Csv_wsd( winddata_1,file_path0,code  )

# 大盘 '139200.MI' | 中盘 139215 | 小盘 139263 | 大中小盘 139251
