# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
功能：对已有的股票基本面数据进行读取或输出，合并拆分，转换等操作。
last update 190411 | since  181018
derived from  get_wind.py\class data_json_rc_head()




数据来源： Wind

===============================================
'''


import json
import pandas as pd 
import sys
sys.path.append("..") 
import datetime as dt 


##############################################################################
##############################################################################
### 展示如何更新组合，| 20190102


##############################################################################
### Step 1 下载最新Wind基本面数据
# from db.assets import stocks
# stocks = stocks()
# wind_api1 = stocks.wind_api()

from db.db_assets.get_wind import wind_api
temp_DB = wind_api()

file_path0 = "D:\\db_wind\\wind_data\\"
file_path_funda = "D:\\db_wind\\wind_data\\funda\\"
time_stamp = dt.datetime.now().strftime('%Y%m%d')
time_stamp ="20180923"

##############################################################################
### Part1 GET RAW INDEX CONSTITUENTS AND WEIGHTS from Wind API
# WindData= temp_DB.GetWind_indexconst(date ,windcode ) 
# file_path = temp_DB.Wind2Csv(WindData,file_path0,windcode, date,time_stamp  )

# # [2011,2012,2013,2014,2015,2016,2017] 
# years = [2007,2008,2009,2010]
# #years = [2006] # ??? 000905 since 20141231 but 2006 is not working 
# count = 0
# for year in years :
#     for mmdd in ['05-31','11-30'] :
#         date = str(year) +'-'+ mmdd
#         # '000300.SH','000905.SH',
#         for windcode in ['000300.SH','000905.SH'] :
#             WindData= temp_DB.GetWind_indexconst(date ,windcode ) 
#             # file saved to "C:\\zd_zxjtzq\\RC_trashes\\temp\\keti_sys_stra_24h\\p1\\wind_data\\"
#             # print("WindData=====")
#             # print( WindData )  

#             file_path = temp_DB.Wind2Csv(WindData,file_path0,windcode, date,time_stamp  )
#             count +=1 
#             print('The '+str(count) +' '+ date+' '+windcode +' has been done ')
#             # 直接新建一个 rc_csi1800 指数，用json描述建立方式

#         if year >=2015 or (date == '2014-11-30') :
#             windcode = '000852.SH'
#             WindData= temp_DB.GetWind_indexconst(date ,windcode ) 
#             file_path = temp_DB.Wind2Csv(WindData,file_path0,windcode, date,time_stamp  )
#             count +=1 
#             print('The '+str(count) +' '+ date+' '+windcode +' has been done ')


##############################################################################
### Part2 Get fundamental indicators for given period and index constituents

# # ### Choice 1 [2013 ~ 2018]
# # years = [2018]  # [2013,2014,2015,2016,2017] 
# # index_list = ['000300.SH','000905.SH','000852.SH']
# # ### Notes: 沪深市场企业员工学历和岗位分类从2013年开始披露，因此2011,2012的数据可能无法使用。
# # items = ['industry_gicscode','west_avgroe_FY2','west_netprofit_CAGR','longcapitaltoinvestment','west_avgoperatingprofit_CAGR','turnover_ttm','employee_tech','employee_MS']

# ### Choice 2 [2007 ~ 2011] | 190411 we only need year 2007~2012 here and no 000852.SH 

# years = [2007,2008,2009,2010,2011,2012 ] 
# index_list = ['000300.SH','000905.SH']
# items = ['industry_gicscode','west_avgroe_FY2','west_netprofit_CAGR','longcapitaltoinvestment','west_avgoperatingprofit_CAGR']

# temp_columns = ['date','rptDate','time_getwind'] + items
# temp_pd0 = pd.DataFrame( columns= temp_columns)
# temp_pd = pd.DataFrame( columns= temp_columns)
# time_stamp_input = "20180923" # timestamp is the day we get 
# country = 'CN'
# # 


# count = 0
# # len_max = 100 for wind limitation, or len_max = 2 for test
# # len_codes = len(code_list) or len_codes = 3 for test
# len_max = 100  # max 100
# len_codes = 5 # 
# for year in years :
#     # year >=2015 or (date == '2014-11-30') :  windcode = '000852.SH' 
#     for mmdd in ['05-31','11-30'] :
#         if mmdd == '05-31' :
#             rptDate = str(year-1) +'1231'
#         elif mmdd == '11-30' :
#             rptDate = str(year) +'0630'
#         date = str(year) +'-'+ mmdd # '2018-05-31' or '20180531' 都可以 
        
#     # CSI指数编制中 05-31对应的是前1年12-31的财务数据已经披露，11-30对应的是当年6-30半年报的数据
#     # 实际工作中5-31应该可以用基于一季报的半年财务数据预测，11-30可以获得基于Q2/Q3的年度财务数据预测
#         # import code lists   ['000300.SH','000905.SH'] 
#         for wind_code in ['000300.SH','000905.SH' ] :
#             count +=1 
#             print('The '+str(count) +' '+ date+' '+wind_code +' is working ... ')
            
#             data_json_rc = temp_DB.Data_funda_csvJson(len_max,len_codes,items,time_stamp,rptDate,wind_code,file_path0,file_path_funda,date,time_stamp_input,country='CN')
#             print('===============1')
#             print( data_json_rc.info )
#             print( data_json_rc.file_json_head )
#             print( data_json_rc.file_csv )
#             print( data_json_rc.file_json )
#             print('===============2')          

#         if year >=2015 or (date == '2014-11-30') :
#             wind_code = '000852.SH'
#             count +=1 
#             print('The '+str(count) +' '+ date+' '+wind_code +' has been done ')
#             data_json_rc = temp_DB.Data_funda_csvJson(len_max,len_codes,items,time_stamp,rptDate,wind_code,file_path0,file_path_funda,date,time_stamp_input,country='CN')
#             print('===============1')
#             print( data_json_rc.info )
#             print( data_json_rc.file_json_head )
#             print( data_json_rc.file_csv )
#             print( data_json_rc.file_json )
#             print('===============2')    


##############################################################################
