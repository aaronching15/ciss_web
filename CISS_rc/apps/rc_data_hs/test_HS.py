# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
todo: 研究HS恒生数据提取功能 || 1分钟不超过120次 
url=https://udata.hs.net/datas/644/onlinePreview
功能：定期检查数据完整性、下载数据。
================= 
数据来源：C:\rc_HUARONG\rc_HUARONG\ciss_web\CISS_rc\db\data_io\get_hs_data.py
last   | since 211122
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
### INPUT
print("恒生数据还是不全：220214，Wind股票基金池里2449个基金，只有700个有匹配代码，其余1400个都没有代码！")
data_type= input("输入数据类型：1，A股A；2，基金fund；3，港股hk：")

#########################################################################
### 每日维护数据表格  
######################################################################### 
### 1，导入数据目录，更新交易日和股票代码等基础信息
obj_data = class_data_hs.update_log_date_code()
# output:obj_data["date_list"] ,obj_data["date_list_hk"]  

########################################################################################
### 2，股票数据维护
### 1.1.5 股票简介——上市日期；"stock_Info",季度维护；1.1.7 ST股票列表，st_stock_list()	
### 按个股-开始和结束时间-返回季度财务数据："stock_key_indicator"
### 按个股-季度末："accounting_data",
### 按个股-区间交易日："stock_quote_daily_list"
### 1.4.7 利润表-一般企业(单季)	  financial_gene_qincome |notes:所有股票数据从20160930才有！

if data_type in [1,"1","股票","a","A","A股","ashares"]:
    obj_data["dict"]["table_name"] = "accounting_data"
    # obj_data["dict"]["table_name"] = "financial_gene_qincome"
    df_table=  class_data_hs.get_table_by_stock( obj_data ) 


#########################################################################
### 4，港股数据维护 || 仅仅维护港股通股票
### 1.1.8 沪深港通成分股,quarter;shszhk_stock_list()
### 5.1.4 港股股票基本信息，"hk_secu":需要上市日期来确保不浪费下载时间
### 5.3.4 港股盈利能力 hk_profit_ability
### 5.2.1 港股日行情 "hk_daily_quote"  

if data_type in [3,"3","港股","h","H","hk","HK"]:
    from get_hs_data_hk import data_hs_hk
    class_data_hs_hk = data_hs_hk()
    obj_data["dict"]["table_name"] = "hk_daily_quote"  
    df_table=  class_data_hs_hk.get_table_by_hk( obj_data ) 



#########################################################################
### 3，基金数据维护
######################################
### 2.1.10 基金分类	,# df_table= fund_type(en_prod_code = "580001.OF")
# notes:publish_date 有问题，很多基金在 211210的最近发布日期210930，
# obj_data["dict"]["table_name"] = "fund_profile"
# df_table=  class_data_hs.get_table_by_fund( obj_data )

######################################
### 2.2.6 基金净值指标 ;记录基金每日单位基金净值，基金每周单位基金净值等资料；
if data_type in [2,"2","基金","f","F","fund","FUND"]:
    print("恒生数据还是不全：220214，Wind股票基金池里2449个基金，只有700个有匹配代码，其余1400个都没有代码！")
    from get_hs_fund import data_hs_fund
    class_data_hs_fund = data_hs_fund()
    obj_data["dict"]["table_name"] = "fund_net_value" 
    df_table=  class_data_hs_fund.get_table_by_fund( obj_data )
 





  


#######################################
### passed time 
time2 =  dt.datetime.now()
print(dt.datetime.strftime(time1, "%Y%m%d %H%M%S"),dt.datetime.strftime(time2,'%Y%m%d %H%M%S') )  

asd 



 










