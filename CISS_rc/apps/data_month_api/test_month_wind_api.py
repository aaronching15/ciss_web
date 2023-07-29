from django.test import TestCase

# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import json
import time
import datetime as dt

import sys,os 
######################################################################################################
path_ciss_web = os.getcwd().split("ciss_exhi")[0]
path_ciss_rc = path_ciss_web +"\\CISS_rc\\"
sys.path.append(path_ciss_rc)
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets" )

######################################################################################################
### 从Config导入常用目录地址
from config_data import config_data
config_data1 = config_data()
path_dict = config_data1.obj_config["dict"] 
path_ciss_web = path_dict["path_ciss_web"]
path_ciss_rc = path_dict["path_ciss_rc"]
path_ciss_exhi= path_dict["path_ciss_exhi"] 
path_db = path_dict["path_db"]
path_db_times = path_dict["path_db_times"]
path_db_assets = path_dict["path_db_assets"]

### 导入数据地址          
path_data_pms = path_dict["path_data_pms"]
path_data_adj = path_dict["path_data_adj"]
path_fundpool = path_dict["path_fundpool"]
path_wind_terminal = path_dict["path_wind_terminal"]  
path_wsd = path_dict["path_wsd"] 
path_wss = path_dict["path_wss"] 

######################################################################################################
### 1, 日期管理

# from times import times
# times1 = times()
# obj_dt = times1.manage_date_trade( )
# # obj_dt["df_dt"]

######################################################################################################
### 下载月度行情数据和核心指标：个股、指数、基金
# notes：3种资产的指标不太一样。
# reference：get_wind_api.py\def get_wss_ma_amt_mv
from assets import quote_ashares_index_fund_month
quote_1 = quote_ashares_index_fund_month()

obj_data = {}
obj_data = quote_1.manage_quote_ashares_month(obj_data)

asd 

###################################################   
### step 0，月末日期的日志管理，从202208 to 201908 
### 导入日期数据，file_dt=date_trade.xlsx; path= C:\rc_202X\rc_202X\ciss_web\CISS_rc\db\db_times
file_dt="date_trade.xlsx"
# path_dt = "C:\\rc_202X\\rc_202X\\ciss_web\\CISS_rc\\db\\db_times\\"
df_dt = pd.read_excel( path_db_times + file_dt )
df_dt = df_dt[ df_dt["exchange"] =="SSE" ]
df_dt = df_dt[ df_dt["type_date"] =="d" ]
df_dt = df_dt.sort_values(by="date",ascending=True  )

### 
temp_date = "20220831"
temp_date_start = temp_date[:6] +"01"
temp_date_end   = temp_date
### 输入一个最近日期导出的a股数据
temp_date_pre = "20220812"

### 获取给定月末日期的前1个交易日，为了计算动量ma指标
df_temp = df_dt[ df_dt["date"]<int(temp_date)  ]
temp_date_pre_1d = df_temp["date"].values[-1]

###################################################
### step 1,确定要获取数据的股票列表 | 每3个月从Wind导出一次股票列表
### 导入给定日期前最近一期的A股列表、港股通港股列表、指数列表、基金列表  

###################################
### A股列表
# 剔除A股总市值后20%、小于90亿元。名称里带有st，但是有的st是历史包袱已经重组了。

# temp_key = "ashares"
# file_import = temp_key +"_shares_"+ temp_date_pre +".xlsx"
# df_data = pd.read_excel( path_data_adj + file_import )
# print( "df_data \n ", df_data.head().T )

# ### 以总市值90亿为标准，A股股票数量从3500 to 1655，2022-8
# # 获取前95%最大的值， np.percentile( df1[col_name],95)
# df_data = df_data[ df_data["总市值1"] >9000000000 ]


###################################
### 港股列表：港股通 TODO
# notes：港股没有预期数据，除非是AH股票，不需要提取："estpe_","west_"
# w.wsd("0700.HK", "pre_close,close,high,low,amt,west_pe,est_peg,west_netprofit_YOY,west_avgnp_yoy,west_nproc_1w,west_nproc_4w,west_sales_YOY,west_sales_CAGR,ev,pe_ttm,pcf_ocf_ttm,estpe_FY1,estpe_FY2", "2022-08-31", "2022-08-31", "year=2022;westPeriod=180;rptYear=2022;unit=1;Period=M")

# temp_key = "hk"
# file_import = temp_key +"_shares_"+ temp_date_pre +".xlsx"
# df_data = pd.read_excel( path_data_adj + file_import )
# print( "df_data \n ", df_data.head().T )

### 港股流动性较差，入选的800只股票80亿总市值以内几乎无投资和交易价值，对应
# # 成交金额不能作为依据，因为有的重要公司只是几个月临时停牌。
# df_data = df_data[ df_data["总市值1"] >90*10000*10000 ] 

###################################
### 美股列表：标普500成分股 TODO
# w.wss("ABMD.O,AAPL.O", "close,ev,amt,MA,val_pettm_low,val_pettm_high","tradeDate=20220831;priceAdj=F;cycle=D;unit=1;MA_N=40;startDate=20220801;endDate=20220831")
# temp_key = "us" | file_name = SP500成份_20220920.xlsx 

# temp_key = "us" 
# file_import = "SP500成份_20220920.xlsx"
# df_data = pd.read_excel( path_wind_terminal + file_import )
# print( "df_data \n ", df_data.head().T )

###################################
### 指数列表：每半年确定一次 TODO
# 导入定期梳理的指数列表，sheet=index_list,file=db_manage.xlsx
# 只筛选A股指数 type=ashares  ;code=000300.SH ; name=沪深300
# A股指数只能提取到pe_ttm，pcf_ocf_ttm，提取不了pb_mrq，mkt_cap。感觉没什么用。 

# temp_key = "index" 
# file_import = "db_manage.xlsx"
# df_data = pd.read_excel( path_ciss_exhi + file_import,sheet_name="index_list" )

# print( "df_data \n ", df_data.head().T )


###################################
### 基金列表：最近4个季度的基础池 
# 基金池文件：主动股票（包括偏股和灵活配置）
# 核心池100个、基础池400个或前20%基金数量。
fund_list=["主动股票","","",""]
temp_key = "fund" 
file_import = "基金池rc_主动股票.xlsx"
df_data = pd.read_excel( path_fundpool + file_import,sheet_name="基础池" )

print( "df_data \n ", df_data.head().T )


###################################################
### step 2，下载行情、估值、预测数据 | wss一次只能1个代码*多个指标或多个代码*1个指标
# code_list = df_data.loc[ 0:100, "代码"]
# print("lenth of code_list:", len(code_list),code_list  )

from get_wind_api import wind_api
wind_api1 = wind_api()

obj_data = {}
if not "code" in df_data.columns :
    if "代码" in df_data.columns :
        df_data["code"] =df_data["代码"] 
    if "基金代码" in df_data.columns :
        df_data["code"] =df_data["基金代码"] 

obj_data["df_data"] =  df_data
obj_data["trade_date"] = temp_date 

## TEST 测试
obj_data["df_data"] =  df_data.head(12) 

col_list = ["code"]

######################################################################################################
### 行情类指标，quote: | 月度频率 cycle=M 
# dict_col_indi 是col_name和indicator_name的一一对应

if temp_key in ["ashares","hk","index","us" ] :

    dict_col_indi = {} 
    dict_col_indi["pre_close-1m"] = "pre_close"
    dict_col_indi["close-1m"] = "close"
    dict_col_indi["high-1m"] = "high"
    dict_col_indi["low-1m"] = "low"
    dict_col_indi["amt-1m"] = "amt"
    dict_col_indi["pct_chg-1m"] = "pct_chg" 

    for temp_col in dict_col_indi.keys() :   
        obj_data["col_name"] = temp_col
        obj_data["indicator_name"] = dict_col_indi[ temp_col ]
        obj_data = wind_api1.get_wss_close_pctchg_amt( obj_data)
        ### 
        col_list = col_list + [temp_col]

    print("Debug ",col_list )
    print("Debug ",df_data )
    ### save to excel
    df_data = obj_data["df_data"]
    file_name2 = "month-end_"+ temp_key +"_shares_"+ temp_date_end +".xlsx"
    df_data.loc[:,col_list].to_excel(path_wsd + file_name2  ,index=False )


######################################################################################################
### 均线类指标 |涉及月底上一个交易日， ma类：para_ma in ["16","40" ]
if temp_key in ["ashares","hk","index","us" ] :
    dict_col_ma = {} 
    dict_col_ma["ma16"] =    ["16", temp_date ]
    dict_col_ma["ma16_pre"] =["16", temp_date_pre_1d ]
    dict_col_ma["ma40"] =    ["40", temp_date ]
    dict_col_ma["ma100"] =   ["100", temp_date ]

    for temp_col in dict_col_ma.keys() :   
        obj_data["col_name"] = temp_col
        obj_data["para_ma"]  = dict_col_ma[ temp_col ][0]
        obj_data["trade_date"]  = dict_col_ma[ temp_col ][1]
        obj_data = wind_api1.get_wss_ma_n( obj_data)
        ### 
        col_list = col_list + [temp_col]
    
    ### save to excel
    df_data = obj_data["df_data"]
    file_name2 = "month-end_"+ temp_key +"_shares_"+ temp_date_end +".xlsx"
    df_data.loc[:,col_list].to_excel(path_wsd + file_name2  ,index=False )
 


###################################################
### 估值类指标
# A股指数只能提取到pe_ttm，pcf_ocf_ttm，提取不了pb_mrq，mkt_cap。感觉没什么用。 
''' 估值类：,"mkt_cap"
# notes:股票市值用ev或mkt_cap、mkt_cap_ard都行，指数市值必须用mkt_cap_ard ；ev1是企业价值不等于市值。
# notes:指数没有pb_mrq,pcf_ocf_ttm
w.wss("000903.SH,300750.SZ", "pe_ttm,pb_mrq,pb_lf","tradeDate=20220831")
w.wss("000903.SH,300750.SZ", "mkt_cap_ard","unit=1;tradeDate=20220831") 
''' 
if temp_key in ["ashares","hk","us"] :
    obj_data["trade_date"] = temp_date 

    dict_col_indi = {} 
    dict_col_indi["pe_ttm"] = "pe_ttm"
    dict_col_indi["pb_mrq"] = "pb_mrq"
    dict_col_indi["pcf_ocf_ttm"] = "pcf_ocf_ttm"
    dict_col_indi["mkt_cap"] = "mkt_cap" 

    for temp_col in dict_col_indi.keys() :   
        obj_data["col_name"] = temp_col
        obj_data["indicator_name"] = dict_col_indi[ temp_col ]
        obj_data = wind_api1.get_wss_close_pctchg_amt( obj_data)
        ### 
        col_list = col_list + [temp_col]

        
    ### save to excel
    df_data = obj_data["df_data"]
    file_name2 = "month-end_"+ temp_key +"_shares_"+ temp_date_end +".xlsx"
    df_data.loc[:,col_list].to_excel(path_wsd + file_name2  ,index=False )


###################################################
### 预测类指标
# notes：港股没有预期数据，除非是AH股票，不需要提取："estpe_","west_"
'''预测类
w.wsd("300146.SZ", "estpe_FY1,estpe_FY2,estpeg_FY1,estpeg_FY2,west_netprofit_YOY,west_avgnp_yoy,
    west_nproc_1w,west_nproc_4w,west_sales_YOY,west_sales_CAGR,", 
    "2022-08-01", "2022-08-31", "unit=1;currencyType=;year=2022;Period=M")
1，w.wss("000903.SH,300750.SZ", "estpe_FY1","tradeDate=20220919")
2，一致预期类：w.wss("000903.SH,300750.SZ", "west_sales_FY1","unit=1;tradeDate=20220919")
3，w.wss("000903.SH,300750.SZ", "west_netprofit_YOY","tradeDate=20220919")
4，盈利预测变化率1周和4周； w.wss("000903.SH,300750.SZ", "west_nproc_1w","year=2022;tradeDate=20220919")
5，w.wss("000903.SH,300750.SZ", "west_sales_YOY,west_sales_CAGR","tradeDate=20220831")
'''
if temp_key in ["ashares" ] :
    obj_data["trade_date"] = temp_date 
    dict_col_indi = {} 
    dict_col_indi["estpe_FY1"] = "estpe_FY1"
    dict_col_indi["estpe_FY2"] = "estpe_FY2"
    dict_col_indi["estpeg_FY1"] = "estpeg_FY1"
    dict_col_indi["estpeg_FY2"] = "estpeg_FY2"
    dict_col_indi["west_netprofit_YOY"] = "west_netprofit_YOY"
    dict_col_indi["west_avgnp_yoy"] = "west_avgnp_yoy"
    dict_col_indi["west_sales_YOY"] = "west_sales_YOY"
    dict_col_indi["west_sales_CAGR"] = "west_sales_CAGR"

    for temp_col in dict_col_indi.keys() :   
        obj_data["col_name"] = temp_col
        obj_data["indicator_name"] = dict_col_indi[ temp_col ]
        obj_data = wind_api1.get_wss_close_pctchg_amt( obj_data)
        ### 
        col_list = col_list + [temp_col]
        
        
    ### save to excel
    df_data = obj_data["df_data"]
    file_name2 = "month-end_"+ temp_key +"_shares_"+ temp_date_end +".xlsx"
    df_data.loc[:,col_list].to_excel(path_wsd + file_name2  ,index=False )

######################################################################################################
### 基金类指标，行情quote和绩效 | 月度频率 
# 单位净值,nav,复权单位净值,NAV_adj, 
# 3种月收益率提取一样：近1月回报，return_1m；单月度回报，return_m,区间回报,return,2022-08-01；2022-08-31
# 涉及"区间"的指标都要开始和结束日期：同类基金区间收益排名（百分比）peer_fund_return_rank_prop_per;
# 近1月回报排名,periodreturnranking_1m;今年以来回报排名,periodreturnranking ytd
# w.wss("519212.OF,166002.OF,004685.OF", "nav,NAV_adj,NAV_adj_return1,NAV_adj_chg,return_1m,return_m,return,
# peer_fund_return_rank_prop_per,periodreturnranking_1m,periodreturnranking_ytd",
# "tradeDate=20220831;startDate=20220801;endDate=20220831;annualized=0;fundType=2")
# 如果不包含区间，那么 w.wss("519212.OF,166002.OF,004685.OF", "return_m,periodreturnranking_1m,periodreturnranking_ytd","tradeDate=20220831;fundType=2")
# fundType=2 指的是基金的二级分类，

if temp_key in ["fund" ] :
    obj_data["trade_date"] = temp_date 
    dict_col_indi = {} 
    dict_col_indi["nav"] = "nav"
    dict_col_indi["NAV_adj"] = "NAV_adj"
    dict_col_indi["return_m"] = "return_m" 
    ### 获取基金多个代码的合并规模
    dict_col_indi["netasset_total"] = "netasset_total" 
    ### 基金排名
    dict_col_indi["peer_fund_return_rank_prop_per"] = "peer_fund_return_rank_prop_per"
    dict_col_indi["periodreturnranking_1m"] = "periodreturnranking_1m"
    dict_col_indi["periodreturnranking_ytd"] = "periodreturnranking_ytd" 
    
    for temp_col in dict_col_indi.keys() :   
        obj_data["col_name"] = temp_col
        obj_data["indicator_name"] = dict_col_indi[ temp_col ]
        obj_data = wind_api1.get_wss_fund_nav_rank( obj_data)
        ### 
        col_list = col_list + [temp_col]
        
        
    ### save to excel
    df_data = obj_data["df_data"]
    file_name2 = "month-end_"+ temp_key +"_shares_"+ temp_date_end +".xlsx"
    df_data.loc[:,col_list].to_excel(path_wsd + file_name2  ,index=False )


######################################################################################################
### 存入sql-table ：基金、指数、A股、港股美股
# table_name = quote_ashares_stock_fund_index_month
# table_name = quote_fund_month









