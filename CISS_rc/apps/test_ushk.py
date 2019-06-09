# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：导入美股季度财务数据，
last update 181226 | since 181226
Menu : 

todo:


Notes:
1,不同股票在同一年的季末日期可能不一样
>>> df.index =[0,1,2,....  72 ]
>>> df.columns
Index(['Unnamed: 0', '2018-09-29', '2018-06-30', '2018-03-31', '2017-12-30',
       '2017-09-30', '2017-07-01', '2017-04-01', '2016-12-31', '2016-09-24',
       '2016-06-25', '2016-03-26', '2015-12-26', '2015-09-26', '2015-06-27',
       '2015-03-28', '2014-12-27', '2014-09-27', '2014-06-28', '2014-03-29',
       '2013-12-28', '2013-09-28', '2013-06-29', '2013-03-30', '2012-12-29',
       '2012-09-29', '2012-06-30', '2012-03-31', '2011-12-31', '2011-09-24',
       '2011-06-25', '2011-03-26', '2010-12-25', '2010-09-25', '2010-06-26',
       '2010-03-27', '2009-12-26', '2009-09-26', '2009-06-27', '2009-03-28',
       '2008-12-27'],

===============================================
'''
import json
import pandas as pd 
import sys
sys.path.append("..") 
import datetime as dt 
# file= "18q3_13q1_AAPL.O.csv"
# df= pd.read_csv(path+file ,encoding="gbk")
from db.times import times
times0 = times('US','nasdaq')
method4time='stock_index_sp'


path = "D:\\db_wind\\financialdata_summary\\"
file_name = "symbol_list_hk.txt" #  "symbol_list.txt" # 
df_symbol = pd.read_csv(path+ file_name)

'''
financial indicator 
index     name_CN    name_EN_raw 
11    净利润："　　Net Profit"
33    经营性现金流:"　　Cash Flow from Operating Activities"
 4    收入"　　Total Operating Revenue"

Period: 三季报 中报  一季报 年报
Unit : CNY 100M , CNY  

'''
###########################################################################
###########################################################################
### 1, anchor stocks in US market 
### 2, anchor Chinese stocks in US and HK market
### 3, anchor stocks in europe market 

###########################################################################
### Given quarter, import profit list, calculate weight,save to csv 
# Qs:股票如 AVGO.O不是按季末最后一个月披露财务数据的，[2013/11/3  2013/8/4 2013/5/5 2013/2/3]
# Ans: 增加Datetime 列，todo
#todo， notes,由于DWDP.N 2015年由DOWS.N，DUPONT.N合并，因此，2015年之前的财务数据需要人工计算补充。
###########################################################################
### step 1 13q1-18q2,计算每半年的当年净利润预测 
temp_date = "2014-05-31" # "2014-05-31" 
temp_date2 = "2018-11-15" # "2014-11-30" 

temp_date_now = times0.get_time_format('%Y%m%d','str')

# get date list with all pivot time point that we need to update stockpool,strategy and portfolio
# DatetimeIndex(['2014-05-31', '2014-11-30', '2015-05-31' ......
date_periods = times0.get_port_rebalance_dates(temp_date,temp_date2 ,method4time )
print("date_periods","   periods_reference_change ")
# [Timestamp('2014-05-31 00:00:00'), Timestamp('2014-11-30 00:00:00')]
# print(date_periods.periods_reference_change )

from db.db_assets.stock_us_hk import stock_foreign
stock_us = stock_foreign( 'CN','nasdaq')
stock_us = stock_foreign( 'CN','hke')
###########################################################################
### step 1 13q1-18q2,计算每半年的当年净利润预测
df_profit_q4_es = stock_us.get_profit_q4_es( path,date_periods,file_name  ) 
# ### step 2 每半年计算市场价值组合的配置计划
df_w_allo= stock_us.get_weight_allocation( df_profit_q4_es)
# df_w_allo = pd.read_csv("D:\\df_w_allo_hk.csv")
# print(df_w_allo.columns.info()  )
# asd
# df_w_allo.index = df_w_allo["Unnamed: 0"]
# df_w_allo = df_w_allo.drop(["Unnamed: 0"],axis=1 )
# print( df_w_allo.head(5) ) 
# asd

# 下载历史行情数据。注意wind非A股票存在很多缺失值，使用后1个交易日的成交量加权平均值

# result = stock_us.get_quotes( path,file_name ) 
### step 3 快速计算组合持仓和净值 || 5日平均建仓方式计算成本价，交易数量，得到账户资金变动   
# todo || df_unit_mdd = stock_us.get_portfolio_unit_quick_1(date_periods,path  ) 

##############################################################################
### parameter initialization
import time
start = time.clock()
import datetime as dt 
from bin.engine_portfolio_us import Engine_ports_us
from db.times import times
times0 = times('CN','SSE')
method4time='stock_index_csi'
init_cash = 300000000.0 
engine_ports_us = Engine_ports_us()
#######################################################
### Working on the whole market portfolio | Method 1 
# 注意：由于是把11个行业等权重分配比例，因此距离理论上的全部股票按净利润比例分配还有差距！！！
# todo：应该按照list里所有股票净利润分配权重，这样的话，估计银行股会占大头，但是似乎也更符合逻辑？
# ind_level='0' means th whole market  
ind_level='0' # '1' '1' '2' '3'
int_ind_x = "999" # means the whole market 
sty_v_g = "value" # "value" growth
name_id= "port_rc181227_hk_market_" + sty_v_g + "_" + int_ind_x
port_name=  name_id
print("We are working at ", port_name )

portfolio_manage,portfolio_suites= engine_ports_us.test_abm_1port_Nperiods_ind_x(df_w_allo,ind_level,sty_v_g,date_periods,int_ind_x,port_name,init_cash)




elapsed = (time.clock() - start)
print("Total time used:",elapsed)





































