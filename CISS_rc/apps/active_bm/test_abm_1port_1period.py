# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function: derived from test_avtive_bm.py and abm_engine.py
last update 181114 | since 181031
Menu :
test_abm_1_ind3_1_period only foucs on one industry and one period


derived from rC
===============================================
'''
##################################################################
### Import requirement modules
import json
import pandas as pd 
import sys
sys.path.append("..")
### Import ABM model engine 

from active_bm.abm_engine import Abm_model

##################################################################
### Initialize common configurations
temp_date = "2014-05-31"
temp_date2 = "2014-11-30"

### Initialize ABM model engine 

### step 1 获取原始研究数据,data_in 经过研究员初步梳理后的结构化信息 
# 7个indicator从属于 核心因子:[行业，流动性，动量，主动收益，价值，成长，
# 资本结构，财务优势，经营能力，人力优势，信息优势] 
# Merge additional data into one pd： create/import database file,update data 
# with imported new info, save to database file.
abm_model = Abm_model()
dataG = abm_model.load_symbol_universe('','' )

##################################################################
### Calculate analyzing indicators for given index conponents and time periods

### get symbol list for 1 index 
# temp_list = abm_model.get_symbol_list( dataG, '000300.SH','2014-05-31')
# get symbol list for all indexes

### get symbol list for all indexes 
# # temp_list = abm_model.get_all_list( dataG, temp_date )

# print('==========================')
# print('temp_list, length ', len(temp_list['code']) )
# print(temp_list.info() ) 
# # get historical fundamental financial and capital data
# # 1st time 
# [df_tb_fi_fi, df_tb_fi_cap] =abm_model.get_histData_finance_capital()
# # load data from sorted values 

# # calculate financial estimates for current year

# [temp_list,cols_new_es] = abm_model.calc_financial_estimates(temp_list,temp_date,df_tb_fi_fi)

# # calculate weights of asset allocation for industry hierachy from level1 to level3 
# [temp_list,cols_new_w_allo] = abm_model.calc_weight_allo_ind_hierachy(temp_list)
# # calculate for anchor stocks in value and growth perspective
# [temp_list,cols_new_anchor] = abm_model.calc_anchor_stocks(temp_list)

# # calculate shadow enterprise value from anchor stocks in value and growth perspectives
# [temp_list,cols_new_shadow] = abm_model.calc_shadow_ev_from_anchor(temp_list) 

##################################################################
### get annalytical dateframe for specific industry     

# ## 1.3，对于ind1~ind每个行业，建立从5-31至11-30的被动跟踪组合，观察6个月内的收益和风险，与等权重组合比较
# ## todo ## Qs:获取当日市值 计算ind3 内的个股市值配置。

#  temp_df["ind3_pct_profit_q4_es"].sum()
int_ind3 = "401010"
# value allocation in industry level 3 
temp_df_value = temp_list[ temp_list["ind3_code"] == int_ind3  ]
temp_df_value["ind3_pct_profit_q4_es"].sum() # 1 
#growth allocation in industry level 3 
temp_df_growth = temp_list[ temp_list["ind3_code"] == int_ind3  ]
temp_df_growth["ind3_pct_profit_q4_es"].sum() # 1 
# equivalent to "para_value" , whcih using 1 for anchor stock, still need to be devided by sum of columns
# get allocation weights for industry level 3 :
print("Working on industry 3 :", int_ind3 )
temp_df_value = temp_list[ temp_list["ind3_code"] == int_ind3  ]
print(temp_df_value["w_allo_value_ind3"].sum() )
print(temp_df_value["w_allo_value_ind3"] ) 
# #growth allocation in industry level 3 
# temp_df_growth = temp_list[ temp_list["ind3_code"] == int_ind3  ]
# print(temp_df_growth["w_allo_growth_ind3"].sum() )
# print(temp_df_growth["w_allo_growth_ind3"] ) 

# # import pandas as pd
# # # 若 apps和abm两级文件夹都要新建，则不用mkdir，用makedirs
temp_path = "D:\\CISS_db\\rc001\\apps\\abm\\"
import os
if not os.path.isdir( temp_path) :
    os.makedirs(temp_path)  
# temp_df_growth.to_csv(temp_path + "temp_df.csv"  )
temp_df_value.to_csv(temp_path + "temp_df_value.csv"  )
##################################################################




temp_df_value = pd.read_csv("D:\\CISS_db\\rc001\\apps\\abm\\temp_df_value.csv")
# temp_df_growth = pd.read_csv("D:\\CISS_db\\rc001\\apps\\abm\\temp_df.csv")
int_ind3 = "401010"
config= {}
sp_name0= 'value_' + str(int_ind3)  
 
##############################################################################

## 20140531后第一个交易日T开始建仓，初始资金{1,5,10,50,100,500}亿元 existing wind data
## need to update portfolio
'''
1,导出组合数据，输出保存至文件
ports.py\\gen_port_suites

2,导入文件数据，用组合管理引擎分析，评估，更新组合状况。
    1，组合管理对象：ports.py\\class admin_portfolios():
    2，组合管理引擎：bin\\engine_portfolio.py

3，参数，配置等：abm模型的参数 config_apps_abm

'''
##############################################################################

### portfolio simulation 
### generate portfolio
from db.ports import gen_portfolios
# sp_df_0 = stockpool_0.sp_df
name_id='port_rc001_value' + int_ind3 
port_name=  name_id
# name_id='sys_rc001' if it is a system with multiple portfolios 
# name_id='tree_rc001' if it is a tree structure with multiple systems 
portfolio_0 = gen_portfolios({},port_name )
print( portfolio_0.port_head['portfolio_name'] )

## generate suites with AS,Asum,trades,signal
from config.config_apps_abm import config_apps_abm

init_cash = 100000000.0
date_start = "20140531" 
date_end ="20141130"
# generate: stockpool_0,account_0,trades_0, signals_0
config_apps = config_apps_abm(init_cash,date_start,date_end,name_id)
port_name = name_id
portfolio_suites = portfolio_0.gen_port_suites(config_apps,temp_df_growth,sp_name0,port_name)

print('portfolio_suites has been generated.')
## we must have portfolio ID to get information. 
# id_port_1541729640_rc001
port_id = portfolio_0.port_head["portfolio_id"]   # 1541729640
port_name=  portfolio_0.port_head["portfolio_name"] #'port_rc001' 
# file_port_head = 'id_port_' + port_id +'_'+ port_name
# id_port_1541729640_rc001
from db.ports import manage_portfolios
# load config values
from config.config_IO import config_IO
path_base= "D:\\CISS_db\\" 

config= config_IO('').load_config_IO_port(port_id,path_base,port_name) 
portfolio_1 = manage_portfolios(config,port_name )
path_base ='' # default "D:\CISS_db\\"

## load portfolio information
# 导入port数据，确定需要计算的时间周期; port_head记录，导入AS,Asum,sp,trade，signal,signal_nextday等数据
(port_head,port_df,config_IO_0 )= portfolio_1.load_portfolio(port_id,path_base,port_name )

print("port_head")
print( port_head ) 

##############################################################################
## run strategy module 
# 选择器：1，导入实际成交文件；2，运行策略模块，按照策略输出策略分析，信号，指令对象。
# the strategy is simple:
# 1, for current date, judge if change of symbol universe 
# 2,if yes, run ideal weights allocation and generate ana,signal,tradeplan 
##############################################################################
stockpool_df = portfolio_suites.stockpool.sp_df
print("Info of stockpool:")
print( stockpool_df.info() )

## stra step 1 get/load data 
## get data by download from Wind-API

# from db.data_io import data_wind
# # ## 下载stockpool里所有股票day,or week数据，
# for temp_code in stockpool_df['code'] :
#     symbols = temp_code #  '600036.SH' 
#     # multi-codes with multi-indicators is not supported 
#     wd1 = data_wind('' ,'' ).data_wind_wsd(symbols,date_start,date_end,'day')
#     print('head  ')
#     print(wd1.wind_head )
 
#     # print(wd1.wind_df )
#     # output wind object to json and csv file 
#     file_json = wd1.wind_head['id']  +'.json'
#     with open( config_IO_0['path_data']+ file_json ,'w') as f:
#         json.dump( wd1.wind_head  ,f) 
#     file_csv =  wd1.wind_head['id'] +'.csv'
#     wd1.wind_df.to_csv(config_IO_0['path_data']+file_csv )

##############################################################################


## load data by import from 'data' directory 
from db.data_io import data_wind
path0 = 'D:\\db_wind\\'
data_wind_0 = data_wind('' ,path0 )
quote_type='CN_day'
# print( stockpool_df.info() )

#####################################################################
### todo, VIP: 策略开发模块： create strategy_suites{data-in, indicator,strategy or function, algorithm or optimization, signals or estimation }

# # get analytical module and strategy modules for single stock
# from db.analysis_indicators import indicator_momentum  
# for temp_code in stockpool_df['code'] :
#     code = temp_code    
#     # todo：需要下载前推至少100天的市场数据！~ 
#     (code_head,code_df)=data_wind_0.load_quotes(config_IO_0,code,date_start,date_end,quote_type)
#     print("666=============")
#     print( code_df.info() )
#     # indicators： get best(MA) type indicator 
    
#     # get analytical moudle
    
#     indi_mom = indicator_momentum('')
#     code_df_ana = indi_mom.indi_mom_ma_all(code_head,code_df)
#     print('code_df_ana')
#     print( code_df_ana.info() )
#     # Output analyzing DataFrame

#     # get strategy module for single sotck 
#     # strategy group：get best strategy 


## run strategy for weights 
# 策略是粗线条的，只说我们要买入多少比例的股票x
from db.func_stra import stra_allocation
stra_weight_list = stra_allocation('').stock_weights(stockpool_df)
print('weight_list')
print(stra_weight_list)
# todo:1,建立 value 组合，growth组合，混合组合{weight_port=[0.5,0.5]}
# 混合组合随着时间变动，根据业绩调整权重
stra_estimates_group = {}
stra_estimates_group['key_1'] = stra_weight_list

from db.algo_opt import optimizer
optimizer_weight_list = optimizer('').optimizer_weight(stra_estimates_group )
## todo todo 3 methods
## 1, w_allo, only value(current choice )
## 2, w_allo, only growth
## 3, w_allo, only half value and half growth


## get signals by strategy estimations 
# 交易信号是精细化的，对应了目标的数量，金额，持仓百分比等要素。
from db.signals import signals
portfolio_suites = signals('sig_stra_weight').update_signals_stock_weight(optimizer_weight_list,portfolio_suites)
signals_list = portfolio_suites.signals.signals_df 
# signals_df['signal_pure'] = 1 
# todo update signals_01 in port_suites 

## get trade plan 
## 包括了何时买，哪个市场买，导入前收盘价计算买入价格区间等
# load trade head file 
from db.trades import manage_trades
manager_trades = manage_trades('')

(trades,quote_df) = manager_trades.manage_tradeplan(portfolio_suites, signals_list, config_IO_0 ,date_start,date_end,quote_type,data_wind_0 )

print('trade_plan')
print( trades.tradeplan )

## get trade details 
trades= manager_trades.manage_tradebook(trades ,quote_df, config_IO_0 ,date_start,date_end,quote_type,data_wind_0 )
print( 'trades.tradebook' )
print( trades.tradebook )
## update trades in portfolio_suites
portfolio_suites.trades = trades


## update accounts using trade result
## todo,we only update trades that have not been used by accounts
from db.accounts import manage_accounts
##  get trading days using account_sum and date_start,date_end 
date_list = portfolio_suites.account.account_sum.index
print('date_list')
# print(date_list[date_list<date_end ] )
#  2014-06-03 to 2014-11-28
date_list_units = date_list[date_list<date_end ]
trades_0 = portfolio_suites.trades
tradebook = trades_0.tradebook
# get all trading dates from tradebook
tradebook['datetime'] = pd.to_datetime(tradebook['date'], format='%Y-%m-%d' ) 
tradebook =tradebook.sort_values('datetime')
date_list_trades = list( tradebook['datetime'].drop_duplicates() )

for temp_date in date_list_units  :
    if_trade =0
    if temp_date in date_list_trades :
        # date with trading 
        portfolio_suites = manage_accounts('').update_accounts_with_trades(temp_date,portfolio_suites,config_IO_0,date_start,date_end,quote_type,data_wind_0)
        if_trade =1 

    # update closing price for all holding stocks, whether date with no trading or not, 
    portfolio_suites = manage_accounts('').update_accounts_with_quotes(temp_date,portfolio_suites,config_IO_0,date_start,date_end,quote_type,data_wind_0,if_trade )
    ## we should update statistics results of portfolio_head file before output 
    # todo todo 

    ## for every trading day, Out portfolio_suites to files
    portfolio_suites = portfolio_1.output_port_suites(temp_date,portfolio_suites,config_IO_0,port_head,port_df)


asd
## todo, update sys for all hierachical portfolios
## {growth,mixed,value}*{61 ind3} portfolios








### VIP 抓重点，课题模块之后再弄！！！
# 我们现在的策略很简单，就是根据sp_df中给的权重，从 2014-05-31开始后的第一个
# 交易日开始建仓，对应组合应该是从 strategy开始，输出策略signals
#1,周日要完成int_ind3=401010 的过去4年历史模拟回撤完整例子，并且至少要开始计算主动基准的
# 收益分布情况，


##############################################################################

##############################################################################

'''
port组合分析应该包括 实盘监控，盈亏分析汇总，阶段性汇报统计，仓位变动，持仓股票分析等数据。
相关记录写在port_head中：
    steps：|参考：rC_Portfolio_17Q1.py
    1，导入port数据，确定需要计算的时间周期。
    2，port_head记录，导入AS,Asum,sp,trade，signal,signal_nextday等数据    
    3，导入(交易)参数? line 3419    
    4,日初更新AS，Asum，...
    5,导入真实数据，Tran_Live2Standard，得到交易模块 ||
    6，获取指数，宏观择时模块 line 3485，
    7，line3524，获取价格和回报率数据，更新Account_Sum, Account_Stocks, StockPool
    8，更新持仓股在stockpool或者port_df里的数据变量。
    9,非持仓股的分析，信号和交易    
    9，预判明日数据ana,signals
    10，port_head 中更新log文件

2, 20140531后第一个交易日T开始建仓，初始资金{1,5,10,50,100,500}亿元 existing wind data 
3，T日生成交易计划，包括T~T+N日之间的逐日盯市交易(T日前无法预知当日和次日交易量？盘中可能需要更新指令)；
    交易策略是不超过市场日均/周成交量的20%，对于不同股票，给定不同的交易成本 0.2%，0.5%，0.8% shock
4，更新AS,Asum,TB等等
5，下一个交易日。
6，抓取相关指数和行业指数，主要基金，比较超额收益情况。还有2016年以来，是否龙头股的超额收益增加。

### Core signal generating process: func_stra --> algo_opt --> signals 
### todo 181102 目标： 达到净值图的输出水平，领导视角{赚多少钱，亏多少钱，怎么赚的，需要多少人，软件和资源}。
'''












 

# 我们思考了个性化策略分析过程和标准化的组合，账户，交易等净值。

#todo 1106 09:55 || 抓紧时间，建立ana,signal 模块
# todo 所有都要输出到系统文件！！！！ AS,A_sum,TB,Signal,Ana
# 增加统一的输出模块，这样可以避免脚本内到处是个性化的地址。
# 还是需要配置统一的文件地址，放在config里
# logic:1,apps个性化的模块，2，
class config_db_abm() :
    def __init__(self,name_db_abm=''):
        self.name_db_abm = name_db_abm































#####################################################################
# ### get data-api to get local data or wind data
# # rC_Portfolio_17Q1\ line 325
# # data.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
# # 导入指数和个股的周数据 | 下载指数和个股的周数据
# date_start = temp_date.replace("-","")   # '20181010' temp_date = "2014-05-31" temp_date2 = "2014-11-30"
# date_end = temp_date2.replace("-","")
# from db.data_io import data_wind
# db_name_win=''
# path0='D:\\db_wind\\'

# ## 下载stockpool里所有股票day,or week数据，
# for temp_code in sp_df_0['code'] :
#     symbols = temp_code #  '600036.SH' 
#     # multi-codes with multi-indicators is not supported 
#     wd1 = data_wind(db_name_win ,path0 ).data_wind_wsd(symbols,date_start,date_end,'day')
#     print('head  ')
#     print(wd1.wind_head )
 
#     # print(wd1.wind_df )
#     # output wind object to json and csv file 
#     file_json = wd1.wind_head['id']  +'.json'
#     with open( config_IO_0['path_data']+ file_json ,'w') as f:
#         json.dump( wd1.wind_head  ,f) 
#     file_csv =  wd1.wind_head['id'] +'.csv'
#     wd1.wind_df.to_csv(config_IO_0['path_data']+file_csv )
#####################################################################

'''
todo 
1,developing indicators
1.1, expected P/E in future 2 years | need (total_shares*price)/e_expected

2,历史每日市场数据应用，特别是关注wind的复权因子数据。
Wind_Input\Wind_all_A_Stocks_wind_170814_updated 


# Alpha因子和风险/系统/Beta因子评定：1，数值稳定且与其他因子相关性低
#2，因子价格变动部分的收益或风险角度价值，
#3，因子收益率显著性，每期个股收益率和因子暴露值回归，看平均值是否显著，显著月份占比
#4，看新增因子是否增加了信息价值。(从短期效应的角度，也许某一年某因子实现了价值，
# 在下一年度投资者仍然会使用该因子，直到投资结果持续不佳或者精确计量该因子效应的数据出现。)
# 数据区间：zxjt 用的数据时 2008-2018
# 难点1：而非线性规模因子则主要强调的是市值中等的股票，计算方法为规模因子的立方,然后和规模因子进行施密特正
#   交化处理去掉其共线性的部分，但非线性规模因子由于构造复杂超额收益一般较难获得。 
# 难点2：Growth 成长因子缺失值较多，多空收益计算误差较大，因此在这里没有加入对比
# 变化：值得注意的是规模因子，该因子在 2017 年之前一直被普遍认定为 Alpha 因子，A 股市场的小盘股溢价效应非常明显，但最近两
# 年大盘股的重新崛起和风格切换使得该风格因子的波动率急剧提升（年化波动率已经上升到 5%），风险属性逐
# 步增强，因此规模因子作为风险因子已经被大多数投资者所认可。
# source 20180830_中信建投_金融工程专题_丁鲁明_Barra风险模型介绍及与中信建投选股体系的比较
# Ana：传统行业因子按照市值加权中性化，我们考虑按照过去40-100天总成交金额中性化
#   逻辑是交易的角度，市场深度可能比流通市值更能反映一段时间内市场交易对手的交易意愿，
#   当然从数据处理的角度，也更容易获得每日成交金额；流动市值中很容易存在显著比例仓位不会出现交易的情况。



5，基准构建策略中的权重策略，zxjt用了行业内市值加权或等权，指数内行业市值加权/等权，衍生方面还对5挡股票的后20%做空
 
# idea 所有现有的模型方法都可以作为一个模块，在其之上进行加强。
idea db的角度，能不能建立一个开源的基本面数据库，有统一的数据标准。
'''
### step2 因子标准化（Z-Score)处理 











































