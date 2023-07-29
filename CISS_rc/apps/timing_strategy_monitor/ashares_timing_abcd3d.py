# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
todo:
1，市场行业交易额+涨跌模型{ma16+ma40，短期和中期偏离幅度，描述全市场和行业和个股 }；
2，四状态三阶段建模；
3, 历史配置比例设置
4，输出：历史月组合收益率，区间收益率观察(按给定的日期划分)

功能
1,市场动量择时，
### 数据下载和abcd3d指标计算、导入 
### abcd3d全市场个股分组和分行业统计
### 历史回测组合：每个月按照abcd3d得分，选择前5、后5的行业构建行业轮动组合

last 200523 | since 200517
------------
计算的指标来源：sheet=常用因子列表,file=abm_factors_manage.xlsx
------------
notes:
分析：
1,csi300,500,1000 更多地是按照流通市值加权，难以反映市场交易额top300,500,1000；
第一步可以观察前后两者有多少比例是重合的
'''
########################################################################
### Initialization 
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "\\db\\" )
sys.path.append(path_ciss_rc + "\\db\\db_assets\\" )
sys.path.append(path_ciss_rc + "\\db\\data_io\\" )
path_output = "D:\\CISS_db\\timing_abcd3d\\market_status_group\\"

import pandas as pd   
import numpy as np 
import math
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

from analysis_indicators import analysis_factor
analysis_factor_1 = analysis_factor()

from data_io import data_io
data_io_1 = data_io()
from data_io_pricevol_financial import data_pricevol_financial
data_pricevol_financial_1 = data_pricevol_financial()
data_pricevol_financial_1.print_info()

year="2020"
mmdd_start = input("Type in mmdd start such as yesterday 0528:")
mmdd_end = input("Type in mmdd end such as today 0529:") 
### for stock abcd3d
obj_in={}
obj_in["dict"] ={}
# 如果只计算T日，则输入T-1,T ;如果计算T~N日，则输入T-1,N  ;"20060105"， "20200515"
obj_in["dict"]["date_start"] = year + mmdd_start
obj_in["dict"]["date_end"] = year + mmdd_end
### for marekt abcd3d
obj_date={}
# 如果只计算T日，则输入T-1,T ;如果计算T~N日，则输入T-1,N 
obj_date["date"]=   year +  mmdd_start
obj_date["date_end"]= year+  mmdd_end

########################################################################
########################################################################
### Step 1，数据下载和abcd3d指标计算、导入|| 单个交易日:ADJ_timing_TRADE_DT_20201013_ALL.csv
# notes:至少要前推100个交易日，因此从20060101开始算合适
# output:obj_data["df_mom_eod_prices"] 新增了几列:obj_data["dict"]["col_list_stra"] = ["indi_short","indi_mid", "abcd3d"]
### 对于期末交易日的所有个股，往前推16,40,100，250天； 

obj_data = data_pricevol_financial_1.import_data_ashare_change_amt_period( obj_in)
print("Previous dates: ", obj_in["dict"]["date_start"] , obj_in["dict"]["date_end"]    )

asd
# "close_pct"：收盘价所处过去40天百分比 | 增加区间收盘价所处价格百分比： "close_pct_s_"+str(x) 


########################################################################
### Step 2，市场数据分析:abcd3d全市场个股分组和分行业统计
'''
1,新建df_abcd3d_ana,index是不同的分组例如沪深300、医疗行业等，columns是分析指标
1.1，成交金额：amt_1_300 :301_800,801_1800,1801_end
1.2，流通市值：mvfloat_1_300:流通市值前300、500、1000；
    AShareEODDerivativeIndicator{当日流通市值,S_DQ_MV;当日总市值,S_VAL_MV};
1.2.1，mvfloat_1_300等内分行业选股
1.2.2，mvfloat_1_300等内成长指标选股
1.3，行业：ind_citics_1_20 :中信一级行业
1.4, 常用指标：pe,pb，pcf, dps;
    AShareEODDerivativeIndicator{S_VAL_PE_TTM,市盈率(PE,TTM){若净利润<=0,则返回空},
    市净率(PB),S_VAL_PB_NEW;
    市现率(PCF,经营现金流TTM)S_VAL_PCF_OCFTTM;股价/每股派息,S_PRICE_DIV_DPS }
1.5,滚动预期类指标：
    Wind一致预测个股滚动指标，AShareConsensusRollingData{
    1，NET_PROFIT
    2，市盈率,EST_PE，FY0,FY1,FTTM,YOY,YOY2
    3，PEG,EST_PEG
    4,市净率,EST_PB
    5,每股现金流,EST_CFPS
    6,利润总额,EST_TOTAL_PROFIT;营业利润,EST_OPER_PROFIT;基准年度,BENCHMARK_YR   }
    中国A股投资评级汇总,AShareStockRatingConsus{    1,    2，    }
1.6,其他指标：涨停家数、创新高等
    AShareEODDerivativeIndicator{
    涨跌停状态,UP_DOWN_LIMIT_STATUS,1表示涨停;0表示非涨停或跌停;-1表示跌停。
    最高最低价状态,LOWEST_HIGHEST_STATUS,1表示是历史最高收盘价;0表示非历史最高价或最低价;-1表示是历史最低收盘价。    }
notes:  
''' 

obj_date = data_io_1.get_trading_days(obj_date) 
# date_list_post  = obj_date["date_list_post"]
date_list = obj_date["date_list_period"]
print( "date_list  " ,date_list  )

obj_ana = {}
obj_ana["dict"] = {}
obj_ana["date_list"] =date_list

obj_ana = analysis_factor_1.market_status_abcd3d_ana( obj_ana) 

print("Path out=",obj_ana["path_output"])
print("list_group_name ",obj_ana["list_group_name"]  )

asd


########################################################################
### 数据下载和abcd3d指标计算、导入|| 历史区间
# obj_date={}
# obj_date["date"]= "20060101" 
# obj_date = data_io_1.get_trading_days(obj_date)
# # obj_date["date_list"] 是从19901220开始
# date_list= obj_date["date_list_post"] 

# obj_in={}
# obj_in["dict"] ={}
# count=0 
# for temp_i in range(0,len(date_list)-1 ):
#     obj_in["dict"]["date_start"] =date_list[temp_i]
#     obj_in["dict"]["date_end"] = date_list[temp_i+1 ]
#     try :
#         obj_data = data_pricevol_financial_1.import_data_ashare_change_amt_period( obj_in)
#     except :
#         pass

# asd

########################################################################
########################################################################
### 历史回测组合：每个月按照abcd3d得分，选择前5、后5的行业构建行业轮动组合；
'''
组合测试目的和计划：
    1，观察策略用于全市场择时的有效性：全部A股、创业板、科创板
    2，观测用于不同行业、风格的有效性
    3，观察不同调仓频率的组合有效性：季度、月、周；以及不同的偏移交易日
    4，年换手率限制
组合回测参数：
1，回测周期、交易、组合参数：config_port.py
    1.1，回测周期
    1.2，建仓：初始资金、
    1.3，调仓频率、调仓频率得偏移日期、
2，股票池：中证800；全市场组合：大小盘轮动组合{大、中、小、微小}；stockpools.py
3，指标和因子选股，abcd3d：analysis_indicators.py
    3.1，单指标：abcd3d_ave_mvfloat, abcd3d_ave_num,abcd3d_pct_down,abcd3d_pct_up
    3.2，单指标参数：数值越大越好或越小越好、极值处理 ；
    3.3，筛选条件-选股：多指标得处理逻辑{and、or}；前X%股票、分行业前X%股票；top,bottom,rank,top_pct,bottom_pct,rank_pct。
4，股票权重计算：algo_opt.py
    4.1, 选择指标、设置因子权重，进行打分排序；
    4.2，筛选条件-加权：前X%股票、分行业前X%股票；top,bottom,rank,top_pct,bottom_pct,rank_pct
    4.3，加权方式：流通市值、指标或因子得分加权{成长g、价值pe、peg、净利润等}
5，组合管理：交易、持仓等：ports.py
6，perf_eval绩效分析：{累计收益、年化收益、逐年收益、年化波动率、最大回撤、最大年还手率、平均年还手、
    Alpha、Beta、Sharpe、IR=基准收益}
    信息比率 =（策略每日收益-基准每日收益）的年化均值/年化标准差
    Sharpe=(Rp− Rf)/σp ,其中Rp为策略年化收益率，Rf为一年定存利率，σp为策略年化波动率

测试策略的有效性：T日，根据策略打分对行业进行配置，分数越高配越多
定期：20天调整一次
临时：若理论配置比例变动太大，则临时调整。

'''
########################################################################
### 组合配置config_port_simu_hist
### todo，定义要回测的股票范围：
obj_port = {}
obj_port["dict"] = {}
obj_port["dict"]["port_name"] = "rc01_s6"
# 加权方式：mvfloat=市值加权,ew=等权重,growth = 成长加权,value=价值加权
obj_port["dict"]["weighting_type"] = input("加权方式：mvfloat=市值加权,ew=等权重,growth = 成长加权,value=价值加权 ")
obj_port["dict"]["port_id"] = "200524"

obj_port["dict"]["len_rebalance"] = 20
obj_port["dict"]["stra_name"] = "abcd3d"
# 定义股票池限定的范围
obj_port["dict"]["sp_column"] = ""
### 设定组合类型，例如成长内行业轮动、市场内大小流通市值轮动、不同行业成长和价值轮动
# "market","value_growth","industry","mixed"=所有分组都考虑
obj_port["dict"]["group_type"] = "market"

# 
path_output = "D:\\CISS_db\\timing_abcd3d\\market_status_group\\"


########################################################################
### 导入月末日期列表,设定偏离10个交易日（为了未来5个交易日平均价格取5~10个交易日）；
from data_io import data_io,data_factor_model
data_io_1= data_io()
obj_stock_des =data_io_1.get_stock_des_name_listday( {} )
# obj_stock_des["df_stock_des"],obj_stock_des["col_list_stock_des"]
# 17,300830.SZ,N金现代,创业板,20200506.0,NaN
# print( obj_stock_des["df_stock_des"].head() )
obj_port["df_stock_des"] = obj_stock_des["df_stock_des"]
obj_port["col_list_stock_des"] = obj_stock_des["col_list_stock_des"]

data_factor_model_1 = data_factor_model()
from algo_opt import optimizer_ashare_factor
optimizer_ashare_factor_1 = optimizer_ashare_factor()
from performance_eval import perf_eval_ashare_stra
perf_eval_ashare_stra_1 = perf_eval_ashare_stra()

obj_date={}
# # input("Type in date end s.t. 20190101 , 20060101" ，"20200521" # 
obj_date["date"]=  "20060104"
obj_date["date_end"]= "20200522"
obj_date = data_io_1.get_trading_days(obj_date) 
date_list = obj_date["date_list_period"]

print( "date_list  " ,date_list[0],date_list[-1] ,len(date_list)  )

########################################################################
### 每20个交易日调整一次持仓
lag_port_rebalancing = 20 
# [60,40,20]
for lag_port_rebalancing in [ 20] :
    obj_port["dict"]["len_rebalance"] = lag_port_rebalancing

    ### 根据间隔生成跟新日期列
    date_list_sub = []
    for temp_i in range(0, len(date_list) ) :
        if temp_i % obj_port["dict"]["len_rebalance"] == 0 :
            date_list_sub =date_list_sub +[ date_list[temp_i] ]

    if not date_list_sub[-1] == date_list[-1] :
        date_list_sub=date_list_sub+ [ date_list[-1] ]
    
    ### Loop  
    count_df_ret = 0 
    for temp_date in date_list_sub :
        temp_i = date_list_sub.index(temp_date)

        temp_date_next = date_list_sub[temp_i+ 1 ]
        print( temp_i,temp_date,temp_date_next )
        ########################################################################
        ### 对于每个交易日，计算最优配置权重，并于下一个交易日实施。
        # 1~3的步骤已经计算并保存了，只需要导入T日市场和个股的指标数据
        obj_port["dict"]["date"] =  temp_date # "20200522" #
        
        ### 导入当日个股和市场分组指标和因子数据
        obj_port = data_factor_model_1.import_data_factor(obj_port)

        ### 用指标和因子数据计算组合最优权重
        obj_port = optimizer_ashare_factor_1.opt_port_weights_factor(obj_port)

        ### 保存到csv
        obj_port = data_factor_model_1.export_data_factor(obj_port)

        ########################################################################
        ### 3，perf_eval绩效分析：{累计收益、年化收益、逐年收益、年化波动率、最大回撤、最大年还手率、平均年还手、
        # Alpha、Beta、Sharpe、IR=基准收益}
        obj_data={}
        obj_data["dict"] ={}
        obj_data["dict"]["date_start"] =  temp_date
        obj_data["dict"]["date_end"] = temp_date_next
        obj_data["df_factor"] =  obj_port["df_port_weight"]
        obj_data["dict"]["stra_name"] =obj_port["dict"]["stra_name"]
        obj_data = perf_eval_ashare_stra_1.perf_eval_ashare_factors_group(obj_data)
        
        if count_df_ret == 0  :
            df_ret =  obj_data["df_ret"]
            count_df_ret = 1 
        else:
            df_ret =  df_ret.append(obj_data["df_ret"], ignore_index=True)
        
        print( obj_data["df_ret"].T )
        df_ret.to_csv(obj_port["dict"]["path_export"]+"df_ret.csv" )

asd



