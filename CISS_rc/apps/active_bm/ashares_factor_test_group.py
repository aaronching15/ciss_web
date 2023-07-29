# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
todo:
1，对roe，roic等历史财务ttm指标做初步回测

功能
1,市场动量择时，

last  | since 200520

derived from:ashares_timing_abcd3d.py
notes:


分析：
1,csi300,500,1000 更多地是按照流通市值加权，难以反映市场交易额top300,500,1000；
第一步可以观察前后两者有多少比例是重合的
'''


########################################################################
### Initialization 
import os 
# 获取当前目录 os.getcwd() =: G:\zd_zxjtzq\ciss_web
import sys
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\" )
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )
sys.path.append(os.getcwd()[:2] + "\\ciss_web\\CISS_rc\\db\\" )
sys.path.append(os.getcwd()[:2] + "\\ciss_web\\CISS_rc\\db\\db_assets\\" )

import pandas as pd 
import numpy as np 
import math
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

from data_io import data_timing_abcd3d
data_timing_abcd3d_1 = data_timing_abcd3d()
data_timing_abcd3d_1.print_info()

########################################################################
### 1,给定T日，导入市场所有个股，主要指数成分和行业分类 
# notes:至少要前推100个交易日，因此从20060101开始算合适
# output:obj_data["df_mom_eod_prices"] 新增了几列:obj_data["dict"]["col_list_stra"] = ["indi_short","indi_mid", "abcd3d"]


# obj_in={}
# obj_in["dict"] ={}
# obj_in["dict"]["date_start"] = "20060105"
# # obj_in["dict"]["date_end"] = "20200515"

# obj_in["dict"]["date_start"] = input("Type in year start such as 20070903:")
# obj_in["dict"]["date_end"] = input("Type in year end such as 20111231:")
# # input_1[:4] +"1231"

# ### 对于期末交易日的所有个股，往前推16,40,100，250天； 
# obj_data = data_timing_abcd3d_1.import_data_ashare_change_amt_period( obj_in)

 
# asd
# notes:很多1231那一天的数据还没计算

########################################################################
### 给定交易日，进行数据分析
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
    中国A股投资评级汇总,AShareStockRatingConsus{
    1,
    2，
    }
1.6,其他指标：涨停家数、创新高等
    AShareEODDerivativeIndicator{
    涨跌停状态,UP_DOWN_LIMIT_STATUS,1表示涨停;0表示非涨停或跌停;-1表示跌停。
    最高最低价状态,LOWEST_HIGHEST_STATUS,1表示是历史最高收盘价;0表示非历史最高价或最低价;-1表示是历史最低收盘价。    }
notes: 
'''
obj_data={}
obj_data["dict"] ={}
obj_data["dict"]["date_start"] =  "20100104"   # "20060104" 
# obj_data["dict"]["date_start"] = input("Type in year start :from 20060104:")
# obj_data["dict"]["date_end"] = input("Type in year end such as 20111231:")


########################################################################
### 单独计算roe和roic历史收益
'''共6个指标
S_FA_ROE_TTM_pre,FA_ROIC_TTM_pre
S_FA_ROE_TTM_pre_growth,FA_ROIC_TTM_pre_growth
S_FA_ROE_TTM_pre_inv_std3y,FA_ROIC_TTM_pre_inv_std3y
'''
col_list_fi_hist = ["S_FA_ROE_TTM_pre","FA_ROIC_TTM_pre","S_FA_ROE_TTM_pre_growth","FA_ROIC_TTM_pre_growth","S_FA_ROE_TTM_pre_inv_std3y","FA_ROIC_TTM_pre_inv_std3y"]
# notes：务必保证指标都是越大越好

from analysis_indicators import analysis_factor
analysis_factor_1 = analysis_factor()
analysis_factor_1.print_info()
from func_stra import stra_weighting_score
stra_weighting_score_1 = stra_weighting_score()
stra_weighting_score_1.print_info()
from performance_eval import perf_eval_ashare_stra
perf_eval_ashare_stra_1 = perf_eval_ashare_stra()
perf_eval_ashare_stra_1.print_info()

# 导入历史季度日期列表
file_name = "date_list_quarter_06q1_20q1.csv"
df_dates = pd.read_csv( "D:\\db_wind\\data_adj\\" + file_name  )
# type of date_list is numpy.int64
date_list_q = list( df_dates["date"].values )
date_list_q.sort()

file_ret =  "ret_trade_dt_"+ str( date_list_q[0] )+ "_"+str( date_list_q[-1] ) +".csv"
dir_out = input("Set name of output dir..." ) +"\\"

### 半年调整一次：
date_list_q =[]
for year in range(2014,2020) :
    date_list_q =date_list_q +[ int(str(year)+"0531") ]
    date_list_q =date_list_q +[ int(str(year)+"1130") ]
print(date_list_q)


for temp_quarter in date_list_q :
    # 获取下一个季度日期
    # 获取当前季度所处list的index
    temp_i = date_list_q.index(temp_quarter)
    temp_quarter_next = date_list_q[temp_i+1 ]

    print("Quarter ", temp_quarter,temp_quarter_next )
    obj_data["dict"]["date_start"] = temp_quarter
    obj_data["dict"]["date_end"] = temp_quarter_next
    
    ### 导入历史行情和abcd3d数据
    obj_data = data_timing_abcd3d_1.import_data_ashare_change_amt( obj_data)
        
    ### 导入中国A股TTM指标历史数据
    obj_data = data_timing_abcd3d_1.import_data_ashare_fi_hist( obj_data)
    
    ### 取市值前800的股票
    temp_df = obj_data["df_mom_eod_prices"].sort_values(by="S_DQ_AMOUNT")
    obj_data["df_mom_eod_prices"] = temp_df.iloc[:800, :]

    # obj_data["df_mom_eod_prices"]
    ### 对6个指标分别取标准分、
    for temp_col in col_list_fi_hist :
        obj_data["df_factor"]=analysis_factor_1.cal_replace_extreme_value_mad(obj_data["df_mom_eod_prices"], temp_col)
    
    ### 对单因子和全部因子排序，取前100只股票加权
    obj_indi = {}
    obj_indi["df_indi"] = obj_data["df_factor"]
    obj_indi["col_list"] = col_list_fi_hist  

    obj_data["df_factor"] = stra_weighting_score_1.cal_weight_indicator_score(obj_indi)

    ### 计算几个加权组合的区间收益率
    obj_data["col_list"] = col_list_fi_hist
    obj_data = perf_eval_ashare_stra_1.perf_eval_ashare_factors_group(obj_data)
    # obj_data["df_ret"]

    ### save to csv 
    obj_data["df_factor"].to_csv( obj_data["dict"]["path_output"] + "df_factor_trade_dt_"+ str(obj_data["dict"]["date_tradingdate"]) +".csv" ,encoding="gbk"  )
    pd.DataFrame(obj_data["df_factor"].columns).to_csv( obj_data["dict"]["path_output"]  + "columns.csv" ,encoding="gbk"  )
    ### save to csv 
    obj_data["df_ret"][ "unit_port" ] = obj_data["df_ret"][ "ret_port" ]+1
    obj_data["df_ret"][ "unit_port" ] = obj_data["df_ret"][ "unit_port" ].cumprod()
    obj_data["df_ret"][ "unit_bm" ] = obj_data["df_ret"][ "ret_bm" ]+1
    obj_data["df_ret"][ "unit_bm" ] = obj_data["df_ret"][ "unit_bm" ].cumprod()
    for temp_col in col_list_fi_hist :
        obj_data["df_ret"][ "unit_port_"+temp_col ] = obj_data["df_ret"][ "ret_port_"+temp_col ]+1
        obj_data["df_ret"][ "unit_port_"+temp_col ] = obj_data["df_ret"][ "unit_port_"+temp_col ].cumprod()
    
    if not os.path.exists( obj_data["dict"]["path_output"] +dir_out+ file_ret ) :
        # create dir 
        os.makedirs( obj_data["dict"]["path_output"] +dir_out )
    obj_data["df_ret"].to_csv( obj_data["dict"]["path_output"] +dir_out+ file_ret  ,encoding="gbk"  )



# col_weight_list=["S_FA_ROE_TTM_pre_mad_weight","FA_ROIC_TTM_pre_mad_weight","S_FA_ROE_TTM_pre_growth_mad_weight","FA_ROIC_TTM_pre_growth_mad_weight","S_FA_ROE_TTM_pre_inv_std3y_mad_weight","FA_ROIC_TTM_pre_inv_std3y_mad_weight","sum_mad_weight"]
asd






########################################################################

# [0.0, 10.0, 11.0, 12.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 30.0, 
# 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 40.0, 41.0, 42.0, 50.0, 60.0, 61.0, 62.0, 63.0, 70.0]
list_ind_code = obj_data["df_mom_eod_prices"]["ind_code"].drop_duplicates().to_list()
list_ind_code.sort()
list_ind_code_str = [ str(int(x)) for x in list_ind_code  ]

### 导入市值、财务指标ttm 、预期数据
obj_data = data_timing_abcd3d_1.import_data_ashare_mv_fi_esti( obj_data)

asd
########################################################################
### 1,新建df_abcd3d_ana,index是不同的分组例如沪深300、医疗行业等，columns是分析指标
### 1.1，成交金额：amt_1_300 :301_800,801_1800,1801_end

list_index0 = []
for word in ["amt","mvfloat","mvtotal"   ] :
    for para in ["_1day_1_300","_1day_301_800","_1day_801_1800","_1day_1801_end"] :
        word_para= word + para 
        list_index0 = list_index0 + [ word_para ]
print( list_index0 )

# for temp_col in ["S_DQ_AMOUNT","mvfloat","mvtotal"   ] :
# df_abcd3d_ana = pd.DataFrame(index=list_index0,columns= obj_data["df_mom_eod_prices"].columns  )






asd








########################################################################
### 













########################################################################
### 










