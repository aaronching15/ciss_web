# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
last | since 204030
功能：
1,用单因子分组打分的方式观察因子分组收益

例子：
1，对于流通市值的因子权重


### 导入因子权重矩阵 factor_weight_np,df_factor_weight_20060228_000300.SH.csv

常用因子列表：由factor_weight_np和len_factor决定
Factor list: [
'zscore_S_DQ_MV', 流通市值标准分
'f_w_ic_ir_S_VAL_PCF_OCFTTM', 经营性现金流
'f_w_ic_ir_amt_ave_1m_6m', 过去1个月和6个月的平均成交金额比
'f_w_ic_ir_close_pct_52w', 收盘价在52周价格区间百分比
'f_w_ic_ir_ep_ttm', 市盈率倒数
'f_w_ic_ir_ma_20d_120d', 20天和120天均线比值
'f_w_ic_ir_ret_accumu_20d', 20天累计收益率
'f_w_ic_ir_ret_accumu_20d_120d', 20天和120天累计收益率比值
'f_w_ic_ir_ret_alpha_ind_citic_1_120d', 相对于中信一级行业的120天相对收益
'f_w_ic_ir_ret_alpha_ind_citic_1_20d', 相对于中信一级行业的20天相对收益
'f_w_ic_ir_ret_averet_ave_20d_120d', 20天和120天平均收益率比值
'f_w_ic_ir_ret_mdd_20d', 20天最大回撤
'f_w_ic_ir_ret_mdd_20d_120d', 20天和120天最大回撤比值
'f_w_ic_ir_roe_ttm', 净资产收益率
'f_w_ic_ir_turnover_ave_1m_6m', 20天和120天换手率比值
'f_w_ic_ir_volatility_std_1m_6m'，20天和120天波动率比值
]

'''

#################################################################################
### Initialization 
import os 
# 获取当前目录 os.getcwd() =: G:\zd_zxjtzq\ciss_web
import sys
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web + "CISS_rc\\"
sys.path.append( path_ciss_rc + "db\\" )
sys.path.append( path_ciss_rc + "db\\db_assets\\" )

import pandas as pd 
import numpy as np 
import math
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

### 
from analysis_indicators import indicator_ashares,analysis_factor
indicator_ashares_1 = indicator_ashares()
analysis_factor_1 = analysis_factor()

from transform_wind_wds import transform_wds
transform_wds1 = transform_wds()

########################################
### 单因子分组测试

#########################################################
### 导入模块：数据读取/写入模块等
from data_io import data_factor_model
obj_data_io_1 = data_factor_model()
print( obj_data_io_1.obj_config["dict"]["path_factor_model"] )
# 最优化模块
from algo_opt import optimizer_ashare_factor
optimizer_ashare_factor_1 = optimizer_ashare_factor()
# 组合评估模块
from performance_eval import perf_eval_ashare_stra
perf_eval_ashare_stra_1 = perf_eval_ashare_stra()

#################################################################################
### 重要参数对象 obj_para
'''
1,'f_w_ic_ir_close_pct_52w', 收盘价在52周价格区间百分比
2,'zscore_S_DQ_MV', 流通市值标准分
'f_w_ic_ir_S_VAL_PCF_OCFTTM', 经营性现金流
'f_w_ic_ir_amt_ave_1m_6m', 过去1个月和6个月的平均成交金额比
'f_w_ic_ir_ep_ttm', 市盈率倒数
'f_w_ic_ir_ma_20d_120d', 20天和120天均线比值
'f_w_ic_ir_ret_accumu_20d', 20天累计收益率
'f_w_ic_ir_ret_accumu_20d_120d', 20天和120天累计收益率比值
'f_w_ic_ir_ret_alpha_ind_citic_1_120d', 相对于中信一级行业的120天相对收益
'f_w_ic_ir_ret_alpha_ind_citic_1_20d', 相对于中信一级行业的20天相对收益
'f_w_ic_ir_ret_averet_ave_20d_120d', 20天和120天平均收益率比值
'f_w_ic_ir_ret_mdd_20d', 20天最大回撤
'f_w_ic_ir_ret_mdd_20d_120d', 20天和120天最大回撤比值
'f_w_ic_ir_roe_ttm', 净资产收益率
'f_w_ic_ir_turnover_ave_1m_6m', 20天和120天换手率比值
'f_w_ic_ir_volatility_std_1m_6m'，20天和120天波动率比值
'''
obj_para ={}
obj_para["dict"] ={}
obj_para["dict"]["code_index"] = "000300.SH"

# 设置单因子指标："1factor"和因子指标的分组组合数量"num_port_1factor"
input1=input('''1,'f_w_ic_ir_close_pct_52w', 收盘价在52周价格区间百分比
2,'zscore_S_DQ_MV', 流通市值标准分
'f_w_ic_ir_S_VAL_PCF_OCFTTM', 经营性现金流
'f_w_ic_ir_amt_ave_1m_6m', 过去1个月和6个月的平均成交金额比
'f_w_ic_ir_ep_ttm', 市盈率倒数
'f_w_ic_ir_ma_20d_120d', 20天和120天均线比值
'f_w_ic_ir_ret_accumu_20d', 20天累计收益率
'f_w_ic_ir_ret_accumu_20d_120d', 20天和120天累计收益率比值
'f_w_ic_ir_ret_alpha_ind_citic_1_120d', 相对于中信一级行业的120天相对收益
'f_w_ic_ir_ret_alpha_ind_citic_1_20d', 相对于中信一级行业的20天相对收益
'f_w_ic_ir_ret_averet_ave_20d_120d', 20天和120天平均收益率比值
'f_w_ic_ir_ret_mdd_20d', 20天最大回撤
'f_w_ic_ir_ret_mdd_20d_120d', 20天和120天最大回撤比值
'f_w_ic_ir_roe_ttm', 净资产收益率
'f_w_ic_ir_turnover_ave_1m_6m', 20天和120天换手率比值
'f_w_ic_ir_volatility_std_1m_6m'，20天和120天波动率比值 \n''')

obj_para["dict"]["1factor"] = input1 # 'zscore_S_DQ_MV'
obj_para["dict"]["id_output"] = "id_200430_1f_perf_eval_" +obj_para["dict"]["1factor"]
# 单因子分组的数量，例如5组
obj_para["dict"]["num_port_1factor"] = 5

obj_para["dict"]["date_start"] =20060831  # 20060228
obj_para["dict"]["date_end"] = 20200401
# 策略结果变量：obj_perf_eval
obj_perf_eval = {}

### 导入月份list | from 20050531 to 20200403
date_list_month = obj_data_io_1.obj_data_io["dict"]["date_list_month"]
# date_list_month对应要计算的月份，date_list_month_pre是开始日期和之前的日期
date_list_month_pre = [m for m in date_list_month if m<= obj_para["dict"]["date_start"] ]
date_list_month = [m for m in date_list_month if m> obj_para["dict"]["date_start"] ]
date_list_month = [m for m in date_list_month if m<= obj_para["dict"]["date_end"] ]
print("date_list_month",date_list_month)
obj_para["dict"]["date_list_month"] = date_list_month

temp_1factor = obj_para["dict"]["1factor"] 
num_port_1factor = obj_para["dict"]["num_port_1factor"]

obj_port={}
obj_port["dict"]=obj_para["dict"]

for temp_period_end in  date_list_month :
    
    # 20060228是第一次有factor weight的月份
    obj_para["dict"]["date_last_month"] = temp_period_end

    #################################################################################
    ### 数据导入环节 || 这里默认导入所有因子指标factor
    obj_data_import = obj_data_io_1.import_data_opt(obj_para )

    '''
    obj_out["df_index_consti"] = df_index_consti
    obj_out["w_index_consti"] = w_index_consti
    obj_out["code_list_csi300"] = code_list_csi300
    obj_out["col_list"] = col_list
    obj_out["factor_weight_np"] = factor_weight_np
    obj_out["len_factor"] = len_factor
    obj_out["df_ind_code"] = df_ind_code
    obj_out["ret_stock_change_np"] = ret_stock_change_np
    obj_out["ind_code_list"] = ind_code_list
    obj_out["len_stock"] = len_stock
    obj_out["ind_code_np"] = ind_code_np
    obj_out["w_stock_np"] = w_stock_np
    obj_out["df_4opt"] = df_4opt
    obj_out["col_list_4opt"] = col_list_4opt
    # 单因子回测用
    obj_out["df_factor_weight"] = df_factor_weight
    '''

    ########################################################################
    ### 对因子指标进行分组，分组方法有：全部股票分5组
    '''
    steps:1,将给定因子或指标排序
    2，分成5组，组内按指标得分加权
    3，计算当期每组收益
    4，输出结果600
    
    obj_para["dict"]["1factor"] = 'f_w_ic_ir_close_pct_52w'
    '''    
    df_factor_weight = obj_data_import["df_factor_weight"]
    # 默认是升序排列...
    df_factor_weight = df_factor_weight.sort_values( by= temp_1factor )
    # print("Columns:" ,df_factor_weight.columns )
    # df_factor_weight = df_factor_weight.sort_values( by= temp_1factor,ascending=False )
    ### 计算每档股票数量，向下取整，最后一档股票数量可能较多 
    num_stock = round(len(df_factor_weight.index)/ num_port_1factor)
    for temp_level in range( num_port_1factor ):
        print("Working on date",temp_period_end," factor level=", temp_level  )
        temp_i1 = temp_level*num_stock 
        temp_i2 = (temp_level+1)*num_stock -1
        if temp_level ==  num_port_1factor-1 :
            # last group
            df_factor_weight_sub = df_factor_weight.iloc[ temp_i1:,:]
        else :
            df_factor_weight_sub = df_factor_weight.iloc[ temp_i1:temp_i2,:]

        ### 计算加权权重，特别要小心极端值和负值
        # 计算标准分值
        # notes:这个标准分是专门为多因子组合做的，设计流通市值加权的统一操作：indicator_data_adjust_zscore(df_factor_weight_sub,temp_1factor)
        # df_factor_weight_sub = temp_obj["df_factor"]

        df_des = df_factor_weight_sub.describe()
        # temp_mean = df_des.loc["mean", temp_1factor ]
        temp_std = df_des.loc["std", temp_1factor ]
        temp_median = df_des.loc["50%", temp_1factor ]
        temp_ub = temp_median + 2*temp_std
        temp_lb = temp_median - 2*temp_std
        # print("std,median,ub,lb",temp_std,temp_median,temp_ub,temp_lb)
        ### 替代最大最小值，然后计算标准分
        df_factor_weight_sub[temp_1factor] = df_factor_weight_sub[temp_1factor].apply( lambda x: min( max(x,temp_lb),temp_ub)  )
        df_factor_weight_sub[temp_1factor] = df_factor_weight_sub[temp_1factor].apply( lambda x: (x-temp_lb)/(temp_ub-temp_lb)  )
        ### 计算组合权重
        df_factor_weight_sub["w_1factor"] = df_factor_weight_sub[temp_1factor]*0.95/df_factor_weight_sub[temp_1factor].sum() 

        ### 导入下一期股票价格涨跌幅，计算虚拟组合收益率
        obj_port["date"] = temp_period_end
        obj_port["num_port_1factor"] = obj_para["dict"]["num_port_1factor"] 
        obj_port["level_1f"] = temp_level
        obj_port["col_weight"] = "w_1factor" # df内权重对应的column name
        obj_port["df_weight"] = df_factor_weight_sub
        # 更新最新的需要计算的未来1、3、6月份
        date_list_month = obj_para["dict"]["date_list_month"]
        date_list_month= [m for m in date_list_month if m> temp_period_end ]
        obj_port["dict"]["date_list_month"]=date_list_month
        

        obj_perf_eval = perf_eval_ashare_stra_1.perf_eval_ashare_1factor(obj_perf_eval, obj_port )

        # print("df_perf_eval")

        ########################################################################
        ### show results and saved to files|用data_io脚本实现
        obj_port["dict"] = obj_para["dict"]
        obj_perf_eval = obj_data_io_1.export_data_1factor(obj_perf_eval,obj_port )



asd

