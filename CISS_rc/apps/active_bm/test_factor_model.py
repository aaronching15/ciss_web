# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
目标：
3，构建多因子基础模型：构建沪深300与中证500增强的基准策略。

todo：
1，

优化模型：
Obj function: max sum( ret_k *w_k)
s.t. 1,𝑠_𝑙≤𝑋(𝑤−𝑤𝑏)≤𝑠_ℎ
    2,ℎ_𝑙≤𝐻(𝑤−𝑤𝑏)≤𝐻_ℎ
    3,𝑤_𝑙≤𝑤−𝑤𝑏≤𝑤_ℎ
    4,𝑤≥0
    5,𝑊_𝑙≤𝟏_T*𝑤≤𝑊_ℎ
    6,∑|𝑤_s_total_t − 𝑤_s_total_t_pre |≤ turnover_limit

notes:
1,目标方程：ret_k 是ICIR加权后的复合因子值，w_k是求解得到的最优化因子权重
    根据过去T期的值，在t时点预测t+1时点的股票收益率，也就是在20151030月末数据可得后，计算
    20151131月份的最优权重。
    分析：w：自变量组合在N个股票上的权重；w_b:市场组合在N个股票上的权重
2，因子约束条件：s_l，s_h是因子暴露的上下限；一般只对市值因子设置上下限，如市值中性设置：
    s_l_mv=0 and s_h_mv = 0 ；只限制市值因子，也就是 0<= sum{x_i_k,k=mv} <=0
    改写：s_l+ X*w_b <= X*w <= s_h+ X*w_b , 
    where X= factor_weight_np ,factor_weight_np from df_factor_weight;因子暴露矩阵,N*K matrix
3,行业暴露矩阵，设置组合相对于基准行业权重的上限和下线ℎ_𝑙，𝐻_ℎ，例如行业中性设置：
    ℎ_𝑙=0.0，𝐻_ℎ = 0。0
    改写：h_l+ H*w_b <= H*w <= h_h+ H*w_b 
4,个股相对于基准指数中权重暴露的上下限，例如上下限+2%/-2%；
    w_L = -0.02,w_h=0.02
5,总仓位的上下限，例如最低80%，最高95%；
    w_total_l = 0.8, w_total_h= 0.95 
6,换手率限制：当期权重变动，当期个股权重减上期个股权重的绝对值之和,例如每个季度60%对应每年240%，买入和卖出都算。
    sum( abs(𝑤_s_total_t − 𝑤_s_total_t_pre )) ≤ turnover_limit 
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


#########################################################
### 导入模块：数据读取/写入模块等

from analysis_indicators import indicator_ashares,analysis_factor
indicator_ashares_1 = indicator_ashares()
analysis_factor_1 = analysis_factor()

from transform_wind_wds import transform_wds
transform_wds1 = transform_wds()

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
obj_para ={}
obj_para["dict"] ={}
obj_para["dict"]["code_index"] = "000300.SH"
# 4方案：min_var;均值方差模型;max_ret;最大化前6月收益;short_long,短空长多;max_score ：最大化因子标准分
# obj_para["dict"]["method_opt"] = "short_long"
obj_para["dict"]["method_opt"] = input("min_var,均值方差模型;max_ret,最大化前6月收益;short_long,短空长多;max_score,最大化因子标准分\n Choose opt method... ")
# 限制条件分为紧1，和松0.
obj_para["dict"]["method_cons"] = 0 
obj_para["dict"]["id_type"] = "9f_9cons"
obj_para["dict"]["id_output"] = "id_200513_" + obj_para["dict"]["method_opt"] +"_"+ obj_para["dict"]["id_type"] +"_"+ str(obj_para["dict"]["method_cons"])
obj_para["dict"]["date_start"] = 20060228
obj_para["dict"]["date_end"] =  20200401
# 策略结果变量：obj_perf_eval
obj_perf_eval = {}

### 导入月份list | from 20050531 to 20200403
date_list_month = obj_data_io_1.obj_data_io["dict"]["date_list_month"]
date_list_month = [m for m in date_list_month if m>= obj_para["dict"]["date_start"] ]
date_list_month = [m for m in date_list_month if m<= obj_para["dict"]["date_end"] ]
print("date_list_month",date_list_month)

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
    '''
    
    ########################################################################
    ### 1,factor_weight_np
    '''
    因子列表：由factor_weight_np和len_factor决定
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
    # 尝试缩小因子数量，从16个变成2个，看看是否还会报错,line 245 at data_io.py
    # col_list= ['zscore_S_DQ_MV','f_w_ic_ir_close_pct_52w' ]

    ### 2,ret_stock_change_np | 给定日期，导入指数成分股、权重和下个月的收益率
    ### 3,构建行业暴露矩阵ind_code_np和上下限ind_lb,ind_ub;ind_code_np矩阵取值都是1或0，其中每一行对应一个行业，
    ### 构建个股权重矩阵 w_stock_np，对于N个股票，N*N矩阵，每行仅对角线1个值为1，其余为0

    ########################################################################
    ### 定义限制条件 1~ C个：constraint_c，输出对象obj_cons，上下界，初始权重    
    obj_cons = optimizer_ashare_factor_1.set_cons_bounds_init( obj_data_import )
    ### 限制条件集合在 obj_cons["cons"]

    ### todo,将cons对应的参数设置为可变的变量

    ########################################################################
    ### 定义目标方程,run_opt_min_model 基于scipyoptimize.minimize生成最优化模型
    obj_opt = optimizer_ashare_factor_1.run_opt_min_model( obj_cons )

    ########################################################################
    ### Evaluate w_opt performance |每一期的重要收益指标存入 df_perf_eval,index=日期
    # 评估最优权重的表现，例如在历史和未来1~6月的收益，行业分布，模拟组合净值等
    
    obj_perf_eval = perf_eval_ashare_stra_1.perf_eval_ashare_factor_model(obj_perf_eval ,obj_opt )
    # obj_perf_eval["df_perf_eval"]，obj_perf_eval[ 20060228 ]

    ########################################################################
    ### show results and saved to files|用data_io脚本实现
    obj_opt = obj_data_io_1.export_data_opt(obj_perf_eval,obj_opt )
    #优化模型重要参数存入 obj_opt["dict"]["opt_model_xxxx"] 和 obj_opt["dict"]["result_xxxx"]

    ### Result 
    res = obj_opt["res"]
    print("result",res.success,res.message)
    # False Positive directional derivative for linesearch
    w_mkt = res.x
    print("return of opt portfolio", res.fun*-1 )
    # 目标方程只能是最小化，因此需要乘以-1 

    # input1=input("Check for result...")

TODO




#################################################################################
### 4，评估指标
'''
两个因子分层打分回溯测试中5个分组的历史表现
衡量每组历史表现的指标包括：年化绝对收益，年化相对收益，累计绝对收益，累计相对收益，
年化波动，夏普比，最大回撤，最大相对回撤，信息比等等
'''




#################################################################################
### 5,通过构建加权组合，测试组合和指数的相关性
'''
1,假设3个股票，s1,s2,s3的权重分别 50%，30%，20%；
2，区间组合收益率为ret_p = ret1*0.5+ret2*0.3+ret3*0.2
3,计算相关性 ret_p.corr(ret_bm,method="pearson")
4,改进优化方向：
'''








































