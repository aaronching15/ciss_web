# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
todo:
topic：基金股票组合仿真和预测
功能：计算基金预测能力 fund_skill, 
###
last   | since 211201
derived from fund_simulation.py 
===============================================
'''

from calendar import c
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )
sys.path.append(path_ciss_rc + "db\\fund_analysis\\" )

import pandas as pd
import numpy as np
import math
import time 
#######################################################################
### 导入配置文件对象，例如path_db_wind等
from config_data import config_data_fund_ana
config_data_fund_ana_1 = config_data_fund_ana()
from data_io import data_io 
data_io_1 = data_io()
from times import times
times_1 = times()
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

from data_io_fund_ana import data_io_fund_ana
data_io_fund_ana_1 =  data_io_fund_ana()

from data_io_fund_ana import data_io_fund_simulation
data_io_fund_simu_1 = data_io_fund_simulation()

from fund_simulation import fund_simulation
class_fund_simu = fund_simulation()

################################################### 
###################################################
class fund_skill():
    def __init__(self ):
        self.obj_config = config_data_fund_ana_1.obj_config
        self.data_io_1 = data_io()
        #######################################################################
        ### 转换后的基金wds数据表
        self.path_wds_fund = self.obj_config["dict"]["path_wds_fund"]
        
        ### 基金分析数据输出目录
        self.path_ciss_db_fund = self.obj_config["dict"]["path_ciss_db_fund"]
        ### simulation 数据输出目录
        self.path_fund_simu = "D:\\CISS_db\\fund_simulation\\output\\"

    def print_info(self):        
        print("计算基金预测能力 fund_skill ")
        print("cal_fund_skill | 预测能力评估：skill_set，计算三类skill 指标")
        print("cal_stat_skill_ret | 统计多季度skill均值和平均拟合收益率")

        
    def cal_fund_skill(self,obj_port,obj_fund_ana_next,obj_port_next ):
        ################################### 
        ### s 2.7, 预测能力评估：skill_set，计算三类skill 指标
        ### 设置日期
        temp_date_next = obj_port["dict"]["temp_date_next"] 
        quarter_end_next = obj_port["dict"]["quarter_end_next"] 
        ###
        fund_list_short = obj_fund_ana_next["dict"]["fund_list_short"] 
        ### df_port_unit_next 包括了每个组合的绩效指标：
        df_port_unit_next = obj_port_next["df_port_unit"] 
        df_port_unit = obj_port["df_port_unit"] 
        
        ### 注意当季用的 df_perf_eval ，下一个季度用的 df_perf_eval_next
        df_perf_eval = obj_port["df_perf_eval"] 
        ### 每个基金约对应10个拟合simu的组合，组合名称保持在 temp_str,temp_str_date
        # temp_simu_port = "simu_" + temp_str + temp_fund
        list_temp_str = [ "ret_last" , "mdd_last", "vol_last","vol_relative_last","alpha_last","sharp_annual_last","calmar_annual_last" ] 
        # temp_simu_port = "simu_" + temp_str_date + "_" + temp_fund
        list_temp_str_date = [ "short" , "mid", "long" ]

        ############################################################################## 
        ### 2，预测能力评估：skill_set ：将下一季度基金净值与基础组合比较：先计算股票配置比例，再计算拟合程度 
        ##############################################################################     
        ### 2.1，skill_ret 基于收益率和风险指标的预测能力
        ### 新建df_skill_next,对于每一个基金、日期、拟合方式，绩效指标，比较和基金净值的差异。|| 
        ### "div" simu组合指标除以基金指标 ; "if_higher_better"：指标数值是否越大越好
        df_skill_next = pd.DataFrame( columns=["date","date_period","fund_code", "simu_type","perf_eval_type","skill","skill_ret","skill_stable","skill_stock","div","if_higher_better" ] ) 
        
        ###################################### 
        ### 更新 fund_list_short
        ### Notes：提取下一个季度净值时，可能出现历史上有净值，但数据文件里没有，比如184688.SZ在20070801没有净值数据
        fund_list_short_new =[]
        for temp_fund in fund_list_short  : 
            if temp_fund in df_port_unit_next.index :
                fund_list_short_new = fund_list_short_new + [temp_fund]
        fund_list_short = fund_list_short_new
        print("debug== fund_list_short",fund_list_short)
        count_index = 0 
        for temp_fund in fund_list_short  : 
            ###            
            ### 获取股票配置比例,用当季度的股票配置比例。
            temp_stock_pct = df_perf_eval.loc[temp_fund,"stock_pct"]
            #######################################
            ### 2.1,skill_ret: 收益率、回撤、风险、超额收益、股票配置比例 四个维度打分 | 还要考虑股票配置比例
            #######################################
            ### 基于单一指标的多个时期
            for temp_simu in list_temp_str +list_temp_str_date  :
                temp_simu_port = "simu_" + temp_simu +"_"+ temp_fund             
                
                #################################################################################
                ### 方法一：1，单指标拟合
                ### 分别对每个指标进行计算：alpha_last_short ，sharp_annual_last_short，calmar_annual_last_short | sortino数值太夸张不要。
                temp_list = []
                ### 指标数值是否越大越好 "if_higher_better"=1 
                for temp_str in [ "ret_last_" , "mdd_last_", "alpha_last_","sharp_annual_last_","calmar_annual_last_" ] :
                    ### 设置基金代码和拟合方式
                    df_skill_next.loc[count_index, "fund_code"] = temp_fund
                    df_skill_next.loc[count_index, "simu_type" ] =  temp_simu
                    ### 设置绩效评估类型
                    df_skill_next.loc[count_index, "perf_eval_type" ] =  temp_str
                    ### 
                    df_skill_next.loc[count_index, "if_higher_better"] =1 
                    #######################################
                    ### 2，基于单个指标拟合   收益率，ret_last_ ；ret_last_short	ret_last_mid,ret_last_long
                    ### 长、中、短期指标加权
                    ### Notes：提取下一个季度净值时，可能出现历史上有净值，但数据文件里没有，比如184688.SZ在20070801没有净值数据

                    temp_div = 0.5* df_port_unit_next.loc[temp_simu_port , temp_str+"long" ]/ df_port_unit_next.loc[temp_fund , temp_str+"long" ]
                    temp_div = temp_div + 0.3* df_port_unit_next.loc[temp_simu_port , temp_str+"mid" ]/ df_port_unit_next.loc[temp_fund , temp_str+"mid" ]
                    temp_div = temp_div + 0.2* df_port_unit_next.loc[temp_simu_port , temp_str+"short" ]/ df_port_unit_next.loc[temp_fund , temp_str+"short" ]
                    ### 还要考虑股票配置比例 | 如果拟合组合指标收益率 10%，股票配置比例60%，则基金收益率应该是6%=10%*60%
                    temp_div = temp_div * temp_stock_pct
                    ### "div" simu组合指标除以基金指标 ; "if_higher_better"：指标数值是否越大越好

                    df_skill_next.loc[count_index, "div" ] = temp_div
                                        ###
                    temp_list = temp_list + [ temp_div ]
                    count_index = count_index + 1 
                ### 指标数值是否越小越好 "if_higher_better"=0 
                for temp_str in [  "vol_last_","vol_relative_last_"  ] :
                    ### 设置基金代码和拟合方式
                    df_skill_next.loc[count_index, "fund_code"] = temp_fund
                    df_skill_next.loc[count_index, "simu_type" ] =  temp_simu
                    ### 设置绩效评估类型
                    df_skill_next.loc[count_index, "perf_eval_type" ] =  temp_str

                    df_skill_next.loc[count_index, "if_higher_better"] = 0 
                    #######################################
                    ### 2，基于单个指标拟合   收益率，ret_last_ ；ret_last_short	ret_last_mid,ret_last_long
                    ### 长、中、短期指标加权
                    temp_div = 0.5* df_port_unit_next.loc[temp_simu_port , temp_str+"long" ]/ df_port_unit_next.loc[temp_fund , temp_str+"long" ]
                    temp_div = temp_div + 0.3* df_port_unit_next.loc[temp_simu_port , temp_str+"mid" ]/ df_port_unit_next.loc[temp_fund , temp_str+"mid" ]
                    temp_div = temp_div + 0.2* df_port_unit_next.loc[temp_simu_port , temp_str+"short" ]/ df_port_unit_next.loc[temp_fund , temp_str+"short" ]
                    ### 还要考虑股票配置比例 | 如果拟合组合指标收益率 10%，股票配置比例60%，则基金收益率应该是6%=10%*60%
                    temp_div = temp_div * temp_stock_pct
                    ### "div" simu组合指标除以基金指标 ; "if_higher_better"：指标数值是否越大越好

                    df_skill_next.loc[count_index, "div" ] = temp_div
                    ###
                    temp_list = temp_list + [ temp_div ]
                    count_index = count_index + 1  

                #################################################################################
                ### 方法二：分长、中、短期拟合 
                for temp_str_date in [ "short" , "mid", "long" ] :
                    ### 设置基金代码和拟合方式
                    df_skill_next.loc[count_index, "fund_code"] = temp_fund
                    df_skill_next.loc[count_index, "simu_type" ] =  temp_simu
                    ### 设置绩效评估类型
                    df_skill_next.loc[count_index, "perf_eval_type" ] =  temp_str_date
                    ### 指标数值有大有小 "if_higher_better"=0.5 
                    df_skill_next.loc[count_index, "if_higher_better"] = 0.5
                    #######################################
                    ### 2，基于单个指标拟合   收益率，ret_last_ ；ret_last_short	ret_last_mid,ret_last_long 等等指标加权
                    col_list_indi_1 = ["ret_last_" + temp_str_date ,"mdd_last_" + temp_str_date,"alpha_last_" + temp_str_date]
                    col_list_indi_2 = ["vol_last_" + temp_str_date,"vol_relative_last_" + temp_str_date, "alpha_annual_last_" + temp_str_date,"sharp_annual_last_" + temp_str_date,"calmar_annual_last_" + temp_str_date]
                    
                    #######################################
                    ### 将指标标准化后相加 | 取绝对值
                    ### 计算基础组合里该指标和基金组合该指标的偏离程度 
                    temp_div = 0.4* df_port_unit_next.loc[temp_simu_port , "ret_last_" + temp_str_date ]/df_port_unit_next.loc[temp_fund , "ret_last_" + temp_str_date ]
                    temp_div =temp_div + 0.2* df_port_unit_next.loc[temp_simu_port , "mdd_last_" + temp_str_date ]/df_port_unit_next.loc[temp_fund , "mdd_last_" + temp_str_date ]
                    temp_div =temp_div + 0.1* df_port_unit_next.loc[temp_simu_port , "alpha_last_" + temp_str_date ]/df_port_unit_next.loc[temp_fund , "alpha_last_" + temp_str_date ]
                    ### 剩余50%权重平均分配给
                    for temp_indi in col_list_indi_2 :
                        ### 
                        temp_div =temp_div + 0.1* df_port_unit_next.loc[temp_simu_port ,temp_indi ]/df_port_unit_next.loc[temp_fund ,temp_indi ]

                    ### 还要考虑股票配置比例 | 如果拟合组合指标收益率 10%，股票配置比例60%，则基金收益率应该是6%=10%*60%
                    temp_div = temp_div * temp_stock_pct
                    df_skill_next.loc[count_index, "div" ] = temp_div
                    ###
                    temp_list = temp_list + [ temp_div ]
                    count_index = count_index + 1 
                

                #######################################
                ### 计算 skill_ret 平均值
                ### 设置基金代码和拟合方式
                df_skill_next.loc[count_index, "fund_code"] = temp_fund
                df_skill_next.loc[count_index, "simu_type" ] =  temp_simu
                df_skill_next.loc[count_index, "perf_eval_type" ] =  "average"
                df_skill_next.loc[count_index, "div" ] =  pd.DataFrame( temp_list ).mean().values[0]
                ###
                count_index = count_index + 1 

        ### 
        df_skill_next["date"] = temp_date_next  
        df_skill_next["date_period"] = quarter_end_next 
        #############################################################
        ### 原始skill数值最理想是70%~100%，但是真实值可能是在 -1000%到+1000%
        # 把 df_skill_next["div" ] 转化成 skill; div去除极端值，取值介于【0，200%】
        ### Skill算法：直接得分，大于100大于100的，200-x ||不管数值越大越好还是越小越好都一样
        df_skill_next["skill_ret" ] = df_skill_next["div"].apply( lambda x : min(2 , max(0, x)) )
        df_skill_next["skill_ret" ] = df_skill_next["skill_ret"].apply( lambda x :  x if x<=1.0 else 2.0-x  ) 

        ###################################################################################################  
        ###################################################################################################
        ### 2.2,skill_stable：稳定性，比较预测季度和上一季度的指标变化程度 | 收益率、回撤、风险、超额收益 四个维度打分
        ### df_port_unit 和 df_port_unit_next 进行比较：
        #######################################
        ### 当季度预测指标，主要区别 ：df_port_unit VS df_port_unit_next
        df_skill = pd.DataFrame( columns=["date","date_period","fund_code", "simu_type","perf_eval_type","skill","skill_ret","skill_stable","skill_stock","div","if_higher_better" ] ) 
        # Notes: df_skill 和 df_skill_next 的每一行对应的数值需要一样。
        count_index = 0 
        for temp_fund in fund_list_short  : 
            ###            
            ### 获取股票配置比例
            temp_stock_pct = df_perf_eval.loc[temp_fund,"stock_pct"]
            #######################################
            ### 2.1,skill_ret: 收益率、回撤、风险、超额收益、股票配置比例 四个维度打分 | 还要考虑股票配置比例
            #######################################
            ### 基于单一指标的多个时期  
            for temp_simu in list_temp_str +list_temp_str_date + ["top10"] :
                if temp_simu == "top10" :
                    temp_simu_port =  temp_simu +"_"+ temp_fund  
                else :
                    temp_simu_port = "simu_" + temp_simu +"_"+ temp_fund  
                #################################################################################
                ### 方法一：1，单指标拟合
                ### 分别对每个指标进行计算：alpha_last_short ，sharp_annual_last_short，calmar_annual_last_short | sortino数值太夸张不要。
                temp_list = []
                ### 指标数值是否越大越好 "if_higher_better"=1 
                for temp_str in [ "ret_last" , "mdd_last", "alpha_last","sharp_annual_last","calmar_annual_last" ] :
                    ### 设置基金代码和拟合方式
                    df_skill.loc[count_index, "fund_code"] = temp_fund
                    df_skill.loc[count_index, "simu_type" ] =  temp_simu
                    ### 设置绩效评估类型
                    df_skill.loc[count_index, "perf_eval_type" ] =  temp_str
                    ### 
                    df_skill.loc[count_index, "if_higher_better"] =1 
                    #######################################
                    ### 2，基于单个指标拟合   收益率，ret_last_ ；ret_last_short	ret_last_mid,ret_last_long
                    ### 长、中、短期指标加权 
                    temp_div = 0.5* df_port_unit.loc[temp_simu_port , temp_str+"_long" ]/ df_port_unit.loc[temp_fund , temp_str+"_long" ]
                    temp_div = temp_div + 0.3* df_port_unit.loc[temp_simu_port , temp_str+"_mid" ]/ df_port_unit.loc[temp_fund , temp_str+"_mid" ]
                    temp_div = temp_div + 0.2* df_port_unit.loc[temp_simu_port , temp_str+"_short" ]/ df_port_unit.loc[temp_fund , temp_str+"_short" ]
                    ### 还要考虑股票配置比例 | 如果拟合组合指标收益率 10%，股票配置比例60%，则基金收益率应该是6%=10%*60%
                    temp_div = temp_div * temp_stock_pct
                    ### "div" simu组合指标除以基金指标 ; "if_higher_better"：指标数值是否越大越好

                    df_skill.loc[count_index, "div" ] = temp_div
                                        ###
                    temp_list = temp_list + [ temp_div ]
                    count_index = count_index + 1 
                ### 指标数值是否越小越好 "if_higher_better"=0 
                for temp_str in [  "vol_last","vol_relative_last"  ] :
                    ### 设置基金代码和拟合方式
                    df_skill.loc[count_index, "fund_code"] = temp_fund
                    df_skill.loc[count_index, "simu_type" ] =  temp_simu
                    ### 设置绩效评估类型
                    df_skill.loc[count_index, "perf_eval_type" ] =  temp_str

                    df_skill.loc[count_index, "if_higher_better"] = 0 
                    #######################################
                    ### 2，基于单个指标拟合   收益率，ret_last_ ；ret_last_short	ret_last_mid,ret_last_long
                    ### 长、中、短期指标加权
                    temp_div = 0.5* df_port_unit.loc[temp_simu_port , temp_str+"_long" ]/ df_port_unit.loc[temp_fund , temp_str+"_long" ]
                    temp_div = temp_div + 0.3* df_port_unit.loc[temp_simu_port , temp_str+"_mid" ]/ df_port_unit.loc[temp_fund , temp_str+"_mid" ]
                    temp_div = temp_div + 0.2* df_port_unit.loc[temp_simu_port , temp_str+"_short" ]/ df_port_unit.loc[temp_fund , temp_str+"_short" ]
                    ### 还要考虑股票配置比例 | 如果拟合组合指标收益率 10%，股票配置比例60%，则基金收益率应该是6%=10%*60%
                    temp_div = temp_div * temp_stock_pct
                    ### "div" simu组合指标除以基金指标 ; "if_higher_better"：指标数值是否越大越好

                    df_skill.loc[count_index, "div" ] = temp_div
                    ###
                    temp_list = temp_list + [ temp_div ]
                    count_index = count_index + 1  

                #################################################################################
                ### 方法二：分长、中、短期拟合 
                for temp_str_date in [ "short" , "mid", "long" ] :
                    ### 设置基金代码和拟合方式
                    df_skill.loc[count_index, "fund_code"] = temp_fund
                    df_skill.loc[count_index, "simu_type" ] =  temp_simu
                    ### 设置绩效评估类型
                    df_skill.loc[count_index, "perf_eval_type" ] =  temp_str_date
                    ### 指标数值有大有小 "if_higher_better"=0.5 
                    df_skill.loc[count_index, "if_higher_better"] = 0.5
                    #######################################
                    ### 2，基于单个指标拟合   收益率，ret_last_ ；ret_last_short	ret_last_mid,ret_last_long 等等指标加权
                    col_list_indi_1 = ["ret_last_" + temp_str_date ,"mdd_last_" + temp_str_date,"alpha_last_" + temp_str_date]
                    col_list_indi_2 = ["vol_last_" + temp_str_date,"vol_relative_last_" + temp_str_date, "alpha_annual_last_" + temp_str_date,"sharp_annual_last_" + temp_str_date,"calmar_annual_last_" + temp_str_date]
                    
                    #######################################
                    ### 将指标标准化后相加 | 取绝对值
                    ### 计算基础组合里该指标和基金组合该指标的偏离程度 
                    ### col_list_indi_1:
                    temp_div = 0.4* df_port_unit.loc[temp_simu_port , "ret_last_" + temp_str_date ]/df_port_unit.loc[temp_fund , "ret_last_" + temp_str_date ]
                    temp_div =temp_div + 0.2* df_port_unit.loc[temp_simu_port , "mdd_last_" + temp_str_date ]/df_port_unit.loc[temp_fund , "mdd_last_" + temp_str_date ]
                    temp_div =temp_div + 0.1* df_port_unit.loc[temp_simu_port , "alpha_last_" + temp_str_date ]/df_port_unit.loc[temp_fund , "alpha_last_" + temp_str_date ]
                    ### col_list_indi_2:剩余50%权重平均分配
                    for temp_indi in col_list_indi_2 :
                        ### 
                        temp_div =temp_div + 0.1* df_port_unit.loc[temp_simu_port ,temp_indi ]/df_port_unit.loc[temp_fund ,temp_indi ]

                    ### 还要考虑股票配置比例 | 如果拟合组合指标收益率 10%，股票配置比例60%，则基金收益率应该是6%=10%*60%
                    temp_div = temp_div * temp_stock_pct
                    df_skill.loc[count_index, "div" ] = temp_div
                    ###
                    temp_list = temp_list + [ temp_div ]
                    count_index = count_index + 1 
                    
                #######################################
                ### 计算 skill_ret 平均值
                ### 设置基金代码和拟合方式
                df_skill.loc[count_index, "fund_code"] = temp_fund
                df_skill.loc[count_index, "simu_type" ] =  temp_simu
                df_skill.loc[count_index, "perf_eval_type" ] =  "average"
                df_skill.loc[count_index, "div" ] =  pd.DataFrame( temp_list ).mean().values[0]
                ###
                count_index = count_index + 1   

        
        #######################################
        ### 都算完了。
        df_skill["date"] = temp_date_next  
        df_skill["date_period"] = quarter_end_next 
        #############################################################
        ### 原始skill数值最理想是70%~100%，但是真实值可能是在 -1000%到+1000%
        # 把 df_skill["div" ] 转化成 skill; div去除极端值，取值介于【0，200%】
        ### Skill算法：直接得分，大于100大于100的，200-x ||不管数值越大越好还是越小越好都一样
        df_skill["skill_ret" ] = df_skill["div"].apply( lambda x : min(2 , max(0, x)) )
        df_skill["skill_ret" ] = df_skill["skill_ret"].apply( lambda x :  x if x<=1.0 else 2.0-x  )

        #############################################################
        ### skill_stable: 计算前后2个季度之间 skill_ret的变化，保存在 obj_port["df_skill_next"]   
        # notes: df_skill和 df_skill_next数据结构基本一致，需要用3个column的值同时匹配
        # 匹配：fund_code	simu_type	perf_eval_type || 213001.OF	ret_last_	average 
        df_skill_next["div_pre"] = df_skill["div"] 
        df_skill_next["skill_ret_pre"] =df_skill["skill_ret"] 
        ### 预防有 0.0 无法被除的情况。
        df_skill_next["div_pre"].apply(lambda x : 0.00001 if x == 0.0 else x )
        ### 计算差异率指标的变化率：最理想是没有变化，也就是 1.0，
        df_skill_next["div_div"] = df_skill_next["div"] / df_skill_next["div_pre"] 
        ### 计算skill变化率：1，div_div取值介于【0，200%】 ；2，直接得分，大于100大于100的，200-x ||不管数值越大越好还是越小越好都一样
        df_skill_next["skill_stable" ] = df_skill_next["div_div"].apply( lambda x : min(2 , max(0, x)) )
        df_skill_next["skill_stable" ] = df_skill_next["skill_stable"].apply( lambda x :  x if x<=1.0 else 2.0-x  )

        #############################################################
        ### 临时保存 save to 
        # df_skill.to_excel("D:\\debug-df_skill.xlsx")
        # df_skill_next.to_excel("D:\\debug-df_skill_next.xlsx")

        ##############################################################################
        ##############################################################################
        ### 2.2,skill_stock：simu股票组合前十大命中率、前5行业命中率
        #######################################
        ### 2.2.1，导入下一季度基金前十大重仓股
        ### 参考cal_fund_port_top10stock | 再次导入下一个季度的基金十大重仓股，存到 df_ashare_ana_next里用于比较
        obj_fund_ana_next["dict"]["fund_list_given"] = fund_list_short
        obj_fund_ana_next,obj_port_next = obj_fund_ana,obj_port = class_fund_simu.cal_fund_port_top10stock_given(obj_fund_ana_next,obj_port_next )
        ### output: 股票持仓数据在： obj_port_next["df_ashare_ana"] 

        #######################################
        ### 2.2.2，导入下一个季度基金预测组合的基础组合，合并后的前十大重仓股 
        ### Notes:有可能出现部分股票下一个季度无法导入，例如2007-2的600205，在20170109退市,或者港股缺乏数据。
        df_ashare_ana_next = obj_port_next["df_ashare_ana"]
        # print("Debug df_ashare_ana_next可能缺乏某些股票代码") 

        for temp_fund in fund_list_short :
            #######################################
            ### 构成基金simu拟合组合配置的信息在  df_perf_eval
            ### 基于单一指标的多个时期
            for temp_str in list_temp_str:
                temp_simu_port = "simu_" + temp_str +"_" + temp_fund
                ### 上一季度预测的股票配置比例
                temp_stock_pct = df_perf_eval.loc[temp_fund,"stock_pct"]
                ### 基础组合及权重
                temp_port1 = df_perf_eval.loc[ temp_simu_port,"simu_port_1"] 
                temp_port1_w = df_perf_eval.loc[ temp_simu_port,"simu_port_1_w"]
                temp_port2 = df_perf_eval.loc[ temp_simu_port,"simu_port_2"]
                temp_port2_w = df_perf_eval.loc[ temp_simu_port,"simu_port_2_w"] 
                temp_port3 = df_perf_eval.loc[ temp_simu_port,"simu_port_3"] 
                temp_port3_w = df_perf_eval.loc[ temp_simu_port,"simu_port_3_w"]  
                ###
                #######################################
                ### 从df_ashare_ana_next 合并组合权重
                ### Notes:有可能出现部分股票下一个季度无法导入，例如2007-2的600205，在20170109退市,或者港股缺乏数据。
                # 解决办法，逐个判断拟合组合是否存在 
                if temp_port1 in df_ashare_ana_next.columns :
                    temp_sum = temp_port1_w *df_ashare_ana_next[ temp_port1 ]
                    temp_sum_w = 1 
                else :
                    temp_sum = 0.0
                    temp_sum_w = 1 - temp_port1_w 
                if temp_port2 in df_ashare_ana_next.columns :
                    temp_sum = temp_sum + temp_port2_w *df_ashare_ana_next[ temp_port2 ]
                    ### temp_sum_w 数值不变
                else :
                    ### temp_sum 数值不变;temp_sum_w 数值要减少
                    temp_sum_w = temp_sum_w - temp_port2_w 
                if temp_port3 in df_ashare_ana_next.columns :
                    temp_sum = temp_sum + temp_port3_w *df_ashare_ana_next[ temp_port3 ]
                    ### temp_sum_w 数值不变
                else :
                    ### temp_sum 数值不变;temp_sum_w 数值要减少
                    temp_sum_w = temp_sum_w - temp_port3_w 
                if temp_sum_w > 0 :
                    df_ashare_ana_next[temp_simu_port ] = temp_sum
                else :
                    ### 就当组合不存在
                    df_ashare_ana_next[temp_simu_port ] = 0.0
                ####################################### 
                
            # df_port_unit.to_excel("D:\\debug-df_port_unit.xlsx")
            #######################################
            ### 基于日期的指标组合
            for temp_str_date in list_temp_str_date :
                temp_simu_port =  "simu_" + temp_str_date + "_" + temp_fund
                ### 获取数据
                temp_port1 = df_perf_eval.loc[ temp_simu_port,"simu_port_1"] 
                temp_port1_w = df_perf_eval.loc[ temp_simu_port,"simu_port_1_w"]
                temp_port2 = df_perf_eval.loc[ temp_simu_port,"simu_port_2"]
                temp_port2_w = df_perf_eval.loc[ temp_simu_port,"simu_port_2_w"] 
                temp_port3 = df_perf_eval.loc[ temp_simu_port,"simu_port_3"] 
                temp_port3_w = df_perf_eval.loc[ temp_simu_port,"simu_port_3_w"] 
                #######################################
                ### 从df_ashare_ana_next 合并组合权重
                ### Notes:有可能出现部分股票下一个季度无法导入，例如2007-2的600205，在20170109退市,或者港股缺乏数据。
                # 解决办法，逐个判断拟合组合是否存在 
                if temp_port1 in df_ashare_ana_next.columns :
                    temp_sum = temp_port1_w *df_ashare_ana_next[ temp_port1 ]
                    temp_sum_w = 1 
                else :
                    temp_sum = 0.0
                    temp_sum_w = 1 - temp_port1_w 
                if temp_port2 in df_ashare_ana_next.columns :
                    temp_sum = temp_sum + temp_port2_w *df_ashare_ana_next[ temp_port2 ]
                    ### temp_sum_w 数值不变
                else :
                    ### temp_sum 数值不变;temp_sum_w 数值要减少
                    temp_sum_w = temp_sum_w - temp_port2_w 
                if temp_port3 in df_ashare_ana_next.columns :
                    temp_sum = temp_sum + temp_port3_w *df_ashare_ana_next[ temp_port3 ]
                    ### temp_sum_w 数值不变
                else :
                    ### temp_sum 数值不变;temp_sum_w 数值要减少
                    temp_sum_w = temp_sum_w - temp_port3_w 
                if temp_sum_w > 0 :
                    df_ashare_ana_next[temp_simu_port ] = temp_sum
                else :
                    ### 就当组合不存在
                    df_ashare_ana_next[temp_simu_port ] = 0.0
                ### 下边的旧版本会忽视部分组合，特别是单股票组合因为退市没有数据的问题。
                # df_ashare_ana_next[temp_simu_port ] = temp_port1_w *df_ashare_ana_next[ temp_port1 ] + temp_port2_w *df_ashare_ana_next[ temp_port2 ] +  temp_port3_w *df_ashare_ana_next[ temp_port3 ]  
                ####################################### 

        # df_ashare_ana_next.to_excel("D:\\debug-df_ashare_ana_next.xlsx")
        ##############################################################################
        ### 2.2.3，前十大持仓股票命中率、命中股票合计权重命中率、行业配置前三命中率。
        df_skill_next["num_top10"]=0
        df_skill_next["num_top10_pct"]=0.0
        df_skill_next["weight_top10_pct"]=0.0
        ### notes: 每一个simu组合，股票命中率只赋值给 "average"
        for temp_fund in fund_list_short : 
            #######################################
            ### Notes:有可能存在没有前十大重仓股的情况，例如 top10_160612.OF,20121101
            if "top10_"+temp_fund in df_ashare_ana_next.columns :
                #######################################
                ### 前十大持仓组合 |  df_ashare_ana_next.loc[ :, "top10_"+temp_fund ]
                df_temp_top10 = df_ashare_ana_next [ df_ashare_ana_next["top10_"+temp_fund]> 0  ]
                ### weight_top10 :前十大持仓股票合集权重
                weight_top10 = df_temp_top10[ "top10_"+temp_fund ].sum()
                #######################################
                ### 计算前3大行业配置比例："ind_code" 
                df_ind_temp = df_temp_top10.loc[:,["ind_code","top10_"+temp_fund ]  ].groupby("ind_code").sum()
                df_ind_temp = df_ind_temp.sort_values(by="top10_"+temp_fund,ascending=False )
                ### notes:有可能十大重仓股都是一个行业，ind_list 数量在0~3之间 
                ind_list = list( df_ind_temp.index[:3] )
                ### len_ind_list前十大持仓有可能只有一个行业甚至没有。
                len_ind_list = len( ind_list )
                ### 注意：不能简单求和后比较，要1个1个比较            
                if len_ind_list > 0 :
                    ind_1 = df_ind_temp.index[0]
                    ind_1w = df_ind_temp.loc[ind_1 ,"top10_"+temp_fund ]
                if len_ind_list > 1 :
                    ind_2 = df_ind_temp.index[1]
                    ind_2w = df_ind_temp.loc[ind_2 ,"top10_"+temp_fund ]
                if len_ind_list > 2 :
                    ind_3 = df_ind_temp.index[2]
                    ind_3w = df_ind_temp.loc[ind_3 ,"top10_"+temp_fund ]
                # ind_weight_sum = df_ind_temp["top10_"+temp_fund ][:3].sum()

                #######################################
                ### simu拟合组合 || df_ashare_ana_next[temp_simu_port ]
                #######################################
                ### 构成基金simu拟合组合配置的信息在  df_perf_eval
                ### 基于单一指标的多个时期
                for temp_str in [ "ret_last" , "mdd_last", "alpha_last","sharp_annual_last","calmar_annual_last","vol_last","vol_relative_last"]:
                    temp_simu_port = "simu_" + temp_str+ "_" + temp_fund 
                    ### 定位组合在df_skill_next 的位置： 
                    df_skill_next_temp = df_skill_next [ df_skill_next["fund_code"] == temp_fund ] 
                    df_skill_next_temp = df_skill_next_temp [ df_skill_next_temp["simu_type" ] ==  temp_str ]              
                    ### 设置绩效评估类型:每一个simu组合，股票命中率只赋值给 "average"
                    df_skill_next_temp = df_skill_next_temp [ df_skill_next_temp["perf_eval_type" ] == "average" ] 
                    ### get index from  df_skill_next_temp 
                    temp_index = df_skill_next_temp.index[0]
                    ### "num_top10"：10个股票里，simu组合有持仓的数量
                    df_skill_next.loc[temp_index, "num_top10"] = len( df_temp_top10[ df_temp_top10[ temp_simu_port ]>0].index) 
                    ####################################### 
                    ### 1，"num_top10"：10个股票里，simu组合有持仓的数量百分比
                    df_skill_next.loc[temp_index, "num_top10_pct"] = df_skill_next.loc[temp_index, "num_top10"] /10
                    ####################################### 
                    ### 2，"weight_top10_pct"：10个股票里，simu组合有持仓的权重比真实组合百分比
                    df_skill_next.loc[temp_index, "weight_top10_pct"] =df_temp_top10[ temp_simu_port ].sum()/ weight_top10  
                    ####################################### 
                    ### 3，"weight_ind3_pct"：计算10个股票里，前三行业配置命中率： 
                    ### 1,获取simu组合股票持仓，再统计行业分布
                    df_temp_simu = df_ashare_ana_next [ df_ashare_ana_next[ temp_simu_port  ]> 0  ]
                    df_temp_simu = df_temp_simu[ df_temp_simu["ind_code"].isin( ind_list ) ]
                    df_temp_simu_ind = df_temp_simu.loc[:,["ind_code", temp_simu_port ]  ].groupby("ind_code").sum()
                    ### 计算top10组合的前3大行业匹配程度 | 注意：不能简单求和后比较，要1个1个比较
                    ### 对于前三大行业配置，分别按照50%、30%、20%的比例计算相似度
                    ### notes:df_temp_simu_ind 不一定包含每一个行业 | len_ind_list前十大持仓有可能只有一个行业甚至没有。
                    temp_pct = 0.0
                    if len_ind_list > 0 :
                        if ind_1 in df_temp_simu_ind.index :
                            temp_pct = 0.5* df_temp_simu_ind.loc[ind_1,temp_simu_port]/ ind_1w
                        else :
                            temp_pct = 0.0
                    if len_ind_list > 1 :
                        if ind_2 in df_temp_simu_ind.index :
                            temp_pct = temp_pct + 0.3* df_temp_simu_ind.loc[ind_2,temp_simu_port]/ ind_2w
                        # else :### 否则数值不变                        
                    if len_ind_list > 2 :
                        if ind_3 in df_temp_simu_ind.index :
                            temp_pct = temp_pct + 0.2* df_temp_simu_ind.loc[ind_3,temp_simu_port]/ ind_3w
                        # else :### 否则数值不变                        
                    ### 
                    df_skill_next.loc[temp_index, "weight_ind3_pct"] = temp_pct
                    ### 4,汇总计算股票学习能力 skill_stock
                    temp_value = 0.3* df_skill_next.loc[temp_index, "num_top10_pct"] + 0.4* df_skill_next.loc[temp_index, "weight_top10_pct"] + 0.3*df_skill_next.loc[temp_index, "weight_ind3_pct"]
                    ### 去除极端值：0~200区间，
                    temp_value =  min(2 , max(0, temp_value )  )
                    if temp_value > 1.0 :
                        temp_value = 2.0- temp_value 
                    ### 
                    df_skill_next.loc[temp_index, "skill_stock"] =temp_value 


                #######################################
                ### 基于日期的指标组合
                for temp_str_date in list_temp_str_date :
                    temp_simu_port =  "simu_" + temp_str_date + "_" + temp_fund
                    ### 定位组合在df_skill_next 的位置： 
                    df_skill_next_temp = df_skill_next [ df_skill_next["fund_code"] == temp_fund ]
                    df_skill_next_temp = df_skill_next_temp [ df_skill_next_temp["simu_type" ] ==  temp_str_date ]                 
                    ### 设置绩效评估类型:每一个simu组合，股票命中率只赋值给 "average"
                    df_skill_next_temp = df_skill_next_temp [ df_skill_next_temp["perf_eval_type" ] == "average" ] 
                    ### get index from  df_skill_next_temp
                    # print("Debug===","simu_" + temp_str_date + "_" + temp_fund,  temp_str_date,"average"  )
                    temp_index = df_skill_next_temp.index[0]
                    ### "num_top10"：10个股票里，simu组合有持仓的数量
                    df_skill_next.loc[temp_index, "num_top10"] = len( df_temp_top10[ df_temp_top10[ temp_simu_port ]>0].index) 
                    ####################################### 
                    ### 1，"num_top10_pct"：10个股票里，simu组合有持仓的数量百分比
                    df_skill_next.loc[temp_index, "num_top10_pct"] = df_skill_next.loc[ temp_index, "num_top10"] /10
                    ####################################### 
                    ### 2,"weight_top10_pct"：10个股票里，simu组合有持仓的权重比真实组合百分比
                    df_skill_next.loc[temp_index, "weight_top10_pct"] =df_temp_top10[ temp_simu_port ].sum()/ weight_top10  
                    ####################################### 
                    ### 3,"weight_ind3_pct"：计算10个股票里，前三行业配置命中率： 
                    ### 1,获取simu组合股票持仓，再统计行业分布
                    df_temp_simu = df_ashare_ana_next [ df_ashare_ana_next[ temp_simu_port  ]> 0  ]
                    df_temp_simu = df_temp_simu[ df_temp_simu["ind_code"].isin( ind_list ) ]
                    df_temp_simu_ind = df_temp_simu.loc[:,["ind_code", temp_simu_port ]  ].groupby("ind_code").sum()
                    ### 计算top10组合的前3大行业匹配程度 | 注意：不能简单求和后比较，要1个1个比较
                    ### 对于前三大行业配置，分别按照50%、30%、20%的比例计算相似度
                    ### notes:df_temp_simu_ind 不一定包含每一个行业 | len_ind_list前十大持仓有可能只有一个行业甚至没有。

                    temp_pct = 0.0
                    if len_ind_list > 0 :
                        if ind_1 in df_temp_simu_ind.index :
                            temp_pct = 0.5* df_temp_simu_ind.loc[ind_1,temp_simu_port]/ ind_1w
                        else :
                            temp_pct = 0.0
                    if len_ind_list > 1 :                
                        if ind_2 in df_temp_simu_ind.index :
                            temp_pct = temp_pct + 0.3* df_temp_simu_ind.loc[ind_2,temp_simu_port]/ ind_2w
                        # else :### 否则数值不变                        
                    if len_ind_list > 2 :
                        if ind_3 in df_temp_simu_ind.index :
                            temp_pct = temp_pct + 0.2* df_temp_simu_ind.loc[ind_3,temp_simu_port]/ ind_3w
                    ### 
                    df_skill_next.loc[temp_index, "weight_ind3_pct"] = temp_pct
                    ### 4,汇总计算股票学习能力 skill_stock
                    temp_value = 0.3* df_skill_next.loc[temp_index, "num_top10_pct"] + 0.4* df_skill_next.loc[temp_index, "weight_top10_pct"] + 0.3*df_skill_next.loc[temp_index, "weight_ind3_pct"]
                    ### 去除极端值：0~200区间，
                    temp_value =  min(2 , max(0, temp_value ) ) 
                    if temp_value > 1.0 :
                        temp_value = 2.0- temp_value 
                    ### 
                    df_skill_next.loc[temp_index, "skill_stock"] =temp_value 

            ##############################################################################
            ### 1个基金已经全部计算完了
        
        ##############################################################################
        ### 全部基金已经全部计算完了
        

        ##############################################################################
        ### 计算不分指标的平均值   
        index_next = df_skill_next.index.max()+1 
        for temp_fund in fund_list_short : 
            df_skill_next.loc[index_next, "fund_code"] = temp_fund
            df_skill_next.loc[index_next, "simu_type" ] =  "average"
            df_skill_next.loc[index_next, "perf_eval_type" ] =  "average"
            ### 
            df_temp = df_skill_next[ df_skill_next["fund_code"] == temp_fund ]
            df_temp = df_temp [df_temp ["perf_eval_type" ] ==  "average" ]
            df_skill_next.loc[index_next, "skill_ret" ] = df_temp["skill_ret"].mean()
            df_skill_next.loc[index_next, "skill_stable" ] = df_temp["skill_stable"].mean()
            df_skill_next.loc[index_next, "skill_stock" ] = df_temp["skill_stock"].mean()
            ###
            index_next = index_next + 1

        df_skill_next["date"] = temp_date_next  
        df_skill_next["date_period"] = quarter_end_next 
        ##############################################################################
        ### "skill","skill_ret","skill_stable","skill_stock"
        df_skill_next["skill" ] = 0.6*df_skill_next["skill_ret"] + 0.3*df_skill_next["skill_stable"] + 0.1*df_skill_next["skill_stock"]   

        ##############################################################################
        ### 保留部分columns:  columns=["date","date_period","fund_code", "simu_type","perf_eval_type","skill","skill_ret","skill_stable","skill_stock" ] 
        col_list_keep = ["date","date_period","fund_code", "simu_type","perf_eval_type","skill","skill_ret","skill_stable","skill_stock" ]
        df_skill_next = df_skill_next.loc[:, col_list_keep  ]
        df_skill_next_ave = df_skill_next[ df_skill_next["simu_type" ] == "average" ]
        df_skill_next.to_excel("D:\\debug-df_skill_next.xlsx")
        df_skill_next_ave.to_excel("D:\\debug-df_skill_next_ave.xlsx")
        
        ##############################################################################     
        ### 3，预测优化 | 比如10只基金的选择；10只基金综合来看哪个指标最好


        ### 
        obj_fund_ana_next["dict"]["fund_list_short"] = fund_list_short
        obj_port["df_skill"] = df_skill
        obj_port["df_skill_next"] = df_skill_next
        obj_port["df_skill_next_ave"] = df_skill_next_ave
        

        return obj_port

    def cal_stat_skill_ret(self,obj_skill ) :
        ######################################################################
        ### 统计多季度skill均值和平均拟合收益率
        
        df_date  = obj_skill["df_date"] 
        
        ### 1, 
        for temp_i in df_date.index:
            ### 获取区间日期            
            temp_date = df_date.loc[temp_i,"after_ann" ] 
            print("Working on date : ",temp_date  )
            #####################################
            ### 
            obj_port = data_io_fund_simu_1.import_fund_skill(temp_date)
            
            df_skill = obj_port["df_skill"]
            df_skill_next = obj_port["df_skill_next"]
            df_skill_next_ave = obj_port["df_skill_next_ave"]
            df_all_perf_eval_next = obj_port["df_all_perf_eval_next"]
            df_all_perf_eval = obj_port["df_all_perf_eval"]

            ### 当季度基金列表
            fund_list_short = obj_port["dict"]["fund_list_short"]  

            #########################################################
            ### 1, 每个季度所有基金的平均skill、3个细项目，组合数量x
            if len( df_skill_next.index ) > 0 : 
                ### "skill_ret","skill_stable","skill_stock"
                df_date.loc[temp_i, "skill"] = df_skill_next["skill"].mean()
                df_date.loc[temp_i, "skill_ret"] = df_skill_next["skill_ret"].mean()
                df_date.loc[temp_i, "skill_stable"] = df_skill_next["skill_stable"].mean()
                df_date.loc[temp_i, "skill_stock"] = df_skill_next["skill_stock"].mean()
                df_date.loc[temp_i, "skill_num"] = df_skill_next["skill"].count()
                
            #########################################################
            ### 2, 每个季度，计算开头为simu的基金平均季度收益率，如果没有，用市场组合替代；每一期收益率减去市场组合skill_ret	skill_stable	skill_stock
            df_all_perf_eval_next["index"] = df_all_perf_eval_next.index 
            ### 当季度的 simu_ 组合
            df_port_simu = df_all_perf_eval_next[ df_all_perf_eval_next["index"].str.contains("simu_") ] 

            ### 基金组合和十大重仓股组合 | 020005.OF
            ### notes:拟合组合里既包括
            df_fund = df_all_perf_eval_next[ df_all_perf_eval_next["index"].str.contains(".OF") ]
            df_fund = df_fund [df_fund.index.isin( fund_list_short ) ]
            if len( df_port_simu.index ) > 0 : 
                for temp_col in ["ret_last_long","mdd_last_long","alpha_last_long","sharp_annual_last_long"]:
                    ###
                    df_date.loc[temp_i, temp_col ] = df_port_simu[temp_col].mean()
                    ### 市场基准组合= "ret_last_long" - "alpha_last_long"
            if len( df_fund.index ) > 0 : 
                for temp_col in ["ret_last_long","mdd_last_long","alpha_last_long","sharp_annual_last_long"]:
                    ###
                    df_date.loc[temp_i,"fund_" +  temp_col ] = df_fund[temp_col].mean()
                    ### 市场基准组合= "ret_last_long" - "alpha_last_long"
            
            #########################################################
            ### 每个季度选取基金可预测指标前后20%数量的基金， "skill" 
            ### 保留当季度的平均skill
            df_skill= df_skill[df_skill["simu_type"]=="average"]
            df_skill= df_skill[df_skill["perf_eval_type"]=="average"]	
            # 降序排列 
            df_skill= df_skill.sort_values(by="skill" ,ascending=False)
            len_1 = int( len(df_skill_next.index)/5 )
            fund_list_skill_head20 = list( df_skill["fund_code" ].iloc[:len_1] )
            fund_list_skill_tail20 = list( df_skill["fund_code" ].iloc[-1*len_1:])
            ### 在下一季度里找打对应的基金 
            df_skill_next_head20 = df_skill_next[ df_skill_next["fund_code"].isin( fund_list_skill_head20 ) ]
            df_skill_next_tail20 = df_skill_next[ df_skill_next["fund_code"].isin( fund_list_skill_tail20 ) ]

            if len( df_skill_next_head20.index ) > 0 : 
                df_date.loc[temp_i, "skill"+"_head20"] = df_skill_next_head20["skill"].mean()
                df_date.loc[temp_i, "skill_ret"+"_head20"] = df_skill_next_head20["skill_ret"].mean()
                df_date.loc[temp_i, "skill_stable"+"_head20"] = df_skill_next_head20["skill_stable"].mean()
                df_date.loc[temp_i, "skill_stock"+"_head20"] = df_skill_next_head20["skill_stock"].mean() 
                

            if len( df_skill_next_tail20.index ) > 0 : 
                df_date.loc[temp_i, "skill"+"_tail20"] = df_skill_next_tail20["skill"].mean()
                df_date.loc[temp_i, "skill_ret"+"_tail20"] = df_skill_next_tail20["skill_ret"].mean()
                df_date.loc[temp_i, "skill_stable"+"_tail20"] = df_skill_next_tail20["skill_stable"].mean()
                df_date.loc[temp_i, "skill_stock"+"_tail20"] = df_skill_next_tail20["skill_stock"].mean() 

            #########################################################
            ### 选择超额收益最高、最低的前20%基金跟踪下一季度收益情况 ；"alpha_last_long"
            df_all_perf_eval= df_all_perf_eval[ df_all_perf_eval.index.isin( fund_list_short )  ]
            df_all_perf_eval = df_all_perf_eval.sort_values(by="alpha_last_long" ,ascending=False)
            len_2 = int( len( df_all_perf_eval.index)/5 )
            fund_list_head20 = list( df_all_perf_eval.index[:len_2] )
            fund_list_tail20 = list( df_all_perf_eval.index[-1*len_2:])
            ### 在下一季度里找打对应的基金 
            df_perf_eval_next_head20 = df_all_perf_eval_next[ df_all_perf_eval_next.index.isin( fund_list_head20 ) ]
            df_perf_eval_next_tail20 = df_all_perf_eval_next[ df_all_perf_eval_next.index.isin( fund_list_tail20 ) ]

            if len( df_perf_eval_next_head20.index ) > 0 : 
                for temp_col in ["ret_last_long" ]:
                    ###
                    df_date.loc[temp_i,"fund_head20_" + temp_col ] = df_perf_eval_next_head20[temp_col].mean()
                    ### 市场基准组合= "ret_last_long" - "alpha_last_long"
            if len( df_perf_eval_next_tail20.index ) > 0 : 
                for temp_col in ["ret_last_long" ]:
                    ###
                    df_date.loc[temp_i,"fund_tail20_" +  temp_col ] = df_perf_eval_next_tail20[temp_col].mean()
                    ### 市场基准组合= "ret_last_long" - "alpha_last_long"
            #################################################################
            ### 在下一季度里找打对应的基金拟合组合
            df_port_simu[ "if_head" ] = 0 
            df_port_simu[ "if_tail" ] = 0 
            df_port_simu[ "if_skill_head" ] = 0 
            df_port_simu[ "if_skill_tail" ] = 0 
            for temp_j in df_port_simu.index :
                # temp_i可能是 simu_short_162703.SZ
                if temp_j.split("_")[-1] in fund_list_head20  :
                    df_port_simu.loc[temp_j, "if_head" ] = 1 
                if temp_j.split("_")[-1] in fund_list_tail20  :
                    df_port_simu.loc[temp_j, "if_tail" ] = 1 
                ### skill_head20
                if temp_j.split("_")[-1] in fund_list_skill_head20  :
                    df_port_simu.loc[temp_j, "if_skill_head" ] = 1 
                if temp_j.split("_")[-1] in fund_list_skill_tail20  :
                    df_port_simu.loc[temp_j, "if_skill_tail" ] = 1 
            
            ### 计算平均收益率
            df_temp = df_port_simu[ df_port_simu["if_head" ]== 1  ]
            df_date.loc[temp_i,"fund_head20_" + "ret_last_long" ] = df_temp[ "ret_last_long" ].mean()
            df_temp = df_port_simu[ df_port_simu["if_tail"]== 1  ]
            df_date.loc[temp_i,"fund_tail20_" + "ret_last_long" ] = df_temp[ "ret_last_long" ].mean()
            df_temp = df_port_simu[ df_port_simu["if_skill_head"]== 1  ]
            df_date.loc[temp_i,"simu_skill_head20_" + "ret_last_long" ] = df_temp[ "ret_last_long" ].mean()
            df_temp = df_port_simu[ df_port_simu["if_skill_tail"]== 1  ]
            df_date.loc[temp_i,"simu_skill_tail20_" + "ret_last_long" ] = df_temp[ "ret_last_long" ].mean()


            ###
            df_date.to_excel("D:\\df_stat_skill_ret.xlsx") 

        ### save to output 
        obj_skill["df_stat_skill_ret"] = df_date 

        return obj_skill