# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
class perf_eval:策略绩效评估母类
1, class perf_eval_ashare_stra：子类：A股策略评估策略
2, class perf_eval_ashare_port：子类：A股组合收益率和持仓评估 

3，class perf_eval_money_cn：子类：中国现金工具，如逆回购GC001
4，class perf_eval_bond_cn：子类：中国债券
5，class perf_eval_fund_cn：子类：中国基金
6，class perf_eval_ashare_deriv_cn：子类：中国权益衍生品
1.1.7，class perf_eval_fund_deriv_cn：子类：中国债券衍生品
1.2,### 子类：A股组合收益率和持仓评估
2, TODO

3，关联脚本：对应配置文件 | config\ 

4,OUTPUT:
    1,obj_1["dict"]，字典信息,json
    1.1，把所有变量的中英文注释存在字典的obj_1["dict"]["notes"]里
    2,obj_1["df"]，表格信息,dataframe  

5,分析：目标是所有数据变量以object类型作为输入输出，其中主要是2个key:
    1,obj_1["dict"]:字典格式，数据io都采用json的字典格式。
    2,obj_1["df"]:DataFrame格式

6，Notes: 
6.1，因子组合分析功能perf_eval_ashare_stra()挪到 factor_model.py
    refernce: algo_opt.py
date:last 210124 | since 200422
===============================================
'''
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
import pandas as pd
import numpy as np
import math
#######################################################################
### 策略绩效评估母类
class perf_eval():
    def __init__(self):
        
        # 导入配置文件对象，例如path_db_wind等
        sys.path.append(path_ciss_rc+ "config\\")
        from config_data import config_data
        config_data_1 = config_data()
        self.obj_config = config_data_1.obj_config
        # print( self.obj_config["dict"] )

#######################################################################
### 子类：A股策略评估策略
class perf_eval_ashare_stra():
    ## 因子组合分析功能perf_eval_ashare_stra()挪到 factor_model.py
    def __init__(self):
        #######################################################################
        ### 导入配置文件对象，例如path_db_wind等
        sys.path.append(path_ciss_rc+ "config\\")
        from config_data import config_data_factor_model
        config_data_1 = config_data_factor_model()
        self.obj_config = config_data_1.obj_config
        ### factor_model 目录位置：
        # self.obj_config["dict"]["path_factor_model"] = dict_config["path_ciss_db"] +"factor_model\\"
        
        ### 导入date_list, 导入A股历史交易日期 
        df_dates = pd.read_csv(self.obj_config["dict"]["path_wind_adj"] + self.obj_config["dict"]["file_date_tradingday"]  )
        # type of date_list is numpy.int64
        self.date_list = list( df_dates["date"].values )
        self.date_list.sort()

        # month
        # df_dates = pd.read_csv(self.path_adj + self.obj_config["dict"]["file_date_month"] )
        # # type of date_list is numpy.int64
        # self.date_list_m = list( df_dates["date"].values )
        # self.date_list_m.sort()
        # # quarter
        # df_dates = pd.read_csv(self.path_adj + self.obj_config["dict"]["file_date_quarter"]   )
        # # type of date_list is numpy.int64
        # self.date_list_q = list( df_dates["date"].values )
        # self.date_list_q.sort()

    def print_info(self):
        print("perf_eval_ashare_factor_model |评估多因子模型单一时期的表现，例如历史和未来1~6月的收益，行业分布；每一期的重要收益指标存入 df_perf_eval,index=日期 ")
        print("perf_eval_ashare_1factor |评估单因子的分组收益 ")
        print("perf_eval_ashare_factors_group |给定区间，计算多因子和单因子组合收益 ")
        print("perf_eval_ashare_factors_ind_group |给定区间，计算多因子和单因子分行业分组收益 ")


        return 1 


#######################################################################
### 子类：A股组合收益率和持仓评估
class perf_eval_ashare_port():
    def __init__(self):
        #######################################################################
        ### 导入配置文件对象，例如path_db_wind等
        sys.path.append(path_ciss_rc+ "config\\")

        ###################################
        ### 因子模型不一定是必须的
        from config_data import config_data_factor_model
        config_data_1 = config_data_factor_model()
        self.obj_config = config_data_1.obj_config
        ### factor_model 目录位置：
        # self.obj_config["dict"]["path_factor_model"] = dict_config["path_ciss_db"] +"factor_model\\"
        
        ###################################
        ### 导入date_list, 导入A股历史交易日期 
        df_dates = pd.read_csv(self.obj_config["dict"]["path_wind_adj"] + self.obj_config["dict"]["file_date_tradingday"]  )
        # type of date_list is numpy.int64
        self.date_list = list( df_dates["date"].values )
        self.date_list.sort()

        ###################################
        # month
        # df_dates = pd.read_csv(self.path_adj + self.obj_config["dict"]["file_date_month"] )
        # # type of date_list is numpy.int64
        # self.date_list_m = list( df_dates["date"].values )
        # self.date_list_m.sort()
        # # quarter
        # df_dates = pd.read_csv(self.path_adj + self.obj_config["dict"]["file_date_quarter"]   )
        # # type of date_list is numpy.int64
        # self.date_list_q = list( df_dates["date"].values )
        # self.date_list_q.sort()

    def print_info(self):
        print("perf_eval_port_ret |评估单个组合区间收益指标  ")
        print("perf_eval_skill_ret |评估基于收益率的仿真组合得分 ")
        print("perf_eval_skill_stock |评估基于个股的仿真组合得分 ")
        print("perf_eval_port_ret_N |评估多个组合区间收益指标 ")
        print(" ")


        return 1 
        
    def perf_eval_port_ret(self,obj_perf_eval  ) :
        ### 评估组合日期收益率序列与不同基准的相关性、累计收益、最大回撤、区间内最高收益率、最低收益率等
        # notes:1,收益率应当是小数，例如 0.015，而不是百分比 1.5
        # 2, df_port_ret的index列是升序排列的日期，columns是不同的基金组合或市场、行业、主题分组
        df_port_ret = obj_perf_eval["df_port_ret"] 
        # notes:最近20、60、90天指的是期末日期前推！若日期升序排列，应该从尾部选
        # .sort_index()#默认为index升序
        df_port_ret = df_port_ret.sort_index()

        #######################################################################
        ### 计算df_port_ret 的累计收益率
        df_unit = df_port_ret+1 
        df_unit = df_unit.cumprod()
        ### 取df_unit 第20、60、90行的净值进行比较，还有计算区间最大回撤
        # df_unit.to_csv("D:\\df_unit.csv") 

        ### 新建组合收益、风险指标df
        df_port_perf_eval = pd.DataFrame( index = df_unit.columns )

        #######################################################################
        ### 计算所有组合区间内长期、中期、短期的最大回撤、高点开始最大跌幅，低点开始最大涨幅；收益率长期-中期，中期-短期。
        for temp_port in df_unit.columns : 
            print( temp_port )
            ######################################
            ### 长期收益和风险，90天
            unit_list =  df_unit[temp_port]
            ### 1,累计收益，ret_accu
            df_port_perf_eval.loc[temp_port,"ret_end_long" ] = unit_list[-1]/unit_list[0] -1 
            
            ### 区间内的最大回撤、
            # accu_high:区间累计最高价
            accu_high = np.maximum.accumulate( unit_list )
            mdd_list = unit_list /accu_high  -1 
            df_port_perf_eval.loc[temp_port, "mdd_long" ] = mdd_list.min()
            df_port_perf_eval.loc[temp_port, "mdd_index_long" ] = np.argmin( mdd_list )
            
            ### 最高收益及其index，高点开始最大跌幅，低点开始最大涨幅
            # 计算高点开始最大跌幅，先定位最高价的index,再计算最高点开始的list的最大回撤
            index_high = np.argmax( unit_list )
            df_port_perf_eval.loc[temp_port, "ret_high_long" ] = unit_list.max()/unit_list[0] -1 
            df_port_perf_eval.loc[temp_port, "ret_high_index_long" ] = index_high
            # 最高点开始取最低点
            accu_min_sub = np.minimum.accumulate( unit_list[index_high:] )
            mdd_list_sub = unit_list[index_high:] / accu_min_sub  -1 
            df_port_perf_eval.loc[temp_port, "mdd_fromhigh_long" ] =mdd_list_sub.min()
            # notes:index不能用最高价开始list的index，要加回最高价所处的index值
            df_port_perf_eval.loc[temp_port, "mdd_fromhigh_index_long" ] = np.argmin( mdd_list_sub ) + index_high

            ### 最低收益及其index，低点开始最大涨幅
            index_low = np.argmin( unit_list )
            df_port_perf_eval.loc[temp_port, "ret_low_long" ] = unit_list.min()/unit_list[0] -1 
            df_port_perf_eval.loc[temp_port, "ret_low_index_long" ] = index_low
            # 最低点开始取最高点
            accu_max_sub = np.maximum.accumulate( unit_list[index_low:] )
            accu_ret_sub = unit_list[index_high:] / accu_min_sub  -1 
            df_port_perf_eval.loc[temp_port, "ret_fromlow_long" ] = accu_ret_sub.max()
            # notes:index不能用最高价开始list的index，要加回最高价所处的index值
            df_port_perf_eval.loc[temp_port, "ret_fromlow_index_long" ] = np.argmax( accu_ret_sub ) + index_high
            ### 夏普比率
            df_port_perf_eval.loc[temp_port, "sharp_long" ] = np.mean(df_port_ret[temp_port])*np.sqrt(252)/ np.std( df_port_ret[temp_port] ) 

            ######################################
            ### 中期收益和风险，60天,选最后60天
            unit_list =  df_unit[temp_port][-60:]
            ### 1,累计收益，ret_accu
            df_port_perf_eval.loc[temp_port,"ret_end_mid" ] = unit_list[-1]/unit_list[0] -1 
            
            ### 区间内的最大回撤、
            # accu_high:区间累计最高价
            accu_high = np.maximum.accumulate( unit_list )
            mdd_list = unit_list /accu_high  -1 
            df_port_perf_eval.loc[temp_port, "mdd_mid" ] = mdd_list.min()
            df_port_perf_eval.loc[temp_port, "mdd_index_mid" ] = np.argmin( mdd_list )

            ### 最高收益及其index，高点开始最大跌幅，低点开始最大涨幅
            # 计算高点开始最大跌幅，先定位最高价的index,再计算最高点开始的list的最大回撤
            index_high = np.argmax( unit_list )
            df_port_perf_eval.loc[temp_port, "ret_high_mid" ] = unit_list.max()/unit_list[0] -1 
            df_port_perf_eval.loc[temp_port, "ret_high_index_mid" ] = index_high
            # 最高点开始取最低点
            accu_min_sub = np.minimum.accumulate( unit_list[index_high:] )
            mdd_list_sub = unit_list[index_high:] / accu_min_sub  -1 
            df_port_perf_eval.loc[temp_port, "mdd_fromhigh_mid" ] =mdd_list_sub.min()
            # notes:index不能用最高价开始list的index，要加回最高价所处的index值
            df_port_perf_eval.loc[temp_port, "mdd_fromhigh_index_mid" ] = np.argmin( mdd_list_sub ) + index_high

            ### 最低收益及其index，低点开始最大涨幅
            index_low = np.argmin( unit_list )
            df_port_perf_eval.loc[temp_port, "ret_low_mid" ] = unit_list.min()/unit_list[0] -1 
            df_port_perf_eval.loc[temp_port, "ret_low_index_mid" ] = index_low
            # 最低点开始取最高点
            accu_max_sub = np.maximum.accumulate( unit_list[index_low:] )
            accu_ret_sub = unit_list[index_high:] / accu_min_sub  -1 
            df_port_perf_eval.loc[temp_port, "ret_fromlow_mid" ] = accu_ret_sub.max()
            # notes:index不能用最高价开始list的index，要加回最高价所处的index值
            df_port_perf_eval.loc[temp_port, "ret_fromlow_index_mid" ] = np.argmax( accu_ret_sub ) + index_high
            ### 夏普比率
            df_port_perf_eval.loc[temp_port, "sharp_mid" ] = np.mean(df_port_ret[temp_port][-60:] )*np.sqrt(252)/ np.std( df_port_ret[temp_port][-60:] ) 

            ######################################
            ### 短期收益和风险，20天,选最后20天 
            unit_list =  df_unit[temp_port][-20:]
            ### 1,累计收益，ret_accu
            df_port_perf_eval.loc[temp_port,"ret_end_short" ] = unit_list[-1]/unit_list[0] -1 
            
            ### 区间内的最大回撤、
            # accu_high:区间累计最高价
            accu_high = np.maximum.accumulate( unit_list )
            mdd_list = unit_list /accu_high  -1 
            df_port_perf_eval.loc[temp_port, "mdd_short" ] = mdd_list.min()
            df_port_perf_eval.loc[temp_port, "mdd_index_short" ] = np.argmin( mdd_list )

            ### 最高收益及其index，高点开始最大跌幅，低点开始最大涨幅
            # 计算高点开始最大跌幅，先定位最高价的index,再计算最高点开始的list的最大回撤
            index_high = np.argmax( unit_list )
            df_port_perf_eval.loc[temp_port, "ret_high_short" ] = unit_list.max()/unit_list[0] -1 
            df_port_perf_eval.loc[temp_port, "ret_high_index_short" ] = index_high
            # 最高点开始取最低点
            accu_min_sub = np.minimum.accumulate( unit_list[index_high:] )
            mdd_list_sub = unit_list[index_high:] / accu_min_sub  -1 
            df_port_perf_eval.loc[temp_port, "mdd_fromhigh_short" ] =mdd_list_sub.min()
            # notes:index不能用最高价开始list的index，要加回最高价所处的index值
            df_port_perf_eval.loc[temp_port, "mdd_fromhigh_index_short" ] = np.argmin( mdd_list_sub ) + index_high

            ### 最低收益及其index，低点开始最大涨幅
            index_low = np.argmin( unit_list )
            df_port_perf_eval.loc[temp_port, "ret_low_short" ] = unit_list.min()/unit_list[0] -1 
            df_port_perf_eval.loc[temp_port, "ret_low_index_short" ] = index_low
            # 最低点开始取最高点
            accu_max_sub = np.maximum.accumulate( unit_list[index_low:] )
            accu_ret_sub = unit_list[index_high:] / accu_min_sub  -1 
            df_port_perf_eval.loc[temp_port, "ret_fromlow_short" ] = accu_ret_sub.max()
            # notes:index不能用最高价开始list的index，要加回最高价所处的index值
            df_port_perf_eval.loc[temp_port, "ret_fromlow_index_short" ] = np.argmax( accu_ret_sub ) + index_high
            ### 夏普比率
            df_port_perf_eval.loc[temp_port, "sharp_short" ] = np.mean(df_port_ret[temp_port][-20:] )*np.sqrt(252) / np.std( df_port_ret[temp_port][-20:] ) 

        ### 增加累计收益率的差额指标 diff_ret ，diff_mdd
        df_port_perf_eval["diff_ret_long_mid"] = df_port_perf_eval["ret_end_long"] - df_port_perf_eval["ret_end_mid"]
        df_port_perf_eval["diff_ret_mid_short"] = df_port_perf_eval["ret_end_mid"] - df_port_perf_eval["ret_end_short"]
        # diff_mdd 这个好像没啥意义

        #######################################################################
        ### 计算基本统计值：stat：20、60、90天
        ### 需要的index： mean,std ,
        df_stat_long = df_port_ret.describe()
        df_stat_mid = df_port_ret.iloc[-60: ,:].describe()
        df_stat_short = df_port_ret.iloc[-20: ,:].describe()

        ###################################### 
        ### df.corr() 计算所有列之间的相关性，其中基金名称那一列对应了所有其他分组的相关性，也包括自己
        
        temp_fund_code = obj_perf_eval["port_name"]
        # 相关性计算：pearson:', data['id'].corr(data['age']))'spearman', data['id'].corr(data['age'], method='spearman'))
        df_temp_long = df_port_ret.corr()[ temp_fund_code  ]
        df_temp_mid = df_port_ret.iloc[-60: ,:].corr()[ temp_fund_code  ]
        df_temp_short = df_port_ret.iloc[-20: ,:].corr()[ temp_fund_code  ]

        ####################################### 
        ### 根据组合或个股的收益率序列或价格序列，统计常用指标
        # 新建df_corr,保存相关性：
        df_corr = pd.DataFrame( [df_temp_short ,df_temp_mid,df_temp_long] ).T
        df_corr.columns=["short","mid","long"]
        df_corr = df_corr.drop([temp_fund_code] ,axis=0 )
        ### 相关性数值看，相关性最高的介于26~42%之间，最低-14%~-53%之间，相关性的变动（）看，
        # notes:日收益率序列相关性最高的组合，收益率可能会和基金组合收益率的值相差+/-15%
        
        #######################################################################    
        ### save to output 
        obj_perf_eval["df_port_perf_eval"] = df_port_perf_eval
        obj_perf_eval["df_corr"] = df_corr                      

        return obj_perf_eval
    
    def perf_eval_skill_ret(self, obj_perf_eval ):
        ### 评估基于收益率的仿真组合得分
        #notes:假定已经完成perf_eval_port_ret() 的计算，和相关指标：累计收益、最大回撤、区间内最高收益率、最低收益率等
        # df_port_perf_eval：index是包括目标组合或基金名称在内的所有组合，columns是不同的评价指标
        df_port_perf_eval = obj_perf_eval["df_port_perf_eval"]
        df_corr = obj_perf_eval["df_corr"]
        port_name = obj_perf_eval["port_name"] 
        
        ### set weight dict 
        # dict_weight["weight_period"]["long"]= 0.4  ; dict_weight["weight_indi"]["ret_end"]
        dict_weight = obj_perf_eval["dict_weight"]

        ### create dcit for specific indicators
        # dict_weight_indi[ "ret_end" + "_" + "long" ] = 0.4 * 0.3
        # 4，15%，越大越好，低点开始最大涨幅, "ret_fromlow_long","ret_fromlow_mid"
        # dict_weight_indi[ "mdd_fromhigh" + "_" + "long" ] = 0.15 * 0.3
        dict_weight_indi = {}
        
        #######################################################################
        ### 根据权重计算skill_ret 
        # 新建column "skill_ret"
        # df_port_perf_eval[ "skill_ret" ] = 0.0 
        df_port_perf_eval.loc["diff_pct_abs", "skill_ret" ] = 0.0
        df_port_perf_eval.loc["diff_pct_semi_abs", "skill_ret" ] = 0.0
        # 参考 def algo_port_weight_by_indicator in algo_opt.py
        #  ["long","mid","short" ]

        for temp_key in dict_weight["weight_period"].keys(): 
            # ["ret_end","ret_fromlow","mdd" ,"mdd_fromhigh"]
            for temp_indi in dict_weight["weight_indi"].keys(): 
                ### 设置指标和指标的权重
                temp_indi_len = temp_indi + "_" + temp_key
                temp_weight = dict_weight["weight_period"][temp_key] *  dict_weight["weight_indi"][temp_indi]
                # 0.4 * 0.3
                dict_weight_indi[ temp_indi_len ] = temp_weight
                
                ### 获得目标组合对应的值
                temp_value = df_port_perf_eval.loc[port_name, temp_indi_len ] 
                ### 这时候不需要和所有组合比较，只需要比较目标组合和模拟组合在不同指标直接的差异
                # 计算方法：对单一指标和全部指标，分别计算偏离绝对值、正偏离，在按照指标权重加权求和。
                ### 默认存在 port_simu
                temp_value_simu = df_port_perf_eval.loc["port_simu", temp_indi_len ] 
                # print("Debug=== ",temp_indi_len , temp_value,temp_value_simu )
                ### 偏离率 diff_pct 越小越好
                if temp_value == 0.0 :
                    diff_pct = 0.0
                    diff_pct_semi = 0.0 
                else :
                    diff_pct = (temp_value_simu - temp_value)/temp_value
                    ### 偏离率不超过-1 or +1
                    diff_pct =min( max(diff_pct ,-1),1 )

                    ### 收益率差值越大越好，只考虑负偏离;最大回撤差值越小越好
                    if "ret" in temp_indi :
                        diff_pct_semi = min(diff_pct, 0.0 )
                    if "mdd" in temp_indi :
                        diff_pct_semi = max(diff_pct, 0.0 )
                    # print("Debug===diff_pct_semi",diff_pct_semi , diff_pct)

                ### 简单绝对值百分比,为了求和需要用绝对值
                df_port_perf_eval.loc["diff_pct_abs", temp_indi_len ] = abs(diff_pct)
                df_port_perf_eval.loc["diff_pct_abs", "skill_ret" ] = abs(diff_pct)* temp_weight + df_port_perf_eval.loc["diff_pct_abs", "skill_ret" ] 
                ### 方向性百分比，即收益率只考虑负偏离、最大回撤只考虑正偏离
                df_port_perf_eval.loc["diff_pct_semi_abs", temp_indi_len ] = abs(diff_pct_semi)
                df_port_perf_eval.loc["diff_pct_semi_abs", "skill_ret" ] = abs(diff_pct_semi) * temp_weight + df_port_perf_eval.loc["diff_pct_semi_abs", "skill_ret" ] 

        df_port_perf_eval.to_csv("D:\\df_port_perf_eval_skill.csv") 
        # notes:skill_ret 指标越小越好
        ### save to output 
        obj_perf_eval["dict_weight_indi"] = dict_weight_indi
        obj_perf_eval["df_port_perf_eval"] = df_port_perf_eval

        return obj_perf_eval

       
    def perf_eval_port_ret_N(self,obj_port  ) :
        ### 评估多个组合日期收益率序列与不同基准的相关性、累计收益、最大回撤、 Alpha、Sharpe、Sortino、Calmar等
        # derived from perf_eval_port_ret()
        # notes:1,收益率应当是小数，例如 0.015，而不是百分比 1.5
        ########################################
        ### 日期列表
        date_list = obj_port["dict"]["date_list"]
        date_list.sort()
        ### 获取日期长度 1/3，2/3的长度，因为1个月有21~28不等的交易日，2个月交易日42~56，3个月交易日直接用date_list长度
        len_1_3 = int( len(date_list)*0.3333 ) 
        len_2_3 = int( len(date_list)*0.6666 ) 

        ### 2, df_port_unit的index列是不同的基金组合或市场、行业、主题分组，columns是升序排列的日期
        df_port_unit = obj_port["df_port_unit"]
        ### 此时 df_port_unit.columns 可能由日期和业绩指标构成，需要仅仅提取日期部分。
        df_port_unit = df_port_unit.loc[:,  date_list  ] 
        ############################################
        ### 如果有2+的index值一样，那么用drop会同时删去所有相同的index
        ### notes：股票组合里因为各种原因，可能出现重复组合。
        count_port = 0 
        df_port_unit_temp = pd.DataFrame( columns=df_port_unit.columns )
        port_list_temp = list( df_port_unit.index.drop_duplicates() )
        if len(df_port_unit.index ) > len( port_list_temp ) :
            ### 说明有重复的组合
            for temp_port in port_list_temp :
                df_temp = df_port_unit[ df_port_unit.index == temp_port ]
                ### 无论df_temp有几行一样，都只要第一行
                df_port_unit_temp = df_port_unit_temp.append( df_temp.iloc[0,:] )
            ### 都弄完了以后，更新 df_port_unit
            df_port_unit = df_port_unit_temp 
        
        # print("Debug==" ,len(df_port_unit.index ) , len(df_port_unit_temp.index ) , len( port_list_temp ) )

        ########################################
        ### 需要确定市场基础组合 :一般用大市值组合，"port_w_mv_large" | 
        port_benchmark = obj_port["dict"]["port_benchmark"] 

        ########################################
        ### 日期升序排列,转置后每一列是一个组合的日期序列,index=date,columns=port 
        df_unit_date_port = df_port_unit.T

        ### 新建组合业绩 df_perf_eval
        df_perf_eval = pd.DataFrame( index=df_port_unit.index  )

        #################################################################################
        ### Part 1 累计收益、最大回撤、波动率、相对累计收益、相对最大回撤
        import math
        ##################################################################################
        ### Step 1 计算近30天、60天、90天收益率 | notes: date_list是升序排列
        col_list = ["ret_last_short","ret_last_mid","ret_last_long"]
        df_port_unit.loc[:,"ret_last_short" ] = df_port_unit.loc[:, date_list[-1] ] /df_port_unit.loc[:, date_list[-1*len_1_3] ] -1
        df_port_unit.loc[:,"ret_last_mid" ] = df_port_unit.loc[:, date_list[-1] ] /df_port_unit.loc[:, date_list[-1*len_2_3] ] -1 
        df_port_unit.loc[:,"ret_last_long" ] = df_port_unit.loc[:, date_list[-1] ] /df_port_unit.loc[:, date_list[ 0] ] -1

        ##################################################################################
        ### Step 2 计算近20天、60天、90天最大回撤 | notes: date_list是升序排列
        str_list = ["short","mid","long"]
        for str1 in str_list  :
            ### add to col_list
            col_list = col_list + [  "mdd_last_" + str1   ]
            col_list = col_list + [  "vol_last_" + str1  ]
            col_list = col_list + [ "vol_relative_last_" + str1 ]
            col_list = col_list +["alpha_last_" + str1, "alpha_annual_last_" + str1  ]
            col_list = col_list +[ "sharp_annual_last_" + str1  ]
            col_list = col_list +[ "sortino_annual_last_" + str1  ]
            col_list = col_list +[ "calmar_annual_last_" + str1  ]

        len_list = [ len_1_3, len_2_3, len(date_list) ]
        for temp_i in [ 0,1,2 ]:
            ###########################################
            ### 
            n_days =len_list[ temp_i ]
            temp_str = str_list[temp_i ] 
            ### 区间最大回撤：30,60,90天 | index=date,columns=port 
            df_temp = df_unit_date_port.iloc[ -1*n_days:, :  ]
            # benchmark,基准组合
            temp_ret_series_bm = df_temp[ port_benchmark  ] / df_temp[port_benchmark ].values[0] -1 

            for temp_col in df_temp.columns :   
                ### temp_col 是一个一个的组合名称
                ### 累计最大值 
                ### 出现过报错，因此先改数据类型：TypeError: '<' not supported between instances of 'str' and 'int'
                ### 主要原因是出现2个基金代码，1个是历史导入数据已有的基金代码，另1个是append加上的基金代码 
                df_temp["max_" +temp_col ] = df_temp[ temp_col ].expanding().max() 
                ### 负值最大回撤,每一个交易日,Notes:这里指的是当日收盘价相对于期初第一行以来最大值的回撤，对应的是当期的值 
                df_temp["mdd_" +temp_col] = ( df_temp[ temp_col ] -  df_temp["max_" +temp_col] ) / df_temp["max_" +temp_col ]  

                ### 赋值给 df_port_unit.loc[:,"mdd_last_long" ]
                # Notes:"mdd_" +temp_col指的是当日收盘价相对于期初第一行以来最大值的回撤，对应的是当期的值 | 由于是负值，因此用 .min()
                df_port_unit.loc[ temp_col ,"mdd_last_" + temp_str ] = df_temp["mdd_" +temp_col].min() 

                
                ###########################################
                ### Step 3 区间波动率 
                ### 组合日收益率 | notes：用净值或涨跌幅算std() 结果是一样的
                temp_ret_series_port = df_temp[temp_col] / df_temp[temp_col].values[0] -1       
                df_port_unit.loc[ temp_col ,"vol_last_" + temp_str ] = temp_ret_series_port.std()

                ##################################################################################
                ### Step 4 相对累计收益、相对最大回撤、相对波动率 || 需要分别减去市场基准和行业基准 
                # 如果所有人都减去同一个组合，那还有什么意义呢？只有相对波动率可以搞一下
                ### 相对波动率
                temp_vol_relative = ( temp_ret_series_port - temp_ret_series_bm   ).std()
                if temp_vol_relative == 0.0 :
                    ### 如果组合是市场组合，则设置一个极小的值
                    temp_vol_relative = 0.0001
                df_port_unit.loc[ temp_col ,"vol_relative_last_" + temp_str ] = temp_vol_relative           
                
                #################################################################################
                ### Part 2 Alpha、Sharpe、Sortino、Calmar
                ##################################################################################
                ### Step 1 Alpha : 超额收益,年化超额收益
                ret_bm = df_port_unit.loc[port_benchmark, date_list[-1] ] /df_port_unit.loc[port_benchmark, date_list[-1* int(n_days) ] ] -1 
                ret_relative  =df_port_unit.loc[temp_col, date_list[-1] ] /df_port_unit.loc[temp_col, date_list[-1* int(n_days) ] ] -1 - ret_bm 
                df_port_unit.loc[temp_col ,"alpha_last_" + temp_str ] = ret_relative 

                # 年化超额收益
                ret_relative_annual = math.sqrt(252/int(n_days) ) * ret_relative 
                df_port_unit.loc[temp_col ,"alpha_annual_last_" + temp_str ] = ret_relative_annual 
                
                ##################################################################################
                ### Step 2 Sharpe : math.sqrt(252)*日超额收益均值/日超额收益波动率 
                temp_ret_series_relative = temp_ret_series_port  - temp_ret_series_bm                 
                df_port_unit.loc[temp_col ,"sharp_annual_last_" + temp_str ] = math.sqrt(252 ) * temp_ret_series_relative.mean()/ temp_vol_relative                

                ##################################################################################
                ### Step 3 Sortino、Calmar 
                ### 保留负值收益率
                temp_ret_series_port_negative = temp_ret_series_port.apply( lambda x : x if x < 0 else -0.0001 ) 
                ### Sortino比率=超额回报率的均值/价格的回落风险值
                temp_std = temp_ret_series_port_negative.std()
                if temp_std > 0 :
                    df_port_unit.loc[temp_col ,"sortino_annual_last_" + temp_str ] = math.sqrt(252 ) * temp_ret_series_port.mean()/ temp_std
                

                ### Calmar 基金的收益率与基金阶段最大回撤的比率 | 感觉calmar不适合高波动的股票基金，特别适合筛选掉回撤大的债基。
                ### notes：最大回撤需要取正值
                #例子1：易方达稳健收益，易方达明星基金经理胡剑的精心作品，从2012年开始管理，业绩年化11.07%，最大回撤6.36%，Calmar比率1.74
                # 上投摩根新兴动力，明星基金经理杜猛管理的一只股票型基金，年化收益率13%，但是回撤巨大，但是回撤巨大，最大回撤54%，Calmar比率只有比率只有0.25
                # print("Debug mdd_last_ " ,  df_port_unit.loc[ temp_col ,"mdd_last_" + temp_str ]   )
                temp_value = df_port_unit.loc[temp_col ,"ret_last_" + temp_str ]  / abs( df_port_unit.loc[ temp_col ,"mdd_last_" + temp_str ] ) 
                df_port_unit.loc[temp_col ,"calmar_annual_last_" + temp_str ] = math.sqrt(252/int(n_days) ) * temp_value 


        #######################################################################
        ### Debug save to excel  
        print("col_list:",col_list )
        # df_port_unit.to_excel("D:\\df_port_unit.xlsx") 
        df_perf_eval = df_port_unit.loc[:, col_list   ]
        # df_perf_eval.to_excel("D:\\df_perf_eval.xlsx")

        
        ### save to output
        obj_port["df_port_unit"] = df_port_unit
        obj_port["col_list"] = col_list
        obj_port["df_perf_eval"] = df_port_unit.loc[:, col_list   ]


        return obj_port










#######################################################################
### 1.1.7，class perf_eval_ashare_deriv_cn：子类：中国权益衍生品
class perf_eval_ashare_deriv_cn():
    def __init__(self):
        #######################################################################
        ### 导入配置文件对象，例如path_db_wind等
        sys.path.append(path_ciss_rc+ "config\\")
        from config_data import config_data_factor_model
        config_data_1 = config_data_factor_model()
        self.obj_config = config_data_1.obj_config
        ### factor_model 目录位置：
        # self.obj_config["dict"]["path_factor_model"] = dict_config["path_ciss_db"] +"factor_model\\"
        

#######################################################################
### 子类：中国债券
class perf_eval_bond_cn():
    def __init__(self):
        #######################################################################
        ### 导入配置文件对象，例如path_db_wind等
        sys.path.append(path_ciss_rc+ "config\\")
        from config_data import config_data_factor_model
        config_data_1 = config_data_factor_model()
        self.obj_config = config_data_1.obj_config
        ### factor_model 目录位置：
        # self.obj_config["dict"]["path_factor_model"] = dict_config["path_ciss_db"] +"factor_model\\"
        





