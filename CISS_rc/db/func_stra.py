# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
===============================================

last 230427 | since 181109

功能： 
1，class stra_ashare_timing_monitor():动量策略abcd3d
2，class stra_weighting_score；打分加权策略
3，class stra_allocation():给定指标，计算配置权重

分析：0，策略算法开发流程： 假设 --》 方法论，模型 --》 
    
3,output file 
    1,head json file of portfolio
    2,portfolio dataframe of portfolio 
    3,stockpool dataframe of portfolio

Notes: 
refernce: rC_Stra_MAX.py 
===============================================
'''
import sys,os
import pandas as pd
import numpy as np
import json
import time
import datetime as dt

### 数据地址都迁移至 
# path0 = "C:\\rc_HUARONG\\rc_HUARONG\\"
# path_ciss_web = path0+ "ciss_web\\"
# path_ciss_rc = path_ciss_web +"CISS_rc\\"
# sys.path.append(path_ciss_rc + "config\\" )
# sys.path.append(path_ciss_rc + "db\\" )
# sys.path.append(path_ciss_rc + "db\\db_assets\\" )
# sys.path.append(path_ciss_rc + "db\\data_io\\" )


class functions():
    def __init__(self, func_name="" ):
        self.func_name = func_name

class strategies():
    def __init__(self, stra_name="" ):
        self.stra_name = stra_name

    # 策略流程应该是 输入信息管理，计算过程管理(可能涉及专属的策略模型)，输出信息。

##################################################################
class stra_ashare_timing_monitor():
    def __init__(self ):
        ### 继承父类indicators的定义，等价于
        strategies.__init__(self)

        ### 市场动量和择时策略、主要用于监控和跟踪市场状态

    def print_info(self):
        print("stra_timing_abcd3d_s |短周期参数：择时策略计算市场的四种状态和各自的3个阶段 ")
        print("stra_timing_abcd3d_l |长周期参数：择时策略计算市场的四种状态和各自的3个阶段 ")
        
        
    def stra_timing_abcd3d_s(self,obj_data) :
        ### 短周期参数：择时策略计算市场的四种状态和各自的3个阶段
        '''
        ma_list = [16,40,100]
        定义策略：
        价格所处状态：
        上涨：ma16_up>=0 and p_ma40>=0 ; 
        上涨后震荡:ma16_up<0 and p_ma40>=0 ; 
        下跌:ma16_up<0 and p_ma40<0 ; 
        下跌后震荡:ma16_up>=0 and p_ma40<0 ; 

        3阶段，用股价涨幅分：
        上涨：p-ma40:0~10%，10~20%，20%~
        上涨后震荡:p-ma16 : >-0.03, -0.08~-0.03, -0.12~-0.08
        下跌:p-ma40: -0.05~ -0.15, -0.15~-0.25 ，-0.25%~
        下跌后震荡: p-ma16 : <0.03, 0.03~-0.08, 0.08~-0.12

        功能：1，对所有个股、
        2，对个股的行业聚类（流通市值和成交额），计算单个行业所处状态和阶段
        2，对市场不同分组（例如流通市值加权top300、500、1000，创业板股）计算市场组合所处状态和阶段

        input:obj_data
        
        notes:obj_data来源于 data_io.py,因均线数据比较容易计算不需要通过 analysis_indicators.py
        '''
        df_eod_prices = obj_data["df_mom_eod_prices"]
        # abcd3d数值越高预期收益越好，
        # value of abcd3d range from up=6,5,4;up-flat=3,2,1;down=-6,-5,-4;down_flat= -3,-2,-1
        df_eod_prices["abcd3d"] = 0

        # default list values: 16,40,100
        para_ma_s_0 = str(obj_data["dict"]["para_ma_short"][0] )
        para_ma_s_1 = str(obj_data["dict"]["para_ma_short"][1] )
        para_ma_s_2 = str(obj_data["dict"]["para_ma_short"][2] )
        # para for MA up,例如 0.0005 
        para_ma_up_0 =  obj_data["dict"]["para_ma_up"][0]  
        # para for p>= MA,例如 0.001
        para_p_ma_1 =  obj_data["dict"]["para_p_ma"][1]  
        # print( "para_ma_up_0 ", para_ma_up_0 ,"para_p_ma_1 ",para_p_ma_1  ) 
        ### notes:有出现过已有的分析csv文件里是空的，或行业分类csv文件不包括全部A股。
        print("length of index and columns ", len(df_eod_prices.index),len( df_eod_prices.columns )  )
        df_eod_prices["indi_short"] = (df_eod_prices["ma_s_"+para_ma_s_0] - df_eod_prices["ma_s_pre_"+para_ma_s_0])/df_eod_prices["ma_s_pre_"+para_ma_s_0] 
        df_eod_prices["indi_mid"] = ( df_eod_prices["S_DQ_ADJPRECLOSE"] +df_eod_prices["S_DQ_ADJCLOSE"] -2*df_eod_prices["ma_s_"+para_ma_s_1]) / ( 2*df_eod_prices["ma_s_"+para_ma_s_1] ) 

        for temp_i in df_eod_prices.index :
            temp_indi_short = df_eod_prices.loc[temp_i, "indi_short"] - para_ma_up_0
            temp_indi_mid = df_eod_prices.loc[temp_i, "indi_mid"] - para_p_ma_1
            if temp_indi_mid >= 0.0 :
                # a,b
                if temp_indi_short >= 0.0 :
                    df_eod_prices.loc[temp_i, "abcd3d"] = 6
                    ### 看股价和中期均价比较：上涨：p-ma40:0~10%，10~20%，20%~
                    if df_eod_prices.loc[temp_i,"indi_mid"] >= 0.1 and df_eod_prices.loc[temp_i,"indi_mid"] < 0.2 :
                        df_eod_prices.loc[temp_i, "abcd3d"] = 5
                    elif df_eod_prices.loc[temp_i,"indi_mid"] >= 0.2 :
                        df_eod_prices.loc[temp_i, "abcd3d"] = 4
                else :
                    df_eod_prices.loc[temp_i, "abcd3d"] = 3
                    ### 看股价和短期均价比较：上涨后震荡:p-ma16 : >-0.03, -0.08~-0.03, -0.12~-0.08
                    temp_value =  ( df_eod_prices.loc[temp_i,"S_DQ_ADJPRECLOSE"] +df_eod_prices.loc[temp_i,"S_DQ_ADJCLOSE"] -2*df_eod_prices.loc[temp_i,"ma_s_"+para_ma_s_1]) / ( 2*df_eod_prices.loc[temp_i,"ma_s_"+para_ma_s_1] ) 
                    if temp_value >= -0.08 and temp_value < -0.03 :
                        df_eod_prices.loc[temp_i, "abcd3d"] = 2
                    elif temp_value < -0.08 :
                        df_eod_prices.loc[temp_i, "abcd3d"] = 1
            else :
                # c,d
                if temp_indi_short < 0.0 :
                    df_eod_prices.loc[temp_i, "abcd3d"] = -6
                    ### 看股价和中期均价比较：下跌:p-ma40: <-0.05 -0.05~ -0.15，-0.15%< ~
                    if df_eod_prices.loc[temp_i,"indi_mid"] >= -0.15 and df_eod_prices.loc[temp_i,"indi_mid"] <= -0.05 :
                        df_eod_prices.loc[temp_i, "abcd3d"] = -5
                    elif df_eod_prices.loc[temp_i,"indi_mid"] < -0.15 :
                        df_eod_prices.loc[temp_i, "abcd3d"] = -4                   

                else :
                    df_eod_prices.loc[temp_i, "abcd3d"] = -3
                    ### 下跌后震荡: p-ma16 : <0.03, 0.03~0.08, 0.08~-0.12
                    temp_value =  ( df_eod_prices.loc[temp_i,"S_DQ_ADJPRECLOSE"] +df_eod_prices.loc[temp_i,"S_DQ_ADJCLOSE"] -2*df_eod_prices.loc[temp_i,"ma_s_"+para_ma_s_1]) / ( 2*df_eod_prices.loc[temp_i,"ma_s_"+para_ma_s_1] ) 
                    if temp_value >= 0.03 and temp_value < 0.08 :
                        df_eod_prices.loc[temp_i, "abcd3d"] = -2
                    elif temp_value > 0.08 :
                        df_eod_prices.loc[temp_i, "abcd3d"] = -1

        obj_data["df_mom_eod_prices"] = df_eod_prices
        obj_data["dict"]["col_list_stra"] = ["indi_short","indi_mid", "abcd3d"]


        return obj_data

    def stra_timing_abcd3d_l(self,obj_data) :
        ### 长周期参数：择时策略计算市场的四种状态和各自的3个阶段
        '''ma_list = [40,100,250]'''

        return obj_data

##################################################################
class stra_weighting_score():
    def __init__(self  ):
        ### 继承父类indicators的定义，等价于
        strategies.__init__(self)
        # generate allocation weights for portfolio assets 
<<<<<<< HEAD

    def print_info(self):
        print("打分加权计算组合配置 ")
        print("cal_weight_indicator_score | 根据参数和指标列表，计算单指标和多指标打分加权 ")
        print(" ")
        
    
    def cal_weight_indicator_score(self,obj_indi) :
        # 根据参数和指标列表，计算单指标和多指标打分加权  
        # notes:col_list_fi_hist指标的标准分是带 "_mad"后缀
        df_stra = obj_indi["df_indi"] 
        col_list_fi_hist = obj_indi["col_list"]

        ### 负面剔除：todo 

        # ranking method 1 
        # def func1(x): 
        #     if x >= temp_level :
        #         return 1 
        #     else :
        #         return 0

        ### 排序方程：
        def func_weight(df_stra,temp_col) :
            ### 对df里的列col，取数值在1~100的，计算权重
            df_stra[ temp_col+"_mad_weight"] =df_stra[ temp_col+"_mad_if_choose"].apply(lambda x : 1/x if x<=100 else 0.0 )
            df_stra[ temp_col+"_mad_weight"] = df_stra[ temp_col+"_mad_weight"]/df_stra[ temp_col+"_mad_weight"].sum()                    
            return df_stra
        
        count = 0 
        ### 对单个指标取前100名，按
        for temp_col in col_list_fi_hist :
            # notes：务必保证指标都是越大越好
            ### ranking method 1 ,取值1 or 0 
            # df_stra = df_stra.sort_values(by=temp_col+"_mad",decending=True)
            # # 取前100名的值 temp_level 
            # temp_i = df_stra.index[100]
            # temp_level = df_stra.loc[temp_i,  temp_col+"_mad" ]
            # print("temp index ",temp_i) 
            # df_stra[ temp_col+"_mad_if_choose"] = df_stra[ temp_col+"_mad"].apply(func1)

            ### ranking method 2 ;取值：1,2,3，...
            df_stra[ temp_col+"_mad_if_choose"] = df_stra[ temp_col+"_mad"].rank(method='first')
            # 根据排序前100名的加权 ,返回 temp_col+"_mad_weight"
            df_stra = func_weight(df_stra, temp_col )

            if count == 0 :
                df_stra[ "sum_mad"] = df_stra[ temp_col+"_mad"]
                count =1 
            else :
                df_stra[ "sum_mad"] =df_stra[ "sum_mad"] + df_stra[ temp_col+"_mad"]
            
        ### 对所有指标取前100名，按
        # 对总分进行排序
        df_stra[ "sum_mad_if_choose"] = df_stra[ "sum_mad"].rank(method='first')
        df_stra = func_weight(df_stra, "sum" )

        # df 文件保存在 df_stra 里
        return df_stra

=======

    def print_info(self):
        print("打分加权计算组合配置 ")
        print("cal_weight_indicator_score | 根据参数和指标列表，计算单指标和多指标打分加权 ")
        print(" ")
        
    
    def cal_weight_indicator_score(self,obj_indi) :
        # 根据参数和指标列表，计算单指标和多指标打分加权  
        # notes:col_list_fi_hist指标的标准分是带 "_mad"后缀
        df_stra = obj_indi["df_indi"] 
        col_list_fi_hist = obj_indi["col_list"]

        ### 负面剔除：todo 

        # ranking method 1 
        # def func1(x): 
        #     if x >= temp_level :
        #         return 1 
        #     else :
        #         return 0

        ### 排序方程：
        def func_weight(df_stra,temp_col) :
            ### 对df里的列col，取数值在1~100的，计算权重
            df_stra[ temp_col+"_mad_weight"] =df_stra[ temp_col+"_mad_if_choose"].apply(lambda x : 1/x if x<=100 else 0.0 )
            df_stra[ temp_col+"_mad_weight"] = df_stra[ temp_col+"_mad_weight"]/df_stra[ temp_col+"_mad_weight"].sum()                    
            return df_stra
        
        count = 0 
        ### 对单个指标取前100名，按
        for temp_col in col_list_fi_hist :
            # notes：务必保证指标都是越大越好
            ### ranking method 1 ,取值1 or 0 
            # df_stra = df_stra.sort_values(by=temp_col+"_mad",decending=True)
            # # 取前100名的值 temp_level 
            # temp_i = df_stra.index[100]
            # temp_level = df_stra.loc[temp_i,  temp_col+"_mad" ]
            # print("temp index ",temp_i) 
            # df_stra[ temp_col+"_mad_if_choose"] = df_stra[ temp_col+"_mad"].apply(func1)

            ### ranking method 2 ;取值：1,2,3，...
            df_stra[ temp_col+"_mad_if_choose"] = df_stra[ temp_col+"_mad"].rank(method='first')
            # 根据排序前100名的加权 ,返回 temp_col+"_mad_weight"
            df_stra = func_weight(df_stra, temp_col )

            if count == 0 :
                df_stra[ "sum_mad"] = df_stra[ temp_col+"_mad"]
                count =1 
            else :
                df_stra[ "sum_mad"] =df_stra[ "sum_mad"] + df_stra[ temp_col+"_mad"]
            
        ### 对所有指标取前100名，按
        # 对总分进行排序
        df_stra[ "sum_mad_if_choose"] = df_stra[ "sum_mad"].rank(method='first')
        df_stra = func_weight(df_stra, "sum" )

        # df 文件保存在 df_stra 里
        return df_stra

>>>>>>> 8edd3e5... update for changes 20230729 to 20230902
##################################################################
class stra_allocation():
    def __init__(self  ):
        ### 继承父类indicators的定义，等价于
        strategies.__init__(self)
        # generate allocation weights for portfolio assets 
        #########################################################
        ###  
        self.nan = np.nan 
              
        ### 导入配置文件
        from config_data import config_data
        config_data_1 = config_data() 
        ### 
        # self.file= "pms_manage.xlsx"
        # self.path0 = "C:\\rc_HUARONG\\rc_HUARONG\\"
        # self.path = "C:\\rc_HUARONG\\rc_HUARONG\\data_pms\\"
        self.path = config_data_1.obj_config["dict"]["path_data_pms"] 
        self.path_wpf = self.path + "wpf\\" 
        self.path_data = self.path + "wind_terminal\\" 
        self.path_adj = self.path +  "data_adj\\" 
        ### 策略组合文件
        self.path_stra = config_data_1.obj_config["dict"]["path_stra"] 
        
        self.path_pms_fund = self.path + "fund\\"

    def print_info(self):
        ##################################################################
        ### 主要是确保单券权重位于 0.3~10%之间
        print("stock_weights_by_indi | 给定指标，生成组合权重 ")
        print("stock_weights_by_active | 给定股票池，计算主观股票权重 ")
        print("stock_weights_by_active_sql | 给定sql表中股票池权重，计算股票行业研究等主观股票策略的权重 ")

        print("fund_weights_by_score | 给定得分，计算基金配置权重 ")
        ##################################################################
        ### before 2019-11
        print("stock_weights |给定指标，计算配置权重 ")
        print("stock_weights_etf| 生成etf组合权重 ")
        ### 标准化工具
        print("cal_5levels | 给定column，自动划分5档权重 ")
        print("cal_score_gpd | 给定column，去除极端值并计算百分比分数")
<<<<<<< HEAD

    
    def stock_weights_by_indi(self,obj_shares ) :
        ### 给定指标，生成组合权重
        # derived from def cal_stockpool_indi,stockpools.py
        date_latest = obj_shares["date_latest"] 
        df_port = obj_shares["df_stockpool"]  
        # col_list= obj_shares["col_list"] 
        
        ##################################################################
        ### 选出不超过50只个股;单只个股最大权重不超过10%
        num_max = 50 

        ##################################################################
        ### Part1 按指标标准化：除以算术总值的百分比，或剔除极端值
        ########################################## 
        ### notes:只有市值和成交额需要log
        col_list =  ["m_ave_amt","m_ave_mv","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率","trend_mid","trend_short"   ]
        col_list_log =  ["m_ave_amt","m_ave_mv","归母净利润同比增长率"  ]
        
        for temp_col in col_list :
            ### notes：如果小于1，log后会变成 负值
            df_port = self.cal_score_gpd( df_port, temp_col)
        
        ##################################################################
        ### Part2 对不同指标配置权重
        df_port["s_sum"] = df_port["s_"+ "m_ave_amt" ] *0.2
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "m_ave_mv" ] *0.2
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "基金持股比例" ] *0.2
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "净资产收益率(TTM)" ] *0.2
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "归母净利润同比增长率" ] *0.2
        ### 感觉中期趋势不能作为配置权重地依据，反而应该是股价越低越好
        # df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "trend_mid" ] *0.2
        df_port["s_sum"] =df_port["s_sum"]/df_port["s_sum"].sum()
        df_port = df_port.sort_values(by="s_sum",ascending=False )

        ##################################################################
        ### 选出不超过50只个股;单只个股最大权重不超过10%
        df_port = df_port.iloc[ :num_max , : ]
        df_port[ "weight" ] = df_port[ "s_sum" ]/df_port[ "s_sum" ].sum()
        # 
        df_port["w_diff"] = df_port["weight"].apply(lambda x : x-0.095 if x >0.095 else 0.0 )
        ### 多余的需要分配的权重 temp_sum_diff
        temp_sum_diff = df_port["w_diff"].sum()
        ### 未充分配置的权重 temp_sum
        df_port["temp"] = df_port["weight"].apply(lambda x : x if x <0.085 else 0.0 )
        temp_sum = df_port["temp"].sum()
        para_sum = (temp_sum + temp_sum_diff )/temp_sum
        ### 赋值给个股权重
        df_sub = df_port[ df_port["w_diff"] <0.085 ]
        df_port.loc[ df_sub.index, "weight" ] = df_port.loc[ df_sub.index, "weight" ] * para_sum
        df_port["weight"] = df_port["weight"].apply(lambda x : 0.095 if x >0.095 else x )

        ################################################################## 
        ### RESTRICTION 限制条件：单一行业不超过30%
        df_port_ind = df_port.loc[:,   [ "weight" ] ].groupby( df_port["中信一级行业"] ).sum()
        ### 1,判断大于30%的行业
        df_port_ind["diff"] = df_port_ind["weight"].apply(lambda x : x-0.30 if x >0.3 else 0.0 )
        list_ind = df_port_ind[ df_port_ind["diff"]>0.0 ].index
        ### 2，将多余权重平均配置给小于25%的行业
        ### 多余的需要分配的权重 temp_sum_diff
        temp_sum_diff = df_port_ind["diff"].sum()
        ### 未充分配置的权重 temp_sum
        df_port_ind["temp"] = df_port_ind["weight"].apply(lambda x : x if x <0.25 else 0.0 )
        temp_sum = df_port_ind["temp"].sum()
        para_sum = (temp_sum + temp_sum_diff )/temp_sum
        
        df_port_ind["weight"] = df_port_ind["weight"].apply(lambda x : x*para_sum if x <0.25 else x )
        df_port_ind = df_port_ind.sort_values(by= "weight",ascending=False )
        print( df_port_ind  )        
        ######################################
        ### 赋值给个股权重
        df_sub = df_port[ df_port["中信一级行业"].isin( list_ind ) ]
        df_port.loc[ df_sub.index, "weight" ] = df_port.loc[ df_sub.index, "weight" ] * para_sum
              
        ######################################
        ### object
        df_port["weight"] =df_port["weight"] /df_port["weight"].sum()
        print( df_port.loc[:, ["代码","名称","weight","中信一级行业" ]+ col_list ] )
        
        

        ######################################
        ### save to excel and obj 
        obj_port= obj_shares
        obj_port["df_port"] = df_port
        obj_port["df_port_ind"] = df_port_ind 
        obj_port["date_latest"] =obj_shares["date_latest"]  
        ### define column "code" in df_port
        df_port["code"] = df_port["代码"]
        ### excel file
        file_name = "stra_stock_indi_" + date_latest +".xlsx" 
        df_port.to_excel(self.path_stra + file_name ,index=False ) 
        file_name = "stra_stock_indi.xlsx" 
        df_port.to_excel(self.path_stra + file_name ,index=False ) 
        # obj_port["df_port"] = 
        return obj_port


    def stock_weights_by_active(self, obj_shares ): 
        ### 给定股票池，计算主观股票 
        date_latest = obj_shares["date_latest"] 
        ####################################################################################
        ####################################################################################
        ### 导入股票核心池数据：Excel方式 ： file=pms_manage.xlsx ; sheet=股票池
        # dervied from file=rc_个股推荐行业事件.xlsx
        file_name = "pms_manage.xlsx"
        df_stockpool = pd.read_excel( self.path +file_name,sheet_name="股票池"  )
        ### 只要核心池
        df_port = df_stockpool[ df_stockpool["股票池"]=="核心池" ]
        print(df_port.head().T )
        print(df_port.columns )

        ####################################################################################
        ####################################################################################
        ### 选出不超过60只个股;单只个股最大权重不超过10%
        num_max = 60 

        ##################################################################
        ### Part1 按指标标准化：除以算术总值的百分比，或剔除极端值
        ##########################################
        ### notes:只有市值和成交额需要log
        col_list =  ["配置权重","月均成交额","月均总市值","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率"   ]
        col_list_log =  ["月均成交额","月均总市值","归母净利润同比增长率"  ]
        
        ### Notes：log会导致对极端大值得影响特别小，且普通数值体现不出区分度。
        for temp_col in col_list :
            ### notes：如果小于1，log后会变成 负值
            df_port = self.cal_score_gpd( df_port, temp_col)

        ##################################################################
        ### Part2 对不同指标配置权重
        ### 核心指标："配置权重" 
        df_port["s_sum"] = df_port["s_"+ "配置权重" ] *0.7
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "月均成交额" ] *0.05
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "月均总市值" ] *0.05
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "基金持股比例" ] *0.10
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "净资产收益率(TTM)" ] *0.05
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "归母净利润同比增长率" ] *0.05
        ### 感觉中期趋势不能作为配置权重地依据，反而应该是股价越低越好
        # df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "trend_mid" ] *0.2
        df_port["s_sum"] =df_port["s_sum"]/df_port["s_sum"].sum()
        df_port = df_port.sort_values(by="s_sum",ascending=False )

        ##################################################################
        ### 选出不超过50只个股;单只个股最大权重不超过10%
        df_port = df_port.iloc[:num_max, : ]
        df_port[ "weight" ] = df_port[ "s_sum" ]/df_port[ "s_sum" ].sum()
        # 
        df_port["w_diff"] = df_port["weight"].apply(lambda x : x-0.095 if x >0.095 else 0.0 )
        ### 多余的需要分配的权重 temp_sum_diff
        temp_sum_diff = df_port["w_diff"].sum()
        ### 未充分配置的权重 temp_sum
        df_port["temp"] = df_port["weight"].apply(lambda x : x if x <0.085 else 0.0 )
        temp_sum = df_port["temp"].sum()
        para_sum = (temp_sum + temp_sum_diff )/temp_sum
        ### 赋值给个股权重
        df_sub = df_port[ df_port["w_diff"] <0.085 ]
        df_port.loc[ df_sub.index, "weight" ] = df_port.loc[ df_sub.index, "weight" ] * para_sum
        df_port["weight"] = df_port["weight"].apply(lambda x : 0.095 if x >0.095 else x )

        ################################################################## 
        ### RESTRICTION 限制条件：单一行业不超过30%
        df_port_ind = df_port.loc[:,   [ "weight" ] ].groupby( df_port["中信一级行业"] ).sum()
        ### 1,判断大于30%的行业
        df_port_ind["diff"] = df_port_ind["weight"].apply(lambda x : x-0.30 if x >0.3 else 0.0 )
        list_ind = df_port_ind[ df_port_ind["diff"]>0.0 ].index
        ### 2，将多余权重平均配置给小于25%的行业
        ### 多余的需要分配的权重 temp_sum_diff
        temp_sum_diff = df_port_ind["diff"].sum()
        ### 未充分配置的权重 temp_sum
        df_port_ind["temp"] = df_port_ind["weight"].apply(lambda x : x if x <0.25 else 0.0 )
        temp_sum = df_port_ind["temp"].sum()
        para_sum = (temp_sum + temp_sum_diff )/temp_sum
        
        df_port_ind["weight"] = df_port_ind["weight"].apply(lambda x : x*para_sum if x <0.25 else x )
        df_port_ind = df_port_ind.sort_values(by= "weight",ascending=False )
        print( df_port_ind  )        
        ######################################
        ### 赋值给个股权重
        df_sub = df_port[ df_port["中信一级行业"].isin( list_ind ) ]
        df_port.loc[ df_sub.index, "weight" ] = df_port.loc[ df_sub.index, "weight" ] * para_sum
              
        ######################################
        ### 
        df_port["weight"] =df_port["weight"] /df_port["weight"].sum()
        print( df_port.loc[:, ["代码","名称","weight","中信一级行业" ]+ col_list ] )
        ### save to excel and obj 
        obj_port= obj_shares
        obj_port["df_port"] = df_port
        obj_port["df_port_ind"] = df_port_ind 
        obj_port["date_latest"] =obj_shares["date_latest"]  

        ### define column "code" in df_port
        df_port["code"] = df_port["代码"]
        ### "stra_stockpool_active_"  | "stra_port_active_" 
        file_name = "stra_stockpool_active_" + date_latest +".xlsx" 
        df_port.to_excel(self.path_stra + file_name ,index=False ) 
        file_name = "stra_stockpool_active" +".xlsx" 
        df_port.to_excel(self.path_stra + file_name ,index=False ) 
        # obj_port["df_port"]  
        return obj_port

    def stock_weights_by_active_sql(self, obj_shares ): 
        ### 给定sql表中股票池权重，计算股票行业研究等主观股票策略的权重
        date_latest = obj_shares["date_latest"] 
        ####################################################################################
        ### 数据库方式：db=
        # strategy_CN = request.POST.get("strategy_CN_search","") 
        strategy_CN = "股票行业研究"
        # pool_level = request.POST.get("pool_level_search","")
        pool_level = "core"
        ###############################################
        ### 多个input必须至少有一项非空
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "fundpool_stockpool_weight"
        obj_db["dict_select"] = {}
        obj_db["dict_select"][ "strategy_CN"] = strategy_CN
        obj_db["dict_select"][ "pool_level"] = pool_level

        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.select_table_data( obj_db ) 
        ### obj_db["path_db "] = "C:\\rc_2023\\rc_202X\\ciss_web\\"
        path_db = obj_db["path_db "]
        path_0 = path_db.split("ciss_web")[0] 

        ################################################################################
        ### 股票池调整：如果xxx策略股票池的 xx股票有多条记录，选择日期最新的一条
        df_data = obj_db["df_data"]
        ### step1 按日期降序排列
        df_data = df_data.sort_values(by="date" ,ascending=False )
        ### step2 根据列"code"删除重复项目，只保留第一项【第一项对应了最新的日期】
        df_data = df_data.drop_duplicates( subset=["code"] ,keep="first" )
        ### step3 | 只操作weight权重一列：将nan替换为 -1 ; 部分代码权重为0%或更小，对应了退出股票池
        import numpy as np  
        df_data["weight"] = df_data["weight"].astype("float")
        df_data["weight"] = df_data["weight"].replace(np.nan, -1 ) 
        df_data = df_data[ df_data["weight"] > 0.00001  ] 

        ###################################################
        ### 导入最新的ah股基本面指标数据： 
        file_name = "ah_shares.xlsx"
        path_data_adj= path_0 +"data_pms\\data_adj\\"
        df_temp = pd.read_excel(path_data_adj + file_name)
        col_list_indi = df_temp.columns
        ### df_temp中没有code，"代码"
        df_temp.index = df_temp["代码"]
        df_data.index = df_data["code"]
        code_list = list( df_data["code"] )
        ### 将df_temp对应代码的所有列赋值给 df_data 
        for temp_col in col_list_indi :
            df_data.loc[code_list, temp_col] = df_temp.loc[code_list, temp_col] 
        # df_data.loc[code_list, col_list_indi] = df_temp.loc[code_list, col_list_indi] 

        ########################################################
        ### 取小数点 ：.round(decimals=2)  
        for temp_col in ["市盈率(TTM)","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率"  ] :
            df_data[ temp_col ] = df_data[ temp_col ].round(2)

        for temp_col in ["mv","trend_short","trend_mid","20日涨跌幅","60日涨跌幅","120日涨跌幅","年初至今" ] :
            df_data[ temp_col ] = (df_data[ temp_col ]*100).round(2)


        ###################################################
        ### 再次赋值给 df_port
        df_port = df_data 

        ####################################################################################
        ####################################################################################
        ### 选出不超过60只个股;单只个股最大权重不超过10%
        num_max = 60 

        ##################################################################
        ### Part1 按指标标准化：除以算术总值的百分比，或剔除极端值
        ##########################################
        ### notes:只有市值和成交额需要log
        col_list =  ["weight","m_ave_amt","mv","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率"   ]
        col_list_log =  ["m_ave_amt","mv","归母净利润同比增长率"  ]
        
        ### Notes：log会导致对极端大值得影响特别小，且普通数值体现不出区分度。
        for temp_col in col_list :
            ### notes：如果小于1，log后会变成 负值
            df_port = self.cal_score_gpd( df_port, temp_col)

        ##################################################################
        ### Part2 对不同指标配置权重 weight
        ### 核心指标："配置权重" 
        df_port["s_sum"] = df_port[ "weight" ] *0.8
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "m_ave_amt" ] *0.04
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "mv" ] *0.04
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "基金持股比例" ] *0.02
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "净资产收益率(TTM)" ] *0.5
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "归母净利润同比增长率" ] *0.5
        ### 感觉中期趋势不能作为配置权重地依据，反而应该是股价越低越好
        # df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "trend_mid" ] *0.2
        df_port["s_sum"] =df_port["s_sum"]/df_port["s_sum"].sum()
        df_port = df_port.sort_values(by="s_sum",ascending=False )

        ##################################################################
        ### 选出不超过50只个股;单只个股最大权重不超过10%
        df_port = df_port.iloc[:num_max, : ]
        df_port[ "weight" ] = df_port[ "s_sum" ]/df_port[ "s_sum" ].sum()
        # 
        df_port["w_diff"] = df_port["weight"].apply(lambda x : x-0.095 if x >0.095 else 0.0 )
        ### 多余的需要分配的权重 temp_sum_diff
        temp_sum_diff = df_port["w_diff"].sum()
        ### 未充分配置的权重 temp_sum
        df_port["temp"] = df_port["weight"].apply(lambda x : x if x <0.085 else 0.0 )
        temp_sum = df_port["temp"].sum()
        para_sum = (temp_sum + temp_sum_diff )/temp_sum
        ### 赋值给个股权重
        df_sub = df_port[ df_port["w_diff"] <0.085 ]
        df_port.loc[ df_sub.index, "weight" ] = df_port.loc[ df_sub.index, "weight" ] * para_sum
        df_port["weight"] = df_port["weight"].apply(lambda x : 0.095 if x >0.095 else x )

        ################################################################## 
        ### RESTRICTION 限制条件：单一行业不超过30%
        df_port_ind = df_port.loc[:,   [ "weight" ] ].groupby( df_port["中信一级行业"] ).sum()
        ### 1,判断大于30%的行业
        df_port_ind["diff"] = df_port_ind["weight"].apply(lambda x : x-0.30 if x >0.3 else 0.0 )
        list_ind = df_port_ind[ df_port_ind["diff"]>0.0 ].index
        ### 2，将多余权重平均配置给小于25%的行业
        ### 多余的需要分配的权重 temp_sum_diff
        temp_sum_diff = df_port_ind["diff"].sum()
        ### 未充分配置的权重 temp_sum
        df_port_ind["temp"] = df_port_ind["weight"].apply(lambda x : x if x <0.25 else 0.0 )
        temp_sum = df_port_ind["temp"].sum()
        para_sum = (temp_sum + temp_sum_diff )/temp_sum
        
        df_port_ind["weight"] = df_port_ind["weight"].apply(lambda x : x*para_sum if x <0.25 else x )
        df_port_ind = df_port_ind.sort_values(by= "weight",ascending=False )
        print( df_port_ind  )        
        ######################################
        ### 赋值给个股权重
        df_sub = df_port[ df_port["中信一级行业"].isin( list_ind ) ]
        df_port.loc[ df_sub.index, "weight" ] = df_port.loc[ df_sub.index, "weight" ] * para_sum
              
        ######################################
        ### 
        df_port["weight"] =df_port["weight"] /df_port["weight"].sum()
        print( df_port.loc[:, ["代码","名称","weight","中信一级行业" ]+ col_list ] )
        ### save to excel and obj 
        obj_port= obj_shares
        obj_port["df_port"] = df_port
        obj_port["df_port_ind"] = df_port_ind 
        obj_port["date_latest"] =obj_shares["date_latest"]  

        ### define column "code" in df_port
        df_port["code"] = df_port["代码"]
        ### "stra_stockpool_active_"  | "stra_port_active_" 
        file_name = "stra_stockpool_active_" + date_latest +".xlsx" 
        df_port.to_excel(self.path_stra + file_name ,index=False ) 
        file_name = "stra_stockpool_active" +".xlsx" 
        df_port.to_excel(self.path_stra + file_name ,index=False )  

        # obj_port["df_port"]  
        return obj_port


    def fund_weights_by_score(self, obj_fund) :
        ##################################################################
        ### 给定得分，计算基金配置权重 :主要是确保单券权重位于 0.3~10%之间
        date_latest = obj_fund["date_latest"]        
        file_name = obj_fund["file_name"] 
        ##################################################################
        ### Notes:"s_sum" 必须已经有数值;为了适应不同地策略
        if not "col_name" in obj_fund.keys() :
            col_name =  "s_sum"
            df_port = obj_fund["df_funds"]  
        else :
            col_name = obj_fund["col_name"]
            ### steps：1，导入基金池；file=基金池rc_纯债_20220308.xlsx
            file_input = obj_fund["file_input"]
            df_port = pd.read_excel(self.path_pms_fund+ file_input,sheet_name="raw_data"  )
        
        print(df_port.head().T )
        print(df_port.columns )
        ##################################################################
        ### 1，选出总分前50的基金，按照收益率降序排列；最后不超过20个基金
        df_port = df_port.iloc[:50, : ]
        ###  "收益率（%）" 
        # if "收益率（%）" in df_port.keys() :
        #     df_port = df_port.sort_values(by="收益率（%）",ascending=False )
        ###  
        if "score_收益率" in df_port.keys() :
            df_port = df_port.sort_values(by="score_收益率",ascending=False )        
        num_max = 20         

        ##################################################################
        ### 确保单只基金最大权重不超过10%
        df_port = df_port.iloc[:num_max, : ]
        df_port[ "weight" ] = df_port[ col_name ]/df_port[ col_name ].sum()
        # 
        df_port["w_diff"] = df_port["weight"].apply(lambda x : x-0.095 if x >0.095 else 0.0 )
        ### 多余的需要分配的权重 temp_sum_diff
        temp_sum_diff = df_port["w_diff"].sum()
        ### 未充分配置的权重 temp_sum
        df_port["temp"] = df_port["weight"].apply(lambda x : x if x <0.08 else 0.0 )
        temp_sum = df_port["temp"].sum()
        para_sum = (temp_sum + temp_sum_diff )/temp_sum
        ### 赋值给个股权重
        df_sub = df_port[ df_port["w_diff"] <0.08 ]
        df_port.loc[ df_sub.index, "weight" ] = df_port.loc[ df_sub.index, "weight" ] * para_sum
        df_port["weight"] = df_port["weight"].apply(lambda x : 0.095 if x >0.095 else x )

        df_port["weight"] =df_port["weight"] /df_port["weight"].sum()
        ######################################
        ### save to file 
        ### define column "code" in df_port
        df_port["code"] = df_port["基金代码"]

        # notes： 导出的策略文件名不需要包括 str_purchase = "-开放申购"
        df_port.to_excel( self.path_stra + file_name ,index=False ) 
        ### obj_fund["file_name_output"] = "stra_fundpool_activestock.xlsx" 
        if "file_name_output" in obj_fund.keys():
            df_port.to_excel(self.path_stra + obj_fund["file_name_output"] ,index=False ) 
        
        obj_fund["df_port"] = df_port
        return obj_fund
        

=======

    
    def stock_weights_by_indi(self,obj_shares ) :
        ### 给定指标，生成组合权重
        # derived from def cal_stockpool_indi,stockpools.py
        date_latest = obj_shares["date_latest"] 
        df_port = obj_shares["df_stockpool"]  
        # col_list= obj_shares["col_list"] 
        
        ##################################################################
        ### 选出不超过50只个股;单只个股最大权重不超过10%
        num_max = 50 

        ##################################################################
        ### Part1 按指标标准化：除以算术总值的百分比，或剔除极端值
        ########################################## 
        ### notes:只有市值和成交额需要log
        col_list =  ["m_ave_amt","m_ave_mv","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率","trend_mid","trend_short"   ]
        col_list_log =  ["m_ave_amt","m_ave_mv","归母净利润同比增长率"  ]
        
        for temp_col in col_list :
            ### notes：如果小于1，log后会变成 负值
            df_port = self.cal_score_gpd( df_port, temp_col)
        
        ##################################################################
        ### Part2 对不同指标配置权重
        df_port["s_sum"] = df_port["s_"+ "m_ave_amt" ] *0.2
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "m_ave_mv" ] *0.2
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "基金持股比例" ] *0.2
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "净资产收益率(TTM)" ] *0.2
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "归母净利润同比增长率" ] *0.2
        ### 感觉中期趋势不能作为配置权重地依据，反而应该是股价越低越好
        # df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "trend_mid" ] *0.2
        df_port["s_sum"] =df_port["s_sum"]/df_port["s_sum"].sum()
        df_port = df_port.sort_values(by="s_sum",ascending=False )

        ##################################################################
        ### 选出不超过50只个股;单只个股最大权重不超过10%
        df_port = df_port.iloc[ :num_max , : ]
        df_port[ "weight" ] = df_port[ "s_sum" ]/df_port[ "s_sum" ].sum()
        # 
        df_port["w_diff"] = df_port["weight"].apply(lambda x : x-0.095 if x >0.095 else 0.0 )
        ### 多余的需要分配的权重 temp_sum_diff
        temp_sum_diff = df_port["w_diff"].sum()
        ### 未充分配置的权重 temp_sum
        df_port["temp"] = df_port["weight"].apply(lambda x : x if x <0.085 else 0.0 )
        temp_sum = df_port["temp"].sum()
        para_sum = (temp_sum + temp_sum_diff )/temp_sum
        ### 赋值给个股权重
        df_sub = df_port[ df_port["w_diff"] <0.085 ]
        df_port.loc[ df_sub.index, "weight" ] = df_port.loc[ df_sub.index, "weight" ] * para_sum
        df_port["weight"] = df_port["weight"].apply(lambda x : 0.095 if x >0.095 else x )

        ################################################################## 
        ### RESTRICTION 限制条件：单一行业不超过30%
        df_port_ind = df_port.loc[:,   [ "weight" ] ].groupby( df_port["中信一级行业"] ).sum()
        ### 1,判断大于30%的行业
        df_port_ind["diff"] = df_port_ind["weight"].apply(lambda x : x-0.30 if x >0.3 else 0.0 )
        list_ind = df_port_ind[ df_port_ind["diff"]>0.0 ].index
        ### 2，将多余权重平均配置给小于25%的行业
        ### 多余的需要分配的权重 temp_sum_diff
        temp_sum_diff = df_port_ind["diff"].sum()
        ### 未充分配置的权重 temp_sum
        df_port_ind["temp"] = df_port_ind["weight"].apply(lambda x : x if x <0.25 else 0.0 )
        temp_sum = df_port_ind["temp"].sum()
        para_sum = (temp_sum + temp_sum_diff )/temp_sum
        
        df_port_ind["weight"] = df_port_ind["weight"].apply(lambda x : x*para_sum if x <0.25 else x )
        df_port_ind = df_port_ind.sort_values(by= "weight",ascending=False )
        print( df_port_ind  )        
        ######################################
        ### 赋值给个股权重
        df_sub = df_port[ df_port["中信一级行业"].isin( list_ind ) ]
        df_port.loc[ df_sub.index, "weight" ] = df_port.loc[ df_sub.index, "weight" ] * para_sum
              
        ######################################
        ### object
        df_port["weight"] =df_port["weight"] /df_port["weight"].sum()
        print( df_port.loc[:, ["代码","名称","weight","中信一级行业" ]+ col_list ] )
        
        

        ######################################
        ### save to excel and obj 
        obj_port= obj_shares
        obj_port["df_port"] = df_port
        obj_port["df_port_ind"] = df_port_ind 
        obj_port["date_latest"] =obj_shares["date_latest"]  
        ### define column "code" in df_port
        df_port["code"] = df_port["代码"]
        ### excel file
        file_name = "stra_stock_indi_" + date_latest +".xlsx" 
        df_port.to_excel(self.path_stra + file_name ,index=False ) 
        file_name = "stra_stock_indi.xlsx" 
        df_port.to_excel(self.path_stra + file_name ,index=False ) 
        # obj_port["df_port"] = 
        return obj_port


    def stock_weights_by_active(self, obj_shares ): 
        ### 给定股票池，计算主观股票 
        date_latest = obj_shares["date_latest"] 
        ####################################################################################
        ####################################################################################
        ### 导入股票核心池数据：Excel方式 ： file=pms_manage.xlsx ; sheet=股票池
        # dervied from file=rc_个股推荐行业事件.xlsx
        file_name = "pms_manage.xlsx"
        df_stockpool = pd.read_excel( self.path +file_name,sheet_name="股票池"  )
        ### 只要核心池
        df_port = df_stockpool[ df_stockpool["股票池"]=="核心池" ]
        print(df_port.head().T )
        print(df_port.columns )

        ####################################################################################
        ####################################################################################
        ### 选出不超过60只个股;单只个股最大权重不超过10%
        num_max = 60 

        ##################################################################
        ### Part1 按指标标准化：除以算术总值的百分比，或剔除极端值
        ##########################################
        ### notes:只有市值和成交额需要log
        col_list =  ["配置权重","月均成交额","月均总市值","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率"   ]
        col_list_log =  ["月均成交额","月均总市值","归母净利润同比增长率"  ]
        
        ### Notes：log会导致对极端大值得影响特别小，且普通数值体现不出区分度。
        for temp_col in col_list :
            ### notes：如果小于1，log后会变成 负值
            df_port = self.cal_score_gpd( df_port, temp_col)

        ##################################################################
        ### Part2 对不同指标配置权重
        ### 核心指标："配置权重" 
        df_port["s_sum"] = df_port["s_"+ "配置权重" ] *0.7
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "月均成交额" ] *0.05
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "月均总市值" ] *0.05
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "基金持股比例" ] *0.10
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "净资产收益率(TTM)" ] *0.05
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "归母净利润同比增长率" ] *0.05
        ### 感觉中期趋势不能作为配置权重地依据，反而应该是股价越低越好
        # df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "trend_mid" ] *0.2
        df_port["s_sum"] =df_port["s_sum"]/df_port["s_sum"].sum()
        df_port = df_port.sort_values(by="s_sum",ascending=False )

        ##################################################################
        ### 选出不超过50只个股;单只个股最大权重不超过10%
        df_port = df_port.iloc[:num_max, : ]
        df_port[ "weight" ] = df_port[ "s_sum" ]/df_port[ "s_sum" ].sum()
        # 
        df_port["w_diff"] = df_port["weight"].apply(lambda x : x-0.095 if x >0.095 else 0.0 )
        ### 多余的需要分配的权重 temp_sum_diff
        temp_sum_diff = df_port["w_diff"].sum()
        ### 未充分配置的权重 temp_sum
        df_port["temp"] = df_port["weight"].apply(lambda x : x if x <0.085 else 0.0 )
        temp_sum = df_port["temp"].sum()
        para_sum = (temp_sum + temp_sum_diff )/temp_sum
        ### 赋值给个股权重
        df_sub = df_port[ df_port["w_diff"] <0.085 ]
        df_port.loc[ df_sub.index, "weight" ] = df_port.loc[ df_sub.index, "weight" ] * para_sum
        df_port["weight"] = df_port["weight"].apply(lambda x : 0.095 if x >0.095 else x )

        ################################################################## 
        ### RESTRICTION 限制条件：单一行业不超过30%
        df_port_ind = df_port.loc[:,   [ "weight" ] ].groupby( df_port["中信一级行业"] ).sum()
        ### 1,判断大于30%的行业
        df_port_ind["diff"] = df_port_ind["weight"].apply(lambda x : x-0.30 if x >0.3 else 0.0 )
        list_ind = df_port_ind[ df_port_ind["diff"]>0.0 ].index
        ### 2，将多余权重平均配置给小于25%的行业
        ### 多余的需要分配的权重 temp_sum_diff
        temp_sum_diff = df_port_ind["diff"].sum()
        ### 未充分配置的权重 temp_sum
        df_port_ind["temp"] = df_port_ind["weight"].apply(lambda x : x if x <0.25 else 0.0 )
        temp_sum = df_port_ind["temp"].sum()
        para_sum = (temp_sum + temp_sum_diff )/temp_sum
        
        df_port_ind["weight"] = df_port_ind["weight"].apply(lambda x : x*para_sum if x <0.25 else x )
        df_port_ind = df_port_ind.sort_values(by= "weight",ascending=False )
        print( df_port_ind  )        
        ######################################
        ### 赋值给个股权重
        df_sub = df_port[ df_port["中信一级行业"].isin( list_ind ) ]
        df_port.loc[ df_sub.index, "weight" ] = df_port.loc[ df_sub.index, "weight" ] * para_sum
              
        ######################################
        ### 
        df_port["weight"] =df_port["weight"] /df_port["weight"].sum()
        print( df_port.loc[:, ["代码","名称","weight","中信一级行业" ]+ col_list ] )
        ### save to excel and obj 
        obj_port= obj_shares
        obj_port["df_port"] = df_port
        obj_port["df_port_ind"] = df_port_ind 
        obj_port["date_latest"] =obj_shares["date_latest"]  

        ### define column "code" in df_port
        df_port["code"] = df_port["代码"]
        ### "stra_stockpool_active_"  | "stra_port_active_" 
        file_name = "stra_stockpool_active_" + date_latest +".xlsx" 
        df_port.to_excel(self.path_stra + file_name ,index=False ) 
        file_name = "stra_stockpool_active" +".xlsx" 
        df_port.to_excel(self.path_stra + file_name ,index=False ) 
        # obj_port["df_port"]  
        return obj_port

    def stock_weights_by_active_sql(self, obj_shares ): 
        ### 给定sql表中股票池权重，计算股票行业研究等主观股票策略的权重
        date_latest = obj_shares["date_latest"] 
        ####################################################################################
        ### 数据库方式：db=
        # strategy_CN = request.POST.get("strategy_CN_search","") 
        strategy_CN = "股票行业研究"
        # pool_level = request.POST.get("pool_level_search","")
        
        ###############################################
        ### 多个input必须至少有一项非空
        obj_db = {} 
        obj_db["db_name"] = "db_funda.sqlite3"
        obj_db["table_name"] = "fundpool_stockpool_weight"
        obj_db["dict_select"] = {}
        obj_db["dict_select"][ "strategy_CN"] = strategy_CN
        ### notes:很多股票不再输入"core",因此不要再填写该项 pool_level = "core"
        # obj_db["dict_select"][ "pool_level"] = pool_level

        #############################################
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = db_sqlite1.select_table_data( obj_db ) 
        ### obj_db["path_db "] = "C:\\rc_2023\\rc_202X\\ciss_web\\"
        path_db = obj_db["path_db "]
        path_0 = path_db.split("ciss_web")[0] 

        ################################################################################
        ### 股票池调整：如果xxx策略股票池的 xx股票有多条记录，选择日期最新的一条
        df_data = obj_db["df_data"]
        ### step1 按日期降序排列
        df_data = df_data.sort_values(by="date" ,ascending=False )
        ### step2 根据列"code"删除重复项目，只保留第一项【第一项对应了最新的日期】
        df_data = df_data.drop_duplicates( subset=["code"] ,keep="first" )
        ### step3 | 只操作weight权重一列：将nan替换为 -1 ; 部分代码权重为0%或更小，对应了退出股票池
        import numpy as np  
        df_data["weight"] = df_data["weight"].astype("float")
        df_data["weight"] = df_data["weight"].replace(np.nan, -1 ) 
        df_data = df_data[ df_data["weight"] > 0.00001  ] 

        df_data.to_excel("D:\\temp1.xlsx")
        ###################################################
        ### 导入最新的ah股基本面指标数据： 
        file_name = "ah_shares.xlsx"
        path_data_adj= path_0 +"data_pms\\data_adj\\"
        df_temp = pd.read_excel(path_data_adj + file_name)
        col_list_indi = df_temp.columns
        ### df_temp中没有code，"代码"
        df_temp.index = df_temp["代码"]
        df_data.index = df_data["code"]
        code_list = list( df_data["code"] )
        ### 将df_temp对应代码的所有列赋值给 df_data 
        for temp_col in col_list_indi :
            df_data.loc[code_list, temp_col] = df_temp.loc[code_list, temp_col] 
        # df_data.loc[code_list, col_list_indi] = df_temp.loc[code_list, col_list_indi] 

        ########################################################
        ### 取小数点 ：.round(decimals=2)  
        for temp_col in ["市盈率(TTM)","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率"  ] :
            df_data[ temp_col ] = df_data[ temp_col ].round(2)

        for temp_col in ["mv","trend_short","trend_mid","20日涨跌幅","60日涨跌幅","120日涨跌幅","年初至今" ] :
            df_data[ temp_col ] = (df_data[ temp_col ]*100).round(2)


        ###################################################
        ### 再次赋值给 df_port
        df_port = df_data 

        ####################################################################################
        ####################################################################################
        ### 选出不超过60只个股;单只个股最大权重不超过10%
        num_max = 60 

        ##################################################################
        ### Part1 按指标标准化：除以算术总值的百分比，或剔除极端值
        ##########################################
        ### notes:只有市值和成交额需要log
        col_list =  ["weight","m_ave_amt","mv","基金持股比例","净资产收益率(TTM)","归母净利润同比增长率"   ]
        col_list_log =  ["m_ave_amt","mv","归母净利润同比增长率"  ]
        
        ### Notes：log会导致对极端大值得影响特别小，且普通数值体现不出区分度。
        for temp_col in col_list :
            ### notes：如果小于1，log后会变成 负值
            df_port = self.cal_score_gpd( df_port, temp_col)

        ##################################################################
        ### Part2 对不同指标配置权重 weight
        ### 核心指标："配置权重" 
        df_port["s_sum"] = df_port[ "weight" ] *0.8
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "m_ave_amt" ] *0.04
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "mv" ] *0.04
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "基金持股比例" ] *0.02
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "净资产收益率(TTM)" ] *0.5
        df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "归母净利润同比增长率" ] *0.5
        ### 感觉中期趋势不能作为配置权重地依据，反而应该是股价越低越好
        # df_port["s_sum"] = df_port["s_sum"] + df_port["s_"+ "trend_mid" ] *0.2
        df_port["s_sum"] =df_port["s_sum"]/df_port["s_sum"].sum()
        df_port = df_port.sort_values(by="s_sum",ascending=False )

        ### Debug
        df_data.to_excel("D:\\temp2.xlsx")
        ##################################################################
        ### 选出不超过50只个股;单只个股最大权重不超过10%
        df_port = df_port.iloc[:num_max, : ]
        df_port[ "weight" ] = df_port[ "s_sum" ]/df_port[ "s_sum" ].sum()
        # 
        df_port["w_diff"] = df_port["weight"].apply(lambda x : x-0.095 if x >0.095 else 0.0 )
        ### 多余的需要分配的权重 temp_sum_diff
        temp_sum_diff = df_port["w_diff"].sum()
        ### 未充分配置的权重 temp_sum
        df_port["temp"] = df_port["weight"].apply(lambda x : x if x <0.085 else 0.0 )
        temp_sum = df_port["temp"].sum()
        para_sum = (temp_sum + temp_sum_diff )/temp_sum
        ### 赋值给个股权重
        df_sub = df_port[ df_port["w_diff"] <0.085 ]
        df_port.loc[ df_sub.index, "weight" ] = df_port.loc[ df_sub.index, "weight" ] * para_sum
        df_port["weight"] = df_port["weight"].apply(lambda x : 0.095 if x >0.095 else x )

        ################################################################## 
        ### RESTRICTION 限制条件：单一行业不超过30%
        df_port_ind = df_port.loc[:,   [ "weight" ] ].groupby( df_port["中信一级行业"] ).sum()
        ### 1,判断大于30%的行业
        df_port_ind["diff"] = df_port_ind["weight"].apply(lambda x : x-0.30 if x >0.3 else 0.0 )
        list_ind = df_port_ind[ df_port_ind["diff"]>0.0 ].index
        ### 2，将多余权重平均配置给小于25%的行业
        ### 多余的需要分配的权重 temp_sum_diff
        temp_sum_diff = df_port_ind["diff"].sum()
        ### 未充分配置的权重 temp_sum
        df_port_ind["temp"] = df_port_ind["weight"].apply(lambda x : x if x <0.25 else 0.0 )
        temp_sum = df_port_ind["temp"].sum()
        para_sum = (temp_sum + temp_sum_diff )/temp_sum
        
        df_port_ind["weight"] = df_port_ind["weight"].apply(lambda x : x*para_sum if x <0.25 else x )
        df_port_ind = df_port_ind.sort_values(by= "weight",ascending=False )
        print( df_port_ind  )        
        ######################################
        ### 赋值给个股权重
        df_sub = df_port[ df_port["中信一级行业"].isin( list_ind ) ]
        df_port.loc[ df_sub.index, "weight" ] = df_port.loc[ df_sub.index, "weight" ] * para_sum
              
        ######################################
        ### 
        df_port["weight"] =df_port["weight"] /df_port["weight"].sum()
        print( df_port.loc[:, ["代码","名称","weight","中信一级行业" ]+ col_list ] )
        ### save to excel and obj 
        obj_port= obj_shares
        obj_port["df_port"] = df_port
        obj_port["df_port_ind"] = df_port_ind 
        obj_port["date_latest"] =obj_shares["date_latest"]  

        ### define column "code" in df_port
        df_port["code"] = df_port["代码"]
        ### "stra_stockpool_active_"  | "stra_port_active_" 
        file_name = "stra_stockpool_active_" + date_latest +".xlsx" 
        df_port.to_excel(self.path_stra + file_name ,index=False ) 
        file_name = "stra_stockpool_active" +".xlsx" 
        df_port.to_excel(self.path_stra + file_name ,index=False )  

        # obj_port["df_port"]  
        return obj_port


    def fund_weights_by_score(self, obj_fund) :
        ##################################################################
        ### 给定得分，计算基金配置权重 :主要是确保单券权重位于 0.3~10%之间
        date_latest = obj_fund["date_latest"]        
        file_name = obj_fund["file_name"] 
        ##################################################################
        ### Notes:"s_sum" 必须已经有数值;为了适应不同地策略
        if not "col_name" in obj_fund.keys() :
            col_name =  "s_sum"
            df_port = obj_fund["df_funds"]  
        else :
            col_name = obj_fund["col_name"]
            ### steps：1，导入基金池；file=基金池rc_纯债_20220308.xlsx
            file_input = obj_fund["file_input"]
            df_port = pd.read_excel(self.path_pms_fund+ file_input,sheet_name="raw_data"  )
        
        print(df_port.head().T )
        print(df_port.columns )
        ##################################################################
        ### 1，选出总分前50的基金，按照收益率降序排列；最后不超过20个基金
        df_port = df_port.iloc[:50, : ]
        ###  "收益率（%）" 
        # if "收益率（%）" in df_port.keys() :
        #     df_port = df_port.sort_values(by="收益率（%）",ascending=False )
        ###  
        if "score_收益率" in df_port.keys() :
            df_port = df_port.sort_values(by="score_收益率",ascending=False )        
        num_max = 20         

        ##################################################################
        ### 确保单只基金最大权重不超过10%
        df_port = df_port.iloc[:num_max, : ]
        df_port[ "weight" ] = df_port[ col_name ]/df_port[ col_name ].sum()
        # 
        df_port["w_diff"] = df_port["weight"].apply(lambda x : x-0.095 if x >0.095 else 0.0 )
        ### 多余的需要分配的权重 temp_sum_diff
        temp_sum_diff = df_port["w_diff"].sum()
        ### 未充分配置的权重 temp_sum
        df_port["temp"] = df_port["weight"].apply(lambda x : x if x <0.08 else 0.0 )
        temp_sum = df_port["temp"].sum()
        para_sum = (temp_sum + temp_sum_diff )/temp_sum
        ### 赋值给个股权重
        df_sub = df_port[ df_port["w_diff"] <0.08 ]
        df_port.loc[ df_sub.index, "weight" ] = df_port.loc[ df_sub.index, "weight" ] * para_sum
        df_port["weight"] = df_port["weight"].apply(lambda x : 0.095 if x >0.095 else x )

        df_port["weight"] =df_port["weight"] /df_port["weight"].sum()
        ######################################
        ### save to file 
        ### define column "code" in df_port
        df_port["code"] = df_port["基金代码"]

        # notes： 导出的策略文件名不需要包括 str_purchase = "-开放申购"
        df_port.to_excel( self.path_stra + file_name ,index=False ) 
        ### obj_fund["file_name_output"] = "stra_fundpool_activestock.xlsx" 
        if "file_name_output" in obj_fund.keys():
            df_port.to_excel(self.path_stra + obj_fund["file_name_output"] ,index=False ) 
        
        obj_fund["df_port"] = df_port
        return obj_fund
        

>>>>>>> 8edd3e5... update for changes 20230729 to 20230902
    ##################################################################
    ### before 190412
    def stock_weights(self,ind_level,sty_v_g, sp_df) :
        # 给定指标，计算配置权重 | 190412
        # sty_v_g = 'value' or 'growth'
        # INPUT sp_df
        # OUTPUT: weigh_list
        # weigh_list.columns=['code','ind1_code','ind2_code','ind3_code',col_w_value,col_w_growth]
        
        # print(sp_df.loc[:,['code','ind1_code']].head()  )

        ####################################################################
        ### set column item that we want to filtering | col_name = 'w_allo_'+'growth'+'_ind3'
        
        if not ind_level == "0" :
            col_name = 'w_allo_'+sty_v_g+'_ind'+ind_level
            col_w_value = 'w_allo_value_ind'+ind_level
            col_w_growth = 'w_allo_growth_ind'+ind_level
            weight_list= sp_df.loc[:,['code','ind1_code','ind2_code','ind3_code',col_w_value,col_w_growth]]
            # we want to drop stock with portfolio weight smaller than 0.1%, which means no significant support
            # to portfolio return or risks.
            # when calc 601020, we found 600747 holds 0.1088% at 20140531,we think we want to excule this type of firm
            weight_list= weight_list[ weight_list[col_name ] >= 0.0011 ]       
        else :
            ####################################################################
            ### working on whole market 
            ind_level = "1"
            col_name = 'w_allo_'+sty_v_g+'_ind'+ind_level
            col_w_value = 'w_allo_value_ind'+ind_level
            col_w_growth = 'w_allo_growth_ind'+ind_level
            weight_list= sp_df.loc[:,['code','ind1_code','ind2_code','ind3_code',col_w_value,col_w_growth]]
            # we want to drop stock with portfolio weight smaller than 0.1%, which means no significant support
            # to portfolio return or risks.
            # when calc 601020, we found 600747 holds 0.1088% at 20140531,we think we want to excule this type of firm
            weight_list= weight_list[ weight_list[col_name ] >= 0.0005 ]  
            weight_list[col_name] = weight_list[col_name]/weight_list[col_name].sum()

            # Twice optimze :Drop small weights again 
            weight_list= weight_list[ weight_list[col_name ] >= 0.0005 ]  
            weight_list[col_name] = weight_list[col_name]/weight_list[col_name].sum()
            
            weight_list[col_w_value] = weight_list[col_w_value]/weight_list[col_w_value].sum()
            weight_list[col_w_growth] = weight_list[col_w_growth]/weight_list[col_w_growth].sum()

            # print( weight_list.info() )
            # print( weight_list.head() )
            # print( weight_list[col_w_value].sum() )
            # print( weight_list[col_w_growth].sum() )
            # asd

        ### 根据对利润的配置权重和估值，调整配置权重。 估值是个tricky的问题。
        # 读取最新股本，结合股价，计算出每个股票在t时间的P/E：如果w_allo 70:30, 估值调整后w_mv= 59.3:40.7
        # 如果从效率的角度，似乎应该全部选择每单位市值利润最大的股票 profit/(1rmb mv)。如果从均衡捕捉行业价值的角度，
        # 同时控制个股的风险，那么我们的方式就比较合适。

        ''' steps:
        1,get lastest number of shares, using close at reference date to get market value.p/e= MV/profit_q4_es
        2,get new weight of stock value using p/e and w_allo_value|growth
        
        
        '''
        ######################################################################
        ### 根据input参数，赋值给标准权重 
        # last | since 190712
        if sty_v_g == 'value' :
            weight_list[ "pct_port"] = weight_list[col_w_value]
        elif sty_v_g == 'growth':
            weight_list[ "pct_port"] = weight_list[col_w_growth] 


        return weight_list 


    def stock_weights_etf(self, df_head,df_stocks) :
        ### Function：生成etf组合权重
        ### INPUT :df_head{etf组合信息} ；df_stocks or sp_df{股票池权重}，
        ### OUTPUT:
        ### update:190709 | since 190709
        '''
        Example of df_stocks:
        code name num mark premium_pct amount
        0 1 平安银行 2400 3 0.1 33624
        todo items:
        1, if mark in{1=允许,3=深市退补},需要计算可成交价格，乘以股票数量后减去交易成本得到所费的现金。其中etf-sse的"amount"有值的是szse的
        2，if mark ==2=必须,要么选取停牌前20天均价，要么使用“amount"中的价格。

        '''
        ### get last quotation for given period and code list 
        ###TODO 在 db\\db_assets\\get_wind.py 里更新
        # 一次性获取所有的历史当日的股票收盘价，记住这里不要复权！
        '''
        >>> Wp.w.wss("600036.SH,601398.SH", "close,volume","tradeDate=20190704;priceAdj=U;cycle=D")
        .ErrorCode=0
        .Codes=[600036.SH,601398.SH]
        .Fields=[CLOSE,VOLUME]
        .Times=[20190709 16:48:28]
        .Data=[[36.08,5.68],[37917354.0,151690647.0]]
        '''
        import pandas as pd 
        
        ### notesL: df_stocks.code is in raw format : [1, 2, 63, 69, 100, 157, 166, 333]
        code_list = list( df_stocks.code_wind )
        items = ["close","volume"]

        # "20190708" or # "20190708.0"
        tradeDate =  df_head.loc["TradingDay","value"]  
        print( tradeDate )

        ### Method1 : Get wind quotation 
        # from db.db_assets.get_wind import wind_api
        # wind_api0 =wind_api()
        # wind0 = wind_api0.Get_wss(code_list,items,tradeDate)
        # df_wind = pd.DataFrame( wind0.Data, columns=code_list,index=items )
        # df_wind.to_csv("D:\\df_wind_1907.csv")

        ### Method2 :Import quotation from absolute path 
        df_wind = pd.read_csv("D:\\df_wind_190704.csv",index_col="Unnamed: 0")

        print("df_wind \n", df_wind.head)
        print( df_wind.info() )

        ####################################################################
        ### 根据mark计算权重、可执行价格。
        for temp_i in df_stocks.index :
            temp_mark = df_stocks.loc[temp_i, "mark"]
            temp_code_w = df_stocks.loc[temp_i, "code_wind"]
            # type(temp_code) is string 

            ### add quotation for trading stocks 
            #notes: type(temp_mark) is "str"
            
            if temp_mark in ["1","3"] :
                ### get last price from quotation
                df_stocks.loc[temp_i, "mv_es"] = df_stocks.loc[temp_i, "amount"]
                ### find quotes
                temp_close = df_wind.loc["close",temp_code_w]
                temp_vol = df_wind.loc["volume",temp_code_w]
                if temp_vol > 0 :
                    
                    # print( type( df_stocks.loc[temp_i, "num"]) )   # "str"
                    df_stocks.loc[temp_i, "mv_es"] =  temp_close * float( df_stocks.loc[temp_i, "num"] )


            elif temp_mark in ["2"] :
                df_stocks.loc[temp_i, "mv_es"] = df_stocks.loc[temp_i, "amount"]

        ####################################################################
        ### 
        weight_list = df_stocks
        para_cash_pct = 0.03 # 现金比例
        
        # type is str or char change to float
        weight_list[ "mv_es"] = pd.to_numeric( weight_list[ "mv_es"] )
        # print( weight_list[ "mv_es"].sum() )

        weight_list[ "pct_port"] = weight_list[ "mv_es"]/ (weight_list[ "mv_es"].sum()/para_cash_pct  )
        weight_list[ "amt"] =  weight_list[ "mv_es"]
        # df_stocks.to_csv("D:\\df_stocks_1907.csv",encoding="gbk")

        return weight_list

    def cal_5levels(self, obj_in ):
        ### 给定column，自动划分5档权重
        df1 = obj_in["df"]
        col1 = obj_in["col"]
        ### number of levels :5,10
        # num = obj_in["num"]
        ### col 是df里的某一列
        # notes：quantile需要反过来 前5%对应 quantile(0.95) 
        top_020 = df1[ col1 ].quantile( 0.8 )
        top_040 = df1[ col1 ].quantile( 0.6 )
        top_060 = df1[ col1 ].quantile( 0.4 )
        top_080 = df1[ col1 ].quantile( 0.2 )

        df1["s_"+col1 ] = 1.0 
        df1["s_"+col1 ] = df1[col1].apply(lambda x : 5 if x >= top_020 else ( 4 if x >= top_040 else ( 3 if x >= top_060 else ( 2 if x >= top_080 else 1 ) ) ) )
        ###
        obj_in["df"] = df1 

        return obj_in

    def cal_score_gpd(self, df1,col1 ):
        ### 给定column，去除极端值并计算百分比分数
        # df1 = obj_in["df"]
        # col1 = obj_in["col"]
        ### 剔除首位5%的数值
        # notes：quantile需要反过来 前5%对应 quantile(0.95) 
        top_005 = df1[ col1 ].quantile( 0.95 )
        top_095 = df1[ col1 ].quantile( 0.05 )
        
        df1["s_"+col1 ]= df1[ col1 ].apply( lambda x : top_005 if x >= top_005 else ( top_095 if x <= top_095 else x  ) )
        ### 去除负值——这个对负值的不合算，对正数但太小的亏了
        if df1["s_"+col1 ].sum() < 0.0 :
            df1["s_"+col1 ] = df1["s_"+col1 ].apply( lambda x : 0.0 if x < 0 else x )
        
        ### 
        df1["s_"+col1 ]=  df1["s_"+col1 ]/df1["s_"+col1 ].sum()
        
        ### save to obj
        # obj_in["df"] = df1 
        # return obj_in
        
        return df1 
































