# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
dervied from performance_eval.py
功能：因子模型指标计算、因子组合绩效分析

perf_eval_ashare_factor_model |评估多因子模型单一时期的表现，例如历史和未来1~6月的收益，行业分布；每一期的重要收益指标存入 df_perf_eval,index=日期 ")
perf_eval_ashare_1factor |评估单因子的分组收益 ")
perf_eval_ashare_factors_group |给定区间，计算多因子和单因子组合收益 ")
perf_eval_ashare_factors_ind_group |给定区间，计算多因子和单因子分行业分组收益 ")

3，关联脚本：对应配置文件 | config\ 

4,OUTPUT: 
5,分析： 

6，Notes:  

date:last   | since 211117
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

    def perf_eval_ashare_factor_model(self,obj_perf_eval, obj_opt ) :
        ### 评估多因子模型单一时期的表现，例如历史和未来1~6月的收益，行业分布；每一期的重要收益指标存入 df_perf_eval,index=日期
        temp_index = obj_opt["dict"]["date_last_month"]
        
        ######################################################################################
        ### 更新或新建 df_perf_eval,index=日期
        if not "df_perf_eval" in obj_perf_eval.keys() :
            col_list=["date_opt","date_next_1m","date_next_3m","date_next_6m","ret_fun","success","message","ret_s_1m_w_opt","ret_s_1m_w_index_consti","ret_s_3m_w_opt","ret_s_3m_w_index_consti","ret_s_6m_w_opt","ret_s_6m_w_index_consti" ]
            col_list=col_list+["ret_s_1m_alpha","ret_s_3m_alpha","ret_s_6m_alpha"]
            obj_perf_eval["df_perf_eval"] = pd.DataFrame(index=[ temp_index ],columns=col_list )
            obj_perf_eval["notes"] ="评估多因子模型单一时期的表现,如历史和未来1~6月的收益，行业分布;"
        
        # save  obj_opt["res"] to multi-periods df
        obj_perf_eval["df_perf_eval"].loc[temp_index,"date_opt"] = temp_index
        obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_fun"] = obj_opt["res"].fun*-1
        obj_perf_eval["df_perf_eval"].loc[temp_index,"success"] = obj_opt["res"].success
        obj_perf_eval["df_perf_eval"].loc[temp_index,"message"] = obj_opt["res"].message
        
        
        ######################################################################################
        ### df_index_consti里有代码，权重*100,交易日期S_CON_WINDCODE  I_WEIGHT  TRADE_DT等
        # 股票代码列表和组合权重 
        code_list_csi300 = obj_opt["code_list_csi300"]
        df_index_consti = obj_opt["df_index_consti"]
        df_index_consti["wind_code"] = df_index_consti["S_CON_WINDCODE"]
        df_index_consti = df_index_consti[ df_index_consti["wind_code"].isin(code_list_csi300)  ]
        df_index_consti = df_index_consti.sort_values(by="wind_code" )
        df_index_consti.reset_index(drop=True )

        # 行业分类 df_ind_code: wind_code  citics_ind_code_s_1,按wind_code升序排列
        df_ind_code = obj_opt["df_ind_code"]
        df_index_consti["ind_code"] = df_ind_code["citics_ind_code_s_1"]
        # weight opt and index constituents
        df_index_consti["w_opt"] = obj_opt["res"]["x"] 
        df_index_consti["w_index_consti"] =  obj_opt["w_index_consti"]

        ### 导入未来1和6月收益率 ret_stock_change_np，ret_stock_change_6m_np
        from analysis_indicators import indicator_ashares,analysis_factor
        indicator_ashares_1 = indicator_ashares()

        obj_in_index={} 
        obj_in_index["date_pre"] =  obj_opt["dict"]["date_last_month"] # 取当月日期
        # date_list_month :给定交易日到最新交易日例如202003的月份list
        # print("date_list_month",  obj_opt["date_list_month"][:3] )
        
        #######################################################################
        ### 个股：历史和未来1~6月的收益，df_index_consti["ret_s_1m","ret_s_6m"]
        from data_io import data_io
        data_io_1 = data_io()
        obj_days={}
        ### 取1个月后日期和收益
        # notes:需要确保月末日属于交易日,用data_io里的模块。 
        obj_days["date"] = obj_opt["date_list_month"][0] 
        obj_days = data_io_1.get_trading_days(obj_days) 
        # notes，应该选月末日前的最后一个交易日
        temp_date = obj_days["date_list_pre"][-1] 
        obj_perf_eval["df_perf_eval"].loc[temp_index,"date_next_1m"] = temp_date

        obj_in_index["date"] = temp_date 
        obj_in_index["df_change"] = df_index_consti
        obj_in_index["df_change"]["wind_code"] = obj_in_index["df_change"]["S_CON_WINDCODE"]
        # print("df_index_consti" ,obj_in_index["df_change"].head()  ) 
        # notes:有可能因为时间较新，无法取未来1个月数据
        try:
            obj_in_index_1m = indicator_ashares_1.ashares_stock_price_vol_change( obj_in_index )
            df_index_consti["ret_s_1m" ] = 0.0

            for temp_i in df_index_consti.index :
                ### locate wind_code 
                temp_code = df_index_consti.loc[temp_i,"wind_code" ]
                ### ret 1m,
                temp_df = obj_in_index_1m["df_change"][ obj_in_index_1m["df_change"]["wind_code"] ==temp_code ]
                if len(temp_df.index ) > 0 :
                    temp_j = temp_df.index[0] 
                    df_index_consti.loc[temp_i,"ret_s_1m" ] = obj_in_index_1m["df_change"].loc[ temp_j, "s_change_adjclose" ]
                else :
                    df_index_consti.loc[temp_i,"ret_s_1m" ] = 0.0
                
                df_index_consti.loc[temp_i,"ret_s_1m_w_opt" ] =df_index_consti.loc[temp_i,"ret_s_1m" ]*df_index_consti.loc[temp_i,"w_opt" ]
                df_index_consti.loc[temp_i,"ret_s_1m_w_index_consti" ] =df_index_consti.loc[temp_i,"ret_s_1m" ]*df_index_consti.loc[temp_i,"w_index_consti" ]
        except:
            print("Error when fetching return data for next 1 months..",temp_date , df_index_consti)

        ### 取3个月后日期和收益
        # notes:需要确保月末日属于交易日|
        obj_days["date"] = obj_opt["date_list_month"][2] 
        obj_days = data_io_1.get_trading_days(obj_days) 
        temp_date = obj_days["date_list_pre"][-1]
        obj_perf_eval["df_perf_eval"].loc[temp_index,"date_next_3m"] = temp_date
        print("temp_date",temp_date ) 
        obj_in_index["date"] = temp_date 
        # notes:有可能因为时间较新，无法取未来1个月数据
        try:
            obj_in_index_3m = indicator_ashares_1.ashares_stock_price_vol_change( obj_in_index )
            df_index_consti["ret_s_3m" ] = 0.0
            for temp_i in df_index_consti.index : 
                temp_code = df_index_consti.loc[temp_i,"wind_code" ]
                ### ret 3m,
                temp_df = obj_in_index_3m["df_change"][ obj_in_index_3m["df_change"]["wind_code"] ==temp_code ]
                if len(temp_df.index ) > 0 :
                    temp_j = temp_df.index[0] 
                    df_index_consti.loc[temp_i,"ret_s_3m" ] = obj_in_index_3m["df_change"].loc[ temp_j, "s_change_adjclose" ]
                else :
                    df_index_consti.loc[temp_i,"ret_s_3m" ] = 0.0
                
                df_index_consti.loc[temp_i,"ret_s_3m_w_opt" ] =df_index_consti.loc[temp_i,"ret_s_3m" ]*df_index_consti.loc[temp_i,"w_opt" ]
                df_index_consti.loc[temp_i,"ret_s_3m_w_index_consti" ] =df_index_consti.loc[temp_i,"ret_s_3m" ]*df_index_consti.loc[temp_i,"w_index_consti" ]
        except:
            print("Error when fetching return data for next 3 months..",temp_date , df_index_consti)
  
        ### 取6个月后日期
        # notes:需要确保月末日属于交易日|
        obj_days["date"] = obj_opt["date_list_month"][5] 
        obj_days = data_io_1.get_trading_days(obj_days) 
        temp_date = obj_days["date_list_pre"][-1]
        obj_perf_eval["df_perf_eval"].loc[temp_index,"date_next_6m"] = temp_date
        print("temp_date",temp_date ) 
        obj_in_index["date"] = temp_date 
        # notes:有可能因为时间较新，无法取未来1个月数据
        try:
            obj_in_index_6m = indicator_ashares_1.ashares_stock_price_vol_change( obj_in_index )
            # obj_in_index["df_change"]["s_change_adjclose"]
            df_index_consti["ret_s_6m" ] = 0.0
            for temp_i in df_index_consti.index : 
                temp_code = df_index_consti.loc[temp_i,"wind_code" ]
                ### ret 6m,
                temp_df = obj_in_index_6m["df_change"][ obj_in_index_6m["df_change"]["wind_code"] ==temp_code ]
                if len(temp_df.index ) > 0 :
                    temp_j = temp_df.index[0] 
                    df_index_consti.loc[temp_i,"ret_s_6m" ] = obj_in_index_6m["df_change"].loc[ temp_j, "s_change_adjclose" ]
                else :
                    df_index_consti.loc[temp_i,"ret_s_6m" ] = 0.0

                df_index_consti.loc[temp_i,"ret_s_6m_w_opt" ] =df_index_consti.loc[temp_i,"ret_s_6m" ]*df_index_consti.loc[temp_i,"w_opt" ]
                df_index_consti.loc[temp_i,"ret_s_6m_w_index_consti" ] =df_index_consti.loc[temp_i,"ret_s_6m" ]*df_index_consti.loc[temp_i,"w_index_consti" ]
        except:
            print("Error when fetching return data for next 6 months..",temp_date , df_index_consti)
  
        if obj_opt["res"].success == False :
            df_index_consti["ret_s_1m_w_opt" ]=df_index_consti["ret_s_1m_w_index_consti" ]
            df_index_consti["ret_s_3m_w_opt" ]=df_index_consti["ret_s_3m_w_index_consti" ]
            df_index_consti["ret_s_6m_w_opt" ]=df_index_consti["ret_s_6m_w_index_consti" ]

        #######################################################################
        ### 统计组合的股票数量和行业数量
        # 取个股权重大于0.1%的股票
        temp_df = df_index_consti[ df_index_consti["w_opt"] >=0.001 ]
        obj_perf_eval["df_perf_eval"].loc[temp_index,"num_stock"] = temp_df ["w_opt" ].count()
        # 对持仓个股取行业分类：
        obj_perf_eval["df_perf_eval"].loc[temp_index,"num_ind"] = temp_df ["ind_code" ].drop_duplicates().count()
        
        #######################################################################
        ### 行业：持仓权重分布，行业内加权收益，组合内加权收益
        # TODO
        obj_perf_eval["df_ind_"+ str(temp_index) ] = df_index_consti.groupby( "ind_code" ).sum()
        
        #######################################################################
        ### 和上一期比较
        # TODO       

        ######################################################################################
        ### save to object,主要是在"df_change"里加了几列
        obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_1m_w_opt"] = df_index_consti["ret_s_1m_w_opt" ].sum()
        obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_3m_w_opt"] = df_index_consti["ret_s_3m_w_opt" ].sum()
        obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_6m_w_opt"] = df_index_consti["ret_s_6m_w_opt" ].sum()

        obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_1m_w_index_consti"] = df_index_consti["ret_s_1m_w_index_consti"].sum()
        obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_3m_w_index_consti"] = df_index_consti["ret_s_3m_w_index_consti"].sum()
        obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_6m_w_index_consti"] = df_index_consti["ret_s_6m_w_index_consti"].sum()
        
        # 计算超额收益：["ret_s_1m_alpha","ret_s_3m_alpha","ret_s_6m_alpha"]
        obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_1m_alpha"]=obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_1m_w_opt"] - obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_1m_w_index_consti"]
        obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_3m_alpha"]=obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_3m_w_opt"] - obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_3m_w_index_consti"]
        obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_6m_alpha"]=obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_6m_w_opt"] - obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_6m_w_index_consti"]
        
        # obj_opt["df_change"]保存在 obj_perf_eval[ temp_index ]，例如obj_perf_eval[ 20060228 ]
        obj_perf_eval["df_"+ str(temp_index) ] = df_index_consti
        
        
        return obj_perf_eval

    def perf_eval_ashare_1factor(self,obj_perf_eval,obj_port):
        ### 评估单因子的分组收益
        '''input：obj_port
        obj_port["date"] :截止日期
        obj_port["num_port_1factor"] 单因子分组的数量，例如5组
        obj_port["level_1f"] = temp_level # 当前所处单因子分类的第几组
        obj_port["col_weight"] = "w_1factor" # df内权重对应的column name
        obj_port["df_weight"] = df_factor_weight_sub # 也包含股票代码 "wind_code"
        '''
        temp_date_0 = obj_port["date"]
        num_port_1factor = obj_port["num_port_1factor"]
        temp_level = obj_port["level_1f"] 
        temp_index = temp_date_0
        temp_col = obj_port["col_weight"]
        df_weight = obj_port["df_weight"]  
        # date_list_month对应要计算的月份，不包括开始月份
        date_list_month = obj_port["dict"]["date_list_month"]
        
        ######################################################################################
        ### 更新或新建 df_perf_eval,index=日期
        if not "df_perf_eval" in obj_perf_eval.keys() :
            col_list=["date_opt","date_next_1m","date_next_3m","date_next_6m"]
            # Add ,"ret_s_1m_w","ret_s_3m_w","ret_s_6m_w" for level 1,2,3,4
            for temp_i in range(num_port_1factor) :
                col_list= col_list + [ "ret_s_1m_w_"+str(temp_i) ]
            for temp_i in range(num_port_1factor) :
                col_list= col_list + [ "ret_s_3m_w_"+str(temp_i) ]
            for temp_i in range(num_port_1factor) :
                col_list= col_list + [ "ret_s_6m_w_"+str(temp_i) ]
            
            obj_perf_eval["df_perf_eval"] = pd.DataFrame(index=[ temp_index ],columns=col_list )
            obj_perf_eval["notes"] ="评估单因子单一时期的表现,如历史和未来1、3、6月的收益，行业分布;"

        ### 导入未来1和6月收益率 ret_stock_change_np，ret_stock_change_6m_np
        from analysis_indicators import indicator_ashares
        indicator_ashares_1 = indicator_ashares()

        obj_in_index={} 
        obj_in_index["date_pre"] =  temp_date_0 # 取当月日期
        
        #######################################################################
        ### 个股：历史和未来1~6月的收益，df_index_consti["ret_s_1m","ret_s_6m"]
        from data_io import data_io,data_factor_model
        data_io_1 = data_io()
        data_factor_model_1 = data_factor_model()
        
        ### 取1个月后日期和收益
        # notes:需要确保月末日属于交易日,用data_io里的模块。 
        # date_list_month对应要计算的月份，不包括开始月份
        obj_days={}
        obj_days["date"] = date_list_month[0]
        obj_days = data_io_1.get_trading_days(obj_days) 
        # notes，应该选月末日前的最后一个交易日
        temp_date = obj_days["date_list_pre"][-1]  

        obj_perf_eval["df_perf_eval"].loc[temp_index,"date_next_1m"] = temp_date
        obj_in_index["date"] = temp_date 
        obj_in_index["df_change"] =df_weight
        
        obj_in_index_1m = indicator_ashares_1.ashares_stock_price_vol_change( obj_in_index )
        df_weight.loc[:,"ret_s_1m" ] = 0.0

        for temp_i in df_weight.index :
            ### locate wind_code 
            temp_code = df_weight.loc[temp_i,"wind_code" ]
            ### ret 1m,
            temp_df = obj_in_index_1m["df_change"][ obj_in_index_1m["df_change"]["wind_code"] ==temp_code ]
            if len(temp_df.index ) > 0 :
                temp_j = temp_df.index[0] 
                df_weight.loc[temp_i,"ret_s_1m" ] = obj_in_index_1m["df_change"].loc[ temp_j, "s_change_adjclose" ]
            else :
                df_weight.loc[temp_i,"ret_s_1m" ] = 0.0
            
            df_weight.loc[temp_i,"ret_s_1m_w_" +str(temp_level) ] =df_weight.loc[temp_i,"ret_s_1m" ]*df_weight.loc[temp_i,"w_1factor" ]
            
        ### 取3个月后日期和收益
        # notes:需要确保月末日属于交易日|
        obj_days["date"] = date_list_month[2] 
        obj_days = data_io_1.get_trading_days(obj_days) 
        temp_date = obj_days["date_list_pre"][-1]
        obj_perf_eval["df_perf_eval"].loc[temp_index,"date_next_3m"] = temp_date
        print("temp_date",temp_date ) 
        obj_in_index["date"] = temp_date 
        obj_in_index_3m = indicator_ashares_1.ashares_stock_price_vol_change( obj_in_index )
        df_weight.loc[:,"ret_s_3m" ] = 0.0
        for temp_i in df_weight.index : 
            temp_code = df_weight.loc[temp_i,"wind_code" ]
            ### ret 3m,
            temp_df = obj_in_index_3m["df_change"][ obj_in_index_3m["df_change"]["wind_code"] ==temp_code ]
            if len(temp_df.index ) > 0 :
                temp_j = temp_df.index[0] 
                df_weight.loc[temp_i,"ret_s_3m" ] = obj_in_index_3m["df_change"].loc[ temp_j, "s_change_adjclose" ]
            else :
                df_weight.loc[temp_i,"ret_s_3m" ] = 0.0
            
            df_weight.loc[temp_i,"ret_s_3m_w_" +str(temp_level) ] =df_weight.loc[temp_i,"ret_s_3m" ]*df_weight.loc[temp_i,"w_1factor" ]
        
        ### 取6个月后日期
        # notes:需要确保月末日属于交易日|
        obj_days["date"] = date_list_month[5] 
        obj_days = data_io_1.get_trading_days(obj_days) 
        temp_date = obj_days["date_list_pre"][-1]
        obj_perf_eval["df_perf_eval"].loc[temp_index,"date_next_6m"] = temp_date
        print("temp_date",temp_date ) 
        obj_in_index["date"] = temp_date 
        obj_in_index_6m = indicator_ashares_1.ashares_stock_price_vol_change( obj_in_index )
        # obj_in_index["df_change"]["s_change_adjclose"]
        df_weight.loc[:,"ret_s_6m" ] = 0.0
        for temp_i in df_weight.index : 
            temp_code = df_weight.loc[temp_i,"wind_code" ]
            ### ret 6m,
            temp_df = obj_in_index_6m["df_change"][ obj_in_index_6m["df_change"]["wind_code"] ==temp_code ]
            if len(temp_df.index ) > 0 :
                temp_j = temp_df.index[0] 
                df_weight.loc[temp_i,"ret_s_6m" ] = obj_in_index_6m["df_change"].loc[ temp_j, "s_change_adjclose" ]
            else :
                df_weight.loc[temp_i,"ret_s_6m" ] = 0.0

            df_weight.loc[temp_i,"ret_s_6m_w_" +str(temp_level) ] =df_weight.loc[temp_i,"ret_s_6m" ]*df_weight.loc[temp_i,"w_1factor" ]


        #######################################################################
        ### 行业：持仓权重分布，行业内加权收益，组合内加权收益
        # 当前版本df_weight 内无中信行业分类代码 "ind_code"
        
        #######################################################################
        ### 和上一期比较
        # TODO

        ######################################################################################
        ### save to object,主要是在"df_change"里加了几列
        obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_1m_w_" +str(temp_level) ] = df_weight["ret_s_1m_w_" +str(temp_level)  ].sum()
        obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_3m_w_" +str(temp_level) ] = df_weight["ret_s_3m_w_" +str(temp_level)  ].sum()
        obj_perf_eval["df_perf_eval"].loc[temp_index,"ret_s_6m_w_" +str(temp_level) ] = df_weight["ret_s_6m_w_" +str(temp_level)  ].sum()  

        # obj_opt["df_change"]保存在 obj_perf_eval[ temp_index ]，例如obj_perf_eval[ 20060228 ]
        obj_perf_eval["df_"+ str(temp_index) ] = df_weight

        return obj_perf_eval

    def perf_eval_ashare_factors_group(self,obj_data):
        ### 给定区间，计算多因子和单因子组合收益
        date_start = obj_data["dict"]["date_start"] 
        date_end = obj_data["dict"]["date_end"] 
        # 取开始日期前最后一个交易日
        date_start_pre = [x for x in self.date_list if x <= int(date_start)][-1]
        # 取结束日期前最后一个交易日
        date_end_pre = [x for x in self.date_list if x <= int(date_end)][-1]
        print("date_start_pre ",date_start_pre,"date_end_pre " , date_end_pre)
        
        #######################################################################
        ### 获取期初 adjclose
        file_name = "WDS_TRADE_DT_" + str(date_start_pre) +"_ALL.csv"
        table_name = "AShareEODPrices"
        temp_path = self.obj_config["dict"]["path_wind_wds"]+ table_name+"\\" + file_name
        try :
            df_eod_prices_1 = pd.read_csv(temp_path, encoding="gbk"  )
        except :
            df_eod_prices_1 = pd.read_csv(temp_path  )
        ### 获取期末 adjclose
        file_name = "WDS_TRADE_DT_" + str(date_end_pre) +"_ALL.csv"
        table_name = "AShareEODPrices"
        temp_path = self.obj_config["dict"]["path_wind_wds"]+ table_name+"\\" + file_name
        try :
            df_eod_prices_2 = pd.read_csv(temp_path, encoding="gbk"  )
        except :
            df_eod_prices_2 = pd.read_csv(temp_path  )

        #######################################################################
        ### 计算区间涨跌幅
        for temp_i in obj_data["df_factor"].index :
            temp_code = obj_data["df_factor"].loc[temp_i, "S_INFO_WINDCODE" ]
            ### find adjclose in date_start
            df_sub_1 = df_eod_prices_1[ df_eod_prices_1["S_INFO_WINDCODE" ] == temp_code  ]
            if len( df_sub_1.index )>0 :
                temp_j = df_sub_1.index[0]
                obj_data["df_factor"].loc[temp_i, "adjclose_start" ] =  df_sub_1.loc[temp_j,"S_DQ_ADJCLOSE"]

            ### find adjclose in date_end
            df_sub_2 = df_eod_prices_2[ df_eod_prices_2["S_INFO_WINDCODE" ] == temp_code  ]
            if len( df_sub_2.index )>0 :
                temp_j = df_sub_2.index[0]
                obj_data["df_factor"].loc[temp_i, "adjclose_end" ] =  df_sub_2.loc[temp_j,"S_DQ_ADJCLOSE"]
            else :
                obj_data["df_factor"].loc[temp_i, "adjclose_end" ] = obj_data["df_factor"].loc[temp_i, "adjclose_start" ] 

            ### calculate ret 
            if not obj_data["df_factor"].loc[temp_i, "adjclose_start" ] == np.nan :
                if not obj_data["df_factor"].loc[temp_i, "adjclose_start" ] == 0.0 :
                    obj_data["df_factor"].loc[temp_i, "ret_period" ] =  obj_data["df_factor"].loc[temp_i, "adjclose_end" ]/obj_data["df_factor"].loc[temp_i, "adjclose_start" ] -1
                else :
                    obj_data["df_factor"].loc[temp_i, "ret_period" ] =  0.0
            else :
                obj_data["df_factor"].loc[temp_i, "ret_period" ] = 0.0


        #######################################################################
        ### 计算单因子和多因子组合加权涨跌幅
        # 判断obj_data 里是否已经有 df_ret
        if not "df_ret" in obj_data.keys() :
            obj_data["df_ret"]=pd.DataFrame(index=[date_end_pre] ,columns=["date_start","date_end","ret_port","ret_bm" ] )
        
        ### 
        obj_data["df_ret"].loc[ date_end_pre, "date_start" ] = date_start_pre
        obj_data["df_ret"].loc[ date_end_pre, "date_end" ] = date_end_pre
        
        ### 计算基准指数收益率； 获取沪深300区间收益率
        ### 获取期初 adjclose
        # file_name = "WDS_TRADE_DT_" + str(date_start_pre) +"_ALL.csv"
        file_name = "WDS_S_INFO_WINDCODE_000300.SH_ALL.csv"
        table_name = "AIndexEODPrices"
        temp_path = self.obj_config["dict"]["path_wind_wds"]+ table_name+"\\" + file_name
        # print( temp_path )
        try :
            df_eod_prices_1 = pd.read_csv(temp_path, encoding="gbk"  )
        except :
            df_eod_prices_1 = pd.read_csv(temp_path  )
        
        # df_sub = df_eod_prices_1[df_eod_prices_1["S_INFO_WINDCODE"]=="000300.SH" ]
        
        df_sub = df_eod_prices_1[df_eod_prices_1["TRADE_DT"]== date_start_pre ]
        
        index_adjclose_start = df_eod_prices_1.loc[ df_sub.index[0] ,"S_DQ_CLOSE"  ]

        ### 获取期末 adjclose
        # file_name = "WDS_TRADE_DT_" + str(date_end_pre) +"_ALL.csv" 
        # temp_path = self.obj_config["dict"]["path_wind_wds"]+ table_name+"\\" + file_name
        # try :
        #     df_eod_prices_2 = pd.read_csv(temp_path, encoding="gbk"  )
        # except :
        #     df_eod_prices_2 = pd.read_csv(temp_path  )

        # df_sub = df_eod_prices_2[df_eod_prices_2["S_INFO_WINDCODE"]=="000300.SH" ]
        df_eod_prices_2 = df_eod_prices_1
        df_sub = df_eod_prices_2[df_eod_prices_2["TRADE_DT"]== date_end_pre ]

        index_adjclose_end = df_eod_prices_2.loc[ df_sub.index[0] ,"S_DQ_CLOSE"  ]

        index_ret = index_adjclose_end/index_adjclose_start -1 
        obj_data["df_ret"].loc[ date_end_pre, "ret_bm" ] = index_ret

        ### 单因子和多因子分组回测
        if "col_list" in obj_data.keys() :
            ### temp_col+"_mad_weight"
            col_list_fi_hist = obj_data["col_list"] 
            for temp_col in col_list_fi_hist :
                temp_ret = (obj_data["df_factor"]["ret_period" ]*obj_data["df_factor"][temp_col+"_mad_weight" ]).sum()
                obj_data["df_ret"].loc[ date_end_pre, "ret_port_"+temp_col ] = temp_ret

            temp_ret = (obj_data["df_factor"]["ret_period" ]*obj_data["df_factor"]["sum_mad_weight" ]).sum()
            obj_data["df_ret"].loc[ date_end_pre, "ret_port_sum" ] = temp_ret
            
            #######################################################################
            ### 计算每期配置比例最高的行业
            for temp_col in col_list_fi_hist :
                temp_df = obj_data["df_factor"].loc[:,["ind_code",temp_col+"_mad_weight" ]].groupby("ind_code").sum()
                temp_df = temp_df.sort_values(by=temp_col+"_mad_weight" ,ascending=False)
                obj_data["df_ret"].loc[ date_end_pre, "ind_weight_1_code_"+temp_col+"_mad_weight" ] = temp_df.index[0]
                obj_data["df_ret"].loc[ date_end_pre, "ind_weight_1_weight_"+temp_col+"_mad_weight" ] = temp_df.loc[temp_df.index[0], temp_col+"_mad_weight"]

            temp_df = obj_data["df_factor"].loc[:,["ind_code","sum"+"_mad_weight" ]].groupby("ind_code").sum()
            temp_df = temp_df.sort_values(by="sum"+"_mad_weight" ,ascending=False)
            obj_data["df_ret"].loc[ date_end_pre, "ind_weight_1_code_"+"sum"+"_mad_weight" ] = temp_df.index[0]
            obj_data["df_ret"].loc[ date_end_pre, "ind_weight_1_weight_"+"sum"+"_mad_weight" ] = temp_df.loc[temp_df.index[0], "sum"+"_mad_weight"]

            ### 每期第一大重仓股
            temp_df = obj_data["df_factor"].sort_values(by="sum"+"_mad_weight" ,ascending=False)
            obj_data["df_ret"].loc[ date_end_pre, "s_weight_1_code_"+"sum"+"_mad_weight" ] = temp_df.index[0]
            obj_data["df_ret"].loc[ date_end_pre, "s_weight_1_weight_"+"sum"+"_mad_weight" ] = temp_df.loc[temp_df.index[0], "sum"+"_mad_weight"]

        elif obj_data["dict"]["stra_name"] == "abcd3d":
            ### 计算组合收益
            temp_ret = (obj_data["df_factor"]["ret_period" ]*obj_data["df_factor"]["w_port" ]).sum()
            obj_data["df_ret"].loc[ date_end_pre, "ret_port_sum" ] = temp_ret

            ### 计算每期配置比例最高的行业
            temp_df = obj_data["df_factor"].loc[:,["ind_code","w_port" ]].groupby("ind_code").sum()
            temp_df = temp_df.sort_values(by="w_port" ,ascending=False)
            obj_data["df_ret"].loc[ date_end_pre, "ind_weight_1_code_"+"w_port" ] = temp_df.index[0]
            obj_data["df_ret"].loc[ date_end_pre, "ind_weight_1_weight_"+"w_port" ] = temp_df.loc[temp_df.index[0], "w_port"]

            ### 每期第一大重仓股
            temp_df = obj_data["df_factor"].sort_values(by="w_port" ,ascending=False)
            obj_data["df_ret"].loc[ date_end_pre, "s_weight_1_code_"+"w_port" ] = temp_df.loc[temp_df.index[0], "S_INFO_WINDCODE"]
            obj_data["df_ret"].loc[ date_end_pre, "s_weight_1_weight_"+"w_port" ] = temp_df.loc[temp_df.index[0], "w_port"]


        # obj_perf_eval = obj_data

        return obj_data
    
    def perf_eval_ashare_factors_ind_group(self,obj_data):
        ### 给定区间，计算多因子和单因子分行业分组收益
        from data_io import data_io
        data_io_1 = data_io()
        # obj_data["dict"]["date_start"] = str(temp_date_start) #  "20191105" 
        # obj_data["dict"]["date_end"] = str(temp_date_end) #  "20191105" 
        col_list_mad = obj_data["col_list_mad"] 

        #######################################################################################
        ### 获取区间股票收益率:notes!这里要设置正确的起止日期
        obj_temp ={}
        obj_temp["dict"] ={}
        obj_temp["dict"]["date_start"] = obj_data["dict"]["date_ann_pre"]
        obj_temp["dict"]["date_end"] = obj_data["dict"]["date_ann"]
        obj_temp["df_ashare_ana"] = obj_data["df_ashare_ana"]
        obj_temp = data_io_1.get_period_pct_chg_codelist( obj_temp )
        
        df_ashare_ana = obj_temp["df_ashare_ana"]

        ###################################################################################
        ### 新建分组收益计算:分行业计算
        df_ret_all = pd.DataFrame(index=[0], columns=["ind_code","indicator","ret_ew","ret_mvfloat","ret_top_30pct","ret_mid_40pct","ret_bottom_30pct","num_sp"] )
        count_group= 0 
        ### 筛选生物医药行业 35.0
        ind_list = df_ashare_ana["ind_code"].drop_duplicates().to_list()
        print("ind_list ",ind_list  )
        for temp_ind in ind_list :
            # 新建分组收益计算
            df_ret = pd.DataFrame(index=col_list_mad, columns=["ind_code","indicator","ret_ew","ret_mvfloat","ret_top_30pct","ret_mid_40pct","ret_bottom_30pct","num_sp"] )
            df_ret["ind_code"] = temp_ind 
            # type( df_ashare_ana["ind_code"].values[0] = df_ashare_ana  <class 'numpy.float64'> 35.0
            df_ashare_ana_sub = df_ashare_ana [ df_ashare_ana["ind_code"]== temp_ind  ]
            

            ### 对5类因子分别计算3个分组收益【30%，40%，30%】
            '''
            对于单因子，等权重组合收益均为8.9%左右，反应了单因子选中股票的数量比较多，前30%、40%、30%分组收益基本上在13%--8%--7%依次下降，
            分组收益反向的指标是 S_QFA_CGRGR_diff、S_QFA_CGRPROFIT_diff，前30%、40%、30%分组收益基本上在6%--10.8%--10.3%依次上升，
                        ret_ew	ret_mvfloat	ret_top_30pct	ret_mid_40pct	ret_bottom_30pct
            S_FA_ROE_q_ave	 8.93%	9.25%	14.55%	8.94%	6.58%
            S_QFA_CGRGR_diff 8.41%	9.0%	5.48%	10.60%	10.48%
            S_FA_ROA_q_ave	8.98%	9.29%	14.61%	9.28%	6.56%
            '''
            
            for temp_col in col_list_mad :
                df_ret.loc[temp_col,"indicator" ] = temp_col 
                df_ashare_ana_sub2 = df_ashare_ana_sub[ df_ashare_ana_sub[temp_col +"_signal"] == 1  ]
                ### 股票数量
                df_ret.loc[temp_col,"num_sp" ] = df_ashare_ana_sub2["period_pct_chg"].count()
                ### 计算平均收益率
                df_ret.loc[temp_col,"ret_ew" ] = df_ashare_ana_sub2["period_pct_chg"].mean()

                ### 计算流通市值加权收益率
                df_ret.loc[temp_col, "ret_mvfloat" ] =  (df_ashare_ana_sub2["period_pct_chg"]*df_ashare_ana_sub2["S_DQ_MV_mad"]).sum()/df_ashare_ana_sub2["S_DQ_MV_mad"].sum()

                ### 计算指标百分位在 前中后 30%，40%，30%分组收益率。df.quantile(x)中，x=70%意味着取前30%的值
                # quantile_level_70pct > quantile_level_30pct
                quantile_level_30pct = df_ashare_ana_sub2["zscore_" + temp_col  ].quantile( 0.3 )
                quantile_level_70pct = df_ashare_ana_sub2["zscore_" + temp_col  ].quantile( 0.7 )
                # print("quantile_level_30pct ", quantile_level_30pct,quantile_level_70pct  )
                # 取前30%的值，对应 df.quantile( 0.7 )
                df_ret.loc[temp_col, "ret_top_30pct" ] = df_ashare_ana_sub2[ df_ashare_ana_sub2["zscore_" + temp_col  ]>= quantile_level_70pct ]["period_pct_chg"].mean()
                # 取后30%的值，对应 df.quantile( 0.3 )
                df_ret.loc[temp_col, "ret_bottom_30pct" ] = df_ashare_ana_sub2[ df_ashare_ana_sub2["zscore_" + temp_col  ] < quantile_level_70pct ]["period_pct_chg"].mean()
                # 取中间40%
                df_temp = df_ashare_ana_sub2[ df_ashare_ana_sub2["zscore_" + temp_col  ] < quantile_level_70pct ]
                df_temp = df_temp [ df_temp ["zscore_" + temp_col  ] >= quantile_level_30pct  ]
                df_ret.loc[temp_col, "ret_mid_40pct" ] = df_temp["period_pct_chg"].mean()

            ### 计算合并组合    
            temp_col = "all"
            df_ashare_ana_sub2 = df_ashare_ana_sub[ df_ashare_ana_sub[temp_col +"_signal"] == 1  ]
            ### 股票数量
            df_ret.loc[temp_col,"num_sp" ] = df_ashare_ana_sub2["period_pct_chg"].count()
            ### 计算平均收益率
            df_ret.loc[temp_col, "ret_ew" ] = df_ashare_ana_sub2["period_pct_chg"].mean()

            ### 计算流通市值加权收益率
            df_ret.loc[temp_col, "ret_mvfloat" ] =  (df_ashare_ana_sub2["period_pct_chg"]*df_ashare_ana_sub2["S_DQ_MV_mad"]).sum()/df_ashare_ana_sub2["S_DQ_MV_mad"].sum()

            ### 计算指标百分位在 前中后 30%，40%，30%分组收益率。df.quantile(x)中，x=70%意味着取前30%的值
            # quantile_level_70pct > quantile_level_30pct
            quantile_level_30pct = df_ashare_ana_sub2["zscore_" + temp_col  ].quantile( 0.3 )
            quantile_level_70pct = df_ashare_ana_sub2["zscore_" + temp_col  ].quantile( 0.7 )
            # print("quantile_level_30pct ", quantile_level_30pct,quantile_level_70pct  )
            # 取前30%的值，对应 df.quantile( 0.7 )
            df_ret.loc[temp_col, "ret_top_30pct" ] = df_ashare_ana_sub2[ df_ashare_ana_sub2["zscore_" + temp_col  ]>= quantile_level_70pct ]["period_pct_chg"].mean()
            # 取后30%的值，对应 df.quantile( 0.3 )
            df_ret.loc[temp_col, "ret_bottom_30pct" ] = df_ashare_ana_sub2[ df_ashare_ana_sub2["zscore_" + temp_col  ] < quantile_level_70pct ]["period_pct_chg"].mean()
            # 取中间40%
            df_temp = df_ashare_ana_sub2[ df_ashare_ana_sub2["zscore_" + temp_col  ] < quantile_level_70pct ]
            df_temp = df_temp [ df_temp ["zscore_" + temp_col  ] >= quantile_level_30pct  ]
            df_ret.loc[temp_col, "ret_mid_40pct" ] = df_temp["period_pct_chg"].mean()

            if count_group == 0 :
                df_ret_all = df_ret
                count_group = 1 
            else :
                df_ret_all =df_ret_all.append(df_ret,ignore_index=True)

        ### save analytical dataframe to output 
        obj_data["df_ret_all"] = df_ret_all

        return obj_data
 