# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
功能：基于Wind落地数据库的csv数据
1,导入股票行情、财务数据
2,支出 timing_abcd3d模块

from class data_timing_abcd3d(): to class data_pricevol_financial():
derived from data_io_timing_abcd3d.py；data_io.py
date:last 200526 | since 180601
===============================================
'''
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )
import pandas as pd
import numpy as np 

#######################################################################
class data_pricevol_financial():
    def __init__(self):
        #######################################################################
        ### 导入配置文件对象，例如path_db_wind等
        sys.path.append(path_ciss_rc+ "config\\")
        from config_data import config_data_timing_abcd3d
        config_data_1 = config_data_timing_abcd3d()
        self.obj_config = config_data_1.obj_config
        from data_io import data_io
        self.data_io_1 = data_io()
        #######################################################################
        ### timing_abcd3d 目录位置：
        # self.obj_config["dict"]["path_timing_abcd3d"] = dict_config["path_ciss_db"] +"timing_abcd3d\\"
        self.path_timing_abcd3d = self.obj_config["dict"]["path_timing_abcd3d"]
        #######################################################################
        ### 导入日期list，日、周、月 | date_list_tradingday.csv  ...     
        file_name_month = self.obj_config["dict"]["file_date_month"]    
        file_name_tradingday = self.obj_config["dict"]["file_date_tradingday"] 
        # file_name_week = self.obj_config["dict"]["file_date_week"]   

        df_date_month = pd.read_csv(self.obj_config["dict"]["path_dates"] + file_name_month )
        date_list_month = df_date_month["date"].values
        date_list_month.sort()
        
        df_date_tradingday  = pd.read_csv(self.obj_config["dict"]["path_dates"] + file_name_tradingday  )
        date_list_tradingday = df_date_tradingday["date"].values
        date_list_tradingday.sort()
        # 日期升序排列
        self.obj_data_io = {}
        self.obj_data_io["dict"] = {}
        self.obj_data_io["dict"]["date_list_month"] = date_list_month
        self.obj_data_io["dict"]["date_list_tradingday"] = date_list_tradingday
        
        #######################################################################
        ### 导入参数配置
        from config_indicator import config_indi_mom
        self.config_indi_mom_1 = config_indi_mom()


    def print_info(self):        
        print("   ")
        print("import_data_ashare_change_amt |交易日导入：A股行业、涨跌幅和成交额数据和保存动量择时指标，基于AShareEODPrices，AShareEODDerivativeIndicator")
        print("import_data_ashare_change_amt_period |计算一段时间内每个交易日的择时数据和财务指标")
        print("import_data_ashare_mv_fi_esti |导入市值、财务指标ttm 、预期数据等指标  ")
        print("import_data_ashare_fi_hist | 导入roe，roic等历史财务ttm指标")
        print("import_data_ashare_period_change | 给定T日，导入之前或之后N日的A股区间日涨跌幅输出成df |默认N=120天 ")
        
        
    def import_data_ashare_change_amt(self, obj_data) :
        ### 导入A股行业、涨跌幅和成交额数据和保存动量择时指标，基于AShareEODPrices，AShareEODDerivativeIndicator
        #######################################################################
        ### 获取距离给定日期最近的交易日
        
        obj_date={}
        obj_date["date"]= obj_data["dict"]["date_start"]   
        obj_date = self.data_io_1.get_trading_days(obj_date)     
        #notes: type is "numpy.int64"
        date_list_pre = obj_date["date_list_pre"] 
        date_list_post  = obj_date["date_list_post"]

        obj_data["dict"]["date_tradingdate"] = date_list_pre[-1]
        #######################################################################
        ### 判断是否已经存在目标csv文件
        path_ashare_ana = self.obj_config["dict"]["path_wind_adj"] + "ashare_ana\\" 
        file_output = "ADJ_timing_TRADE_DT_"+ str(date_list_pre[-1]) +"_ALL.csv"
        print(path_ashare_ana+file_output)
        if os.path.exists( path_ashare_ana+file_output ) :
            obj_data["df_mom_eod_prices"] = pd.read_csv( path_ashare_ana+file_output,encoding="gbk" )
            
            obj_data["dict"]["para_ma_short"] = self.config_indi_mom_1.obj_config["dict"]['para_ma_short']
            obj_data["dict"]["para_ma_long"] = self.config_indi_mom_1.obj_config["dict"]['para_ma_long']
            # [0.005, -0.005 , 0 ]
            obj_data["dict"]["para_p_ma"] = self.config_indi_mom_1.obj_config["dict"]['para_p_ma'] 
            # [0.003,0.002,0.001] 
            obj_data["dict"]["para_ma_up"] = self.config_indi_mom_1.obj_config["dict"]['para_ma_up']
            ### 获取对应长度的日期列表,多取1日为了计算前一日均值
            obj_data["dict"]["date_list_pre_sub"] = date_list_pre[(-1* max(obj_data["dict"]["para_ma_short"])-1): ]
             
        else : 
            ### 获取当日个股的行业分类
            
            #######################################################################
            ### 导入最近1个交易日的个股列表，
            table_name = "AShareEODPrices"
            path_table = self.obj_config["dict"]["path_wind_wds"] + table_name + "\\"
            # WDS_TRADE_DT_20200511_ALL.csv
            file_name = "WDS_TRADE_DT_"+ str(date_list_pre[-1]) +"_ALL.csv"
            try :
                df_eod_prices = pd.read_csv(path_table+ file_name )       
            except :
                df_eod_prices = pd.read_csv(path_table+ file_name,encoding="gbk" )       
            
            # print("date_list_pre[-1] ", date_list_pre[-1] )
            # print( df_eod_prices.head().T )
            # 剔除所有非正常代码股票，例如 T00018.SH
            list_index_keep = []
            for temp_i in df_eod_prices.index : 
                temp_code = df_eod_prices.loc[temp_i,"S_INFO_WINDCODE"]
                if temp_code[-3:] not in [".SH",".SZ"] or temp_code[0] not in ["6","3","0"] :
                    # 删除列
                    df_eod_prices = df_eod_prices.drop(temp_i, axis= 0 )

            #######################################################################
            ### 获取个股当时所属的行业分类
            object_ind={}
            object_ind["code_list"] = list( df_eod_prices["S_INFO_WINDCODE"].values )
            object_ind["date_end"] = date_list_pre[-1]
            object_ind["if_all_codes"] = 0 
            object_ind["if_column_ind"] = 1 
            object_ind["column_ind"] = "citics_ind_code_s_1"
            object_ind = self.data_io_1.get_ind_date(object_ind )
            # object_ind["df_s_ind_sub"]的index是code_list,columns=["wind_code","ind_code"]
            df_s_ind_sub =  object_ind["df_s_ind_sub"]

            # print( "df_s_ind_sub ", df_s_ind_sub.head() )
            #######################################################################
            ### 赋值个股行业分类
            for temp_i in df_eod_prices.index :
                temp_code = df_eod_prices.loc[temp_i, "S_INFO_WINDCODE" ]
                try :
                    df_eod_prices.loc[temp_i, "ind_code" ] = df_s_ind_sub.loc[temp_code,"ind_code" ]
                except:
                    print( temp_code )
                    df_eod_prices.loc[temp_i, "ind_code" ] = 0
            
            #######################################################################
            ### 根据参数,导入历史市场行情数据
            # 移动平均交易日的数量,大中小三个，分为短期short 和长期long参数
            # [16,40,100] [40,100,250]
            para_ma_short = self.config_indi_mom_1.obj_config["dict"]['para_ma_short']
            para_ma_long = self.config_indi_mom_1.obj_config["dict"]['para_ma_long']
            # [0.005, -0.005 , 0 ]
            para_p_ma = self.config_indi_mom_1.obj_config["dict"]['para_p_ma'] 
            # [0.003,0.002,0.001] 
            para_ma_up = self.config_indi_mom_1.obj_config["dict"]['para_ma_up']
            
            ### Using maximum value of para_ma_short 
            ### 获取对应长度的日期列表,多取1日为了计算前一日均值
            date_list_pre_sub =date_list_pre[-1* max(para_ma_short)-1: ]

            #######################################################################
            # 用 df_eod_prices 保存对应的MA数值 
            col_list_add = []
            # para_ 是参数， "ma_s_40"是
            count_para = 0 
            for x in para_ma_short :
                df_eod_prices[ "para_ma_s_"+str(x) ] = x
                df_eod_prices[ "ma_s_"+str(x) ] = 0.0
                # "ma_s_"+str(x-1) 主要是用于下一个交易日的快速计算
                df_eod_prices[ "ma_s_"+str(x-1) ] = 0.0
                df_eod_prices[ "ma_s_pre_"+str(x) ] = 0.0
                col_list_add = col_list_add + [ "para_ma_s_"+str(x) ]
                ### 基于均线的参数,这个要对应的匹配
                df_eod_prices[ "para_p_ma_"+str(x) ] = para_p_ma[ count_para ]          
                df_eod_prices[ "para_ma_up_"+str(x) ] = para_ma_up[ count_para ]  
                count_para =count_para +1 
            
            count_eod = 0
            for temp_date in date_list_pre_sub :
                file_name = "WDS_TRADE_DT_"+ str( temp_date ) +"_ALL.csv"
                try :
                    temp_df_eod = pd.read_csv(path_table+ file_name,encoding="gbk" )      
                except:
                    temp_df_eod = pd.read_csv(path_table+ file_name )       
                ### 
                if count_eod == 0 :
                    df_eod_all = temp_df_eod
                    count_eod =1 
                else :
                    df_eod_all = df_eod_all.append(temp_df_eod, ignore_index=True)
                
                # print( len(df_eod_all.index ) )
                                
            ### 对每个个股，获取 para_ma_short[0],para_ma_short[1],para_ma_short[2] 三个长度的均线
            for temp_i in df_eod_prices.index :
                temp_code = df_eod_prices.loc[temp_i, "S_INFO_WINDCODE" ]
                df_eod_all_sub = df_eod_all[ df_eod_all["S_INFO_WINDCODE"]==temp_code ]
                # 降序排列 , ascending=False
                df_eod_all_sub = df_eod_all_sub.sort_values(by="TRADE_DT", ascending=False)
                # 
                for x in para_ma_short :
                    temp_len = min(x, len(df_eod_all_sub.index ) )
                    # 有的交易日不足
                    df_eod_prices.loc[temp_i,"ma_s_"+str(x) ] = df_eod_all_sub[ "S_DQ_ADJCLOSE"].iloc[:temp_len].mean()
                    df_eod_prices.loc[temp_i,"ma_s_"+str(x-1) ] = df_eod_all_sub[ "S_DQ_ADJCLOSE"].iloc[:temp_len-1].mean()
                    df_eod_prices.loc[temp_i,"ma_s_pre_"+str(x) ] = df_eod_all_sub[ "S_DQ_ADJPRECLOSE"].iloc[:temp_len ].mean()
                    ### 计算收盘价所处区间最高最低价百分比 | S_DQ_ADJHIGH	S_DQ_ADJLOW
                    temp_period_low = df_eod_all_sub[ "S_DQ_ADJLOW"].iloc[:temp_len].min()
                    temp_period_high = df_eod_all_sub[ "S_DQ_ADJHIGH"].iloc[:temp_len].max()
                    temp_close = df_eod_all_sub[ "S_DQ_ADJCLOSE"].values[0]
                    if temp_period_high-temp_period_low >0 :
                        df_eod_prices.loc[temp_i,"close_pct_s_"+str(x) ] =(temp_close- temp_period_low)/(temp_period_high-temp_period_low )
                    else :
                        df_eod_prices.loc[temp_i,"close_pct_s_"+str(x) ] =0.0
            ### Save to csv 
            path_ashare_ana = self.obj_config["dict"]["path_wind_adj"] + "ashare_ana\\" 
            file_output = "ADJ_timing_TRADE_DT_"+ str(date_list_pre[-1]) +"_ALL.csv"
            df_eod_prices.to_csv(path_ashare_ana + file_output,index=False )
            
            obj_data["df_mom_eod_prices"] = df_eod_prices
            
            obj_data["dict"]["para_ma_short"] = para_ma_short
            obj_data["dict"]["para_ma_long"] =  para_ma_long
            # [0.005, -0.005 , 0 ]
            obj_data["dict"]["para_p_ma"] = para_p_ma
            # [0.003,0.002,0.001] 
            obj_data["dict"]["para_ma_up"] =  para_ma_up
            ### 获取对应长度的日期列表,多取1日为了计算前一日均值
            obj_data["dict"]["date_list_pre_sub"] = para_ma_short

        # 
        obj_data["dict"]["path_output"] = path_ashare_ana
        obj_data["dict"]["file_output"] = file_output
        
        return obj_data

    def import_data_ashare_change_amt_period(self, obj_data) :
        ### 计算一段时间内每个交易日的择时数据和财务指标
        from func_stra import stra_ashare_timing_monitor
        stra_ashare_timing_monitor_1 = stra_ashare_timing_monitor()

        obj_date={}
        obj_date["date"]= obj_data["dict"]["date_start"]
        obj_date["date_end"]= obj_data["dict"]["date_end"]
        
        obj_date = self.data_io_1.get_trading_days(obj_date)
        #notes: type is "numpy.int64"
        ### 区间或T日至今
        if "date_end" in obj_data["dict"].keys() :
            obj_date["date_end"]= obj_data["dict"]["date_end"]
            date_list = obj_date["date_list_period"] 
        else :
            date_list = obj_date["date_list_post"]

        date_list_pre = [obj_date["date_list_pre"][-1]] + date_list[:-1]
        
        print("Date list,",date_list)
        # notes:最后一个交易日可能无数据 
        for temp_date in date_list  :
            obj_data={}
            obj_data["dict"] ={}
            obj_data["dict"]["date_start"] = temp_date
            
            ### 对于期末交易日的所有个股，往前推16,40,100，250天；
            # 衍生行情里的52周最高最低价可能存在问题：AShareEODDerivativeIndicator，S_PQ_ADJHIGH_52W，S_PQ_ADJLOW_52W，这个应该是和当日收盘价S_DQ_CLOSE_TODAY比较的
            # notes:每天要保存近 15,39,100,250的数据，方便下一个交易日计算,例如：x=(39*ma+close)/40
            obj_data = self.import_data_ashare_change_amt( obj_data )

            # 此时obj_data["df_mom_eod_prices"] = df_eod_prices 只有ma数值和参数para，需要进一步做指标计算
            obj_data = stra_ashare_timing_monitor_1.stra_timing_abcd3d_s(obj_data)

            ### 导入前1交易日的市值、财务指标ttm 、预期数据
            obj_data["dict"]["date_pre"] =  date_list_pre[ date_list.index(temp_date) ]
            obj_data = self.import_data_ashare_mv_fi_esti( obj_data)

            ########################################################################
            ### 单独计算roe和roic历史收益 || roe和roic都没什么用
            ### 导入中国A股TTM指标历史数据
            # obj_data = data_timing_abcd3d_1.import_data_ashare_fi_hist( obj_data)

            '''共6个指标
            S_FA_ROE_TTM_pre,FA_ROIC_TTM_pre
            S_FA_ROE_TTM_pre_growth,FA_ROIC_TTM_pre_growth
            S_FA_ROE_TTM_pre_inv_std3y,FA_ROIC_TTM_pre_inv_std3y
            '''
            ### save to csv 
            # obj_data["df_mom_eod_prices"] 新增了几列:obj_data["dict"]["col_list_stra"] = ["indi_short","indi_mid", "abcd3d"]
            print(temp_date, "output:",obj_data["dict"]["path_output"] + obj_data["dict"]["file_output"] )
            obj_data["df_mom_eod_prices"].to_csv(obj_data["dict"]["path_output"] + obj_data["dict"]["file_output"] ,index=False,encoding="gbk"  )

        return obj_data

    def import_data_ashare_mv_fi_esti(self,obj_data ) :
        ### 导入市值、财务指标ttm 、预期数据等指标
        #######################################################################
        ### 获取距离给定日期最近的交易日
        # date_start 是给定的开始日期
        date_start = obj_data["dict"]["date_start"]   
        # date_pre用于导入预期数据
        date_pre = obj_data["dict"]["date_pre"] 
        # date_tradingdate 是给定日期前最后一个交易日
        date_tradingdate = obj_data["dict"]["date_tradingdate"]  
        #######################################################################
        ### 导入市值、财务指标ttm对应的csv文件；
        path_table = self.obj_config["dict"]["path_wind_wds"] + "AShareEODDerivativeIndicator\\" 
        # WDS_TRADE_DT_20200519_ALL.csv
        file_output = "WDS_TRADE_DT_"+ str( date_tradingdate ) +"_ALL.csv"
        # print( path_table + file_output)
        df_mv_fi = pd.read_csv( path_table + file_output )

        ### 赋值给 obj_data["df_mom_eod_prices"]
        # 归属母公司净利润(TTM),NET_PROFIT_PARENT_COMP_TTM;经营活动产生的现金流量净额(TTM),NET_CASH_FLOWS_OPER_ACT_TTM;
        # 营业收入(TTM), OPER_REV_TTM
        # 涨跌停状态,UP_DOWN_LIMIT_STATUS,1表示涨停;0表示非涨停或跌停;-1表示跌停。
        # 最高最低价状态,LOWEST_HIGHEST_STATUS,1表示是历史最高收盘价;0表示非历史最高价或最低价;-1表示是历史最低收盘价。    }
        col_list_mv_fi=[ "S_DQ_MV","S_VAL_MV","S_VAL_PE_TTM","S_VAL_PB_NEW","S_VAL_PCF_OCFTTM" ]
        col_list_others = ["UP_DOWN_LIMIT_STATUS","LOWEST_HIGHEST_STATUS"]

        for temp_col in col_list_mv_fi :
            obj_data["df_mom_eod_prices"][temp_col] = 0.0 
        for temp_col in col_list_others :
            obj_data["df_mom_eod_prices"][temp_col] = 0 
        
        for temp_i in obj_data["df_mom_eod_prices"].index :
            temp_code = obj_data["df_mom_eod_prices"].loc[temp_i, "S_INFO_WINDCODE"]
            ### find temp_code in  df_mv_fi,假设肯定能找到否则需要debug
            df_mv_fi_sub =  df_mv_fi[  df_mv_fi["S_INFO_WINDCODE"]== temp_code  ]
            if len( df_mv_fi_sub.index ) > 0 :
                temp_j = df_mv_fi_sub.index[0]
                for temp_col in col_list_mv_fi :
                    obj_data["df_mom_eod_prices"].loc[temp_i, temp_col] = df_mv_fi.loc[temp_j, temp_col]
                
                for temp_col in col_list_others :
                    obj_data["df_mom_eod_prices"].loc[temp_i, temp_col] = df_mv_fi.loc[temp_j, temp_col]
            else : 
                print( temp_code)
        
        #######################################################################
        ### 导入预期数据
        path_table = self.obj_config["dict"]["path_wind_wds"] + "AShareConsensusRollingData\\" 
        # WDS_EST_DT_20060104_ALL.csv|notes:当天内滚动数据会一直更新，因此取前一日的即可
        # notes:由于预期数据在T日会不断更新，因此若T日计算，只应该计算至T-1日
        # file_output = "WDS_EST_DT_"+ str( date_tradingdate ) +"_ALL.csv"
        file_output = "WDS_EST_DT_"+ str( date_pre ) +"_ALL.csv"
        
        # print( path_table + file_output)
        df_esti = pd.read_csv( path_table + file_output )
        # notes:20060104，每个股票都有FY0值，共988个，但其余指标中，FTTM有690个，其余数量在430~567之间
        
        ### 数据分析：
        # para_list = ["FY0","FTTM","FY1","FY2","FY3","YOY","YOY2","CAGR"  ]
        # 20060104：
        # 1，对于所有"CAGR",YOY,YOY2 
        # EST_EPS,EST_OPER_REVENUE,NET_PROFIT
        # notes:EST_ROE的值很乱，上下波动幅度很大。
        # 2，对于FTTM, FY1,FY2,FY3较多的是
        # EST_EPS,EST_OPER_REVENUE,EST_PE,EST_PEG,EST_ROE,EST_TOTAL_PROFIT，NET_PROFIT
        # 3,FY0,EST_OPER_PROFIT,EST_OPER_REVENUE,EST_PB,EST_PE,EST_PEG,EST_ROE,EST_TOTAL_PROFIT,NET_PROFIT
        # 20200516：
        # 1，对于所有"CAGR",YOY,YOY2，
        # NET_PROFIT,EST_EPS,EST_OPER_REVENUE,EST_EBIT,EST_EBITDA,EST_TOTAL_PROFIT,EST_OPER_PROFIT,EST_OPER_COST
        # 2，对于FTTM,FY1,FY2,FY3，少了"EST_PE","EST_PEG",
        # 多了NET_PROFIT，EST_EPS，EST_ROE,EST_OPER_REVENUE,EST_CFPS,EST_EBIT,EST_EBITDA,EST_TOTAL_PROFIT,EST_OPER_PROFIT,EST_OPER_COST
        # 3,FY0,NET_PROFIT,EST_EPS,EST_PE,EST_PEG,EST_PB,EST_ROE,EST_OPER_REVENUE,EST_CFPS,EST_BPS,EST_EBIT,EST_EBITDA,EST_TOTAL_PROFIT,EST_OPER_PROFIT,EST_OPER_COST
        # notes:fttm不如fy1，因为EST_PB的值都没有；
        col_list_esti_fy1 = [ "EST_OPER_REVENUE","NET_PROFIT", "EST_PE","EST_PEG","EST_PB","EST_ROE"  ]
        col_list_esti_fy0 = [ "EST_OPER_PROFIT","EST_OPER_REVENUE","EST_PB","EST_PE","EST_PEG","EST_ROE","EST_TOTAL_PROFIT","NET_PROFIT"   ]
        col_list_esti_yoy_cagr =[ "NET_PROFIT","EST_OPER_REVENUE"  ]

        ### 以060419为例，当日有交易股票数量1347，预期数据number of FY0=919,FY1=463，YOY=517,CAGR=463
        # FY1
        temp_word = "_FY1"
        for temp_col in col_list_esti_fy1 :
            obj_data["df_mom_eod_prices"][temp_col  +temp_word] = np.nan
        
        df_esti_fy1 = df_esti[ df_esti["ROLLING_TYPE"] =="FY1" ]
        for temp_i in df_esti_fy1.index :
            temp_code = df_esti_fy1.loc[temp_i, "S_INFO_WINDCODE"]
            # find temp_code in obj_data["df_mom_eod_prices"]["S_INFO_WINDCODE"]
            temp_df = obj_data["df_mom_eod_prices"][ obj_data["df_mom_eod_prices"]["S_INFO_WINDCODE"]== temp_code ]
            if len( temp_df.index ) >0 :
                temp_j = temp_df.index[0]
                for temp_col in col_list_esti_fy1 :
                    obj_data["df_mom_eod_prices"].loc[temp_j,temp_col +temp_word ] = df_esti_fy1.loc[temp_i, temp_col]
        # FY0
        temp_word = "_FY0"
        for temp_col in col_list_esti_fy0 :
            obj_data["df_mom_eod_prices"][temp_col +temp_word ] = np.nan
        
        df_esti_fy0 = df_esti[ df_esti["ROLLING_TYPE"] =="FY0" ]
        for temp_i in df_esti_fy0.index :
            temp_code = df_esti_fy0.loc[temp_i, "S_INFO_WINDCODE"]
            # find temp_code in obj_data["df_mom_eod_prices"]["S_INFO_WINDCODE"]
            temp_df = obj_data["df_mom_eod_prices"][ obj_data["df_mom_eod_prices"]["S_INFO_WINDCODE"]== temp_code ]
            if len( temp_df.index ) >0 :
                temp_j = temp_df.index[0]
                for temp_col in col_list_esti_fy0 :
                    obj_data["df_mom_eod_prices"].loc[temp_j, temp_col+ temp_word ] = df_esti_fy0.loc[temp_i, temp_col]
        # YOY
        temp_word = "_YOY"
        for temp_col in col_list_esti_yoy_cagr :
            obj_data["df_mom_eod_prices"][temp_col  +temp_word] = np.nan
        
        df_esti_yoy = df_esti[ df_esti["ROLLING_TYPE"] =="YOY" ]
        for temp_i in df_esti_yoy.index :
            temp_code = df_esti_yoy.loc[temp_i, "S_INFO_WINDCODE"]
            # find temp_code in obj_data["df_mom_eod_prices"]["S_INFO_WINDCODE"]
            temp_df = obj_data["df_mom_eod_prices"][ obj_data["df_mom_eod_prices"]["S_INFO_WINDCODE"]== temp_code ]
            if len( temp_df.index ) >0 :
                temp_j = temp_df.index[0]
                for temp_col in col_list_esti_yoy_cagr :
                    obj_data["df_mom_eod_prices"].loc[temp_j, temp_col+temp_word ] = df_esti_yoy.loc[temp_i, temp_col]
        
        #######################################################################

        return obj_data

    def import_data_ashare_fi_hist(self,obj_data ) :
        ### 导入roe，roic等历史财务ttm指标
        #######################################################################
        ### 获取距离给定日期最近的交易日
        # date_start 是给定的开始日期
        date_start = obj_data["dict"]["date_start"]   
        # date_tradingdate 是给定日期前最后一个交易日
        date_tradingdate = obj_data["dict"]["date_tradingdate"] 

        #######################################################################
        ### 导入历史财务指标，roe,roic等
        # notes:AShareFinancialIndicator和AShareTTMHis不是每个交易日滚动披露全部股票的，例如去年q4和当年q1会集中在0428，0429,0430几天；
        # 中国A股TTM指标历史数据，AShareTTMHis：投入资本回报率(TTM)，FA_ROIC_TTM
        path_table = self.obj_config["dict"]["path_wind_wds"] + "AShareTTMHis\\" 
        # WDS_full_table_full_table_ALL.csv，WDS_ANN_DT_20200430_ALL.csv
        # file_output = "WDS_EST_DT_"+ str( date_tradingdate ) +"_ALL.csv"
        file_output = "WDS_full_table_full_table_ALL.csv"
        # print( path_table + file_output)
        df_fi_hist = pd.read_csv( path_table + file_output )
        
        ### REPORT_PERIOD  2718 non-null int64
        # obj_data["dict"]["date_start"] could be str or float 
        if not type( obj_data["dict"]["date_start"] ) == str :
            temp_date = str( int( obj_data["dict"]["date_start"] ))
        fiscal_year_pre = int( str(int(temp_date[:4])-1) +"1231")
        fiscal_year_pre_2 = int( str(int(temp_date[:4])-2) +"1231")
        fiscal_year_pre_3 = int( str(int(temp_date[:4])-3) +"1231")
        print("fiscal_year_pre ",fiscal_year_pre,fiscal_year_pre_2,fiscal_year_pre_3 )
        df_fi_hist_sub = df_fi_hist[ df_fi_hist["REPORT_PERIOD"].isin([fiscal_year_pre,fiscal_year_pre_2,fiscal_year_pre_3]  ) ]
        print("df_fi_hist_sub ", len( df_fi_hist_sub.index) )              
        '''需要的6个指标：
        择当年净资产收益率、净资产收益率3年标准差倒数、净资产收益率当年增长、
        当年投入资本回报率、投入资本回报率3年标准差倒数、投入资本回报率当年增长
        
        投入资本回报率(TTM),FA_ROIC_TTM;净资产收益率(TTM),S_FA_ROE_TTM
        '''
        col_list_fi_hist = ["S_FA_ROE_TTM", "FA_ROIC_TTM" ]

        for temp_i in obj_data["df_mom_eod_prices"].index :
            temp_code = obj_data["df_mom_eod_prices"].loc[temp_i, "S_INFO_WINDCODE"]
            ### find temp_code in df_fi_hist_sub 
            df_fi_hist_sub_2 =  df_fi_hist_sub[  df_fi_hist_sub["S_INFO_WINDCODE"]== temp_code  ]

            if len( df_fi_hist_sub_2.index ) > 0 :
                temp_j = df_fi_hist_sub_2.index[0]
                ### 1,择当年净资产收益率,当年投入资本回报率
                df_fi_hist_sub_s = df_fi_hist_sub_2[ df_fi_hist_sub_2["REPORT_PERIOD"] == fiscal_year_pre ]
                if len( df_fi_hist_sub_s.index ) > 0 :
                    temp_j = df_fi_hist_sub_s.index[0]
                    for temp_col in col_list_fi_hist :
                        obj_data["df_mom_eod_prices"].loc[temp_i, temp_col+"_pre"] = df_fi_hist_sub.loc[temp_j, temp_col]
                
                ### 2,净资产收益率当年增长、投入资本回报率当年增长  
                df_fi_hist_sub_s = df_fi_hist_sub_2[ df_fi_hist_sub_2["REPORT_PERIOD"] == fiscal_year_pre_2 ] 
                if len( df_fi_hist_sub_s.index ) > 0 :
                    temp_j = df_fi_hist_sub_s.index[0]
                    for temp_col in col_list_fi_hist :
                        obj_data["df_mom_eod_prices"].loc[temp_i, temp_col+"_pre_2"] = df_fi_hist_sub.loc[temp_j, temp_col]
                        if df_fi_hist_sub.loc[temp_j, temp_col] > 0 :
                            obj_data["df_mom_eod_prices"].loc[temp_i, temp_col+"_pre_growth"] = obj_data["df_mom_eod_prices"].loc[temp_i, temp_col+"_pre"]/df_fi_hist_sub.loc[temp_j, temp_col] - 1
                        elif df_fi_hist_sub.loc[temp_j, temp_col] <=0 and obj_data["df_mom_eod_prices"].loc[temp_i, temp_col+"_pre"] > 0:
                            # 负值转正
                            obj_data["df_mom_eod_prices"].loc[temp_i, temp_col+"_pre_growth"] = 1.0
                else :
                    for temp_col in col_list_fi_hist :
                        obj_data["df_mom_eod_prices"].loc[temp_i, temp_col+"_pre_growth"] = 0.0

                ### 3,净资产收益率3年标准差倒数、投入资本回报率2年标准差倒数
                # 投入资本回报率前3年nan
                df_fi_hist_sub_s = df_fi_hist_sub_2[ df_fi_hist_sub_2["REPORT_PERIOD"] == fiscal_year_pre_3 ]

                if len( df_fi_hist_sub_s.index ) > 0 :
                    temp_j = df_fi_hist_sub_s.index[0]
                    for temp_col in col_list_fi_hist :
                        obj_data["df_mom_eod_prices"].loc[temp_i, temp_col+"_pre_3"] = df_fi_hist_sub.loc[temp_j, temp_col]

                        # print( "Debug====" ,type(df_fi_hist_sub.loc[temp_j, temp_col]) , df_fi_hist_sub.loc[temp_j, temp_col] )
                        if not df_fi_hist_sub.loc[temp_j, temp_col] >= 0 :
                            list_2y_values = [obj_data["df_mom_eod_prices"].loc[temp_i, temp_col+"_pre"],obj_data["df_mom_eod_prices"].loc[temp_i, temp_col+"_pre_2"] ]
                            inv_std= 1/ (np.std( list_2y_values )*1.4)
                        else : 
                            list_3y_values = [obj_data["df_mom_eod_prices"].loc[temp_i, temp_col+"_pre"],obj_data["df_mom_eod_prices"].loc[temp_i, temp_col+"_pre_2"],df_fi_hist_sub.loc[temp_j, temp_col] ]
                            inv_std= 1/ np.std( list_3y_values )                       
                        # notes list_3y_values往往最后一个值是 nan
                        obj_data["df_mom_eod_prices"].loc[temp_i, temp_col+"_pre_inv_std3y"] = inv_std

                else :
                    for temp_col in col_list_fi_hist :
                        obj_data["df_mom_eod_prices"].loc[temp_i, temp_col+"_pre_inv_std3y"] = 0.0

            else : 
                print( temp_code)

        #######################################################################
        ### save to csv 
        path_output = self.obj_config["dict"]["path_ciss_db"] + "ydyl\\"
        obj_data["df_mom_eod_prices"].to_csv( path_output + "df_factor_trade_dt_"+ str(date_tradingdate) +".csv",index=False ,encoding="gbk"  )
        # obj_data["df_mom_eod_prices"].columns.to_csv( path_output + "columns.csv" ,encoding="gbk"  )
        # obj_data["df_mom_eod_prices"].to_csv(obj_data["dict"]["path_output"] + obj_data["dict"]["file_output"] ,encoding="gbk"  )
        obj_data["dict"]["date_tradingdate"] = date_tradingdate
        obj_data["dict"]["path_output"] = path_output

        return obj_data

    def import_data_ashare_change_amt_1d(self, obj_data) :
        ### 下一个交易日导入：A股涨跌幅和成交额数据和保存动量择时指标，假设已有上一个交易日obj_data["df_mom_eod_prices"]
        # notes:暂时不需要考虑这么精细的计算成本


        return obj_data

    def import_data_ashare_period_change(self, obj_data):
        ### 给定T日，导入之前或之后N日的A股区间日涨跌幅输出成df |默认N=120天取[T-N+1,T]
        ### 判断是向前"pre"取值还是向后取值"post",默认是"pre"
        if not obj_data["dict"]["if_date_list"] == 1 : 
            ########################################################################
            ### 日期导入方式一：固定导入过去120天交易日日期,
            if not "date_pre_post" in obj_data["dict"].keys() :
                date_pre_post =  "pre"
            else :
                date_pre_post = obj_data["dict"]["date_pre_post"] 

            ### 默认N=120天取[T-N+1,T]或[T,T+N-1]
            if not "date_len" in obj_data["dict"].keys() :
                date_len = 90
            else :
                date_len = int( obj_data["dict"]["date_len"] ) 
            # type of temp_date is str 
            temp_date = obj_data["dict"]["latest_date"]

            # type of date_list_tradingday is class 'numpy.int64'
            # notes:新股或市场较早时期，头尾有可能不足120天
            date_list_tradingday = self.obj_data_io["dict"]["date_list_tradingday"] 
            date_list_tradingday.sort() 

            if date_pre_post == "pre" :
                date_list = [ x for x in date_list_tradingday if x <= int(temp_date) ]
                date_list = date_list[-1*date_len :]
            if date_pre_post == "post" :
                date_list = [ x for x in date_list_tradingday if x >= int(temp_date) ]
                date_list = date_list[:date_len ]
 
        ########################################################################
        ### 日期导入方式二：给定date_list  |
        if obj_data["dict"]["if_date_list"] == 1 : 
            date_list = obj_data["dict"]["date_list"]
         
        ### 
        date_list.sort() 

        ########################################################################
        ### 对120个交易日导入全部股票代码
        table_name = "AShareEODPrices"
        path_table = self.obj_config["dict"]["path_wind_wds"] + table_name + "\\"

        count_days = 0 
        for temp_date in date_list :
            # print("Chekc date ", type(temp_date ), temp_date )
            file_name = "WDS_TRADE_DT_"+ str(temp_date ) + "_ALL.csv"
                        
            #######################################################################
            ### 导入1个交易日的个股列表, WDS_TRADE_DT_20200511_ALL.csv 
            try :
                df_eod_prices = pd.read_csv(path_table+ file_name )       
            except :
                df_eod_prices = pd.read_csv(path_table+ file_name,encoding="gbk" )   
            # S_DQ_PCTCHANGE 涨跌幅百分比的百分比值，"S_DQ_ADJCLOSE" 是复权收盘价
            # 以2006-8为例，原始股票梳理1351，取值后 1326
            df_eod_prices = df_eod_prices.loc[:, ["S_INFO_WINDCODE","S_DQ_ADJCLOSE","S_DQ_PCTCHANGE" ] ]     
            df_eod_prices = df_eod_prices[ df_eod_prices["S_INFO_WINDCODE"].isin(obj_data["dict"]["code_list"])  ] 
            
            ### 赋值给 大的df 
            if count_days == 0 :
                # close 
                df_all_adjclose = df_eod_prices.loc[:, ["S_INFO_WINDCODE","S_DQ_ADJCLOSE" ] ]
                df_all_adjclose = df_all_adjclose.rename( columns={"S_DQ_ADJCLOSE": temp_date }  )
                # pct change 
                df_all_pctchg = df_eod_prices.loc[:, ["S_INFO_WINDCODE","S_DQ_PCTCHANGE" ] ]
                df_all_pctchg = df_all_pctchg.rename( columns={"S_DQ_PCTCHANGE": temp_date }  ) 
                count_days = count_days +1  
            else :
                # close 
                df_temp = df_eod_prices.loc[:, ["S_INFO_WINDCODE","S_DQ_ADJCLOSE" ] ]
                df_temp = df_temp.rename( columns={"S_DQ_ADJCLOSE": temp_date }  )
                # ,on="S_INFO_WINDCODE" 表示基于股票代码链接2个df
                # notes: df_temp 比df_all 有更多的codes; how="outer"指的是用2个df列的并集
                df_all_adjclose = pd.merge( df_temp, df_all_adjclose,on="S_INFO_WINDCODE"   ) 

                # pct change
                df_temp2 = df_eod_prices.loc[:, ["S_INFO_WINDCODE","S_DQ_PCTCHANGE" ] ]
                df_temp2 = df_temp2.rename( columns={"S_DQ_PCTCHANGE": temp_date }  )
                df_all_pctchg = pd.merge( df_temp2, df_all_pctchg ,on="S_INFO_WINDCODE"   ) 

        
        # df_all.to_csv("D:\\df_all.csv")    
            # print("df_all \n", df_all.head() )
            # input1 = input("Check to proceed.....")

        ### save to output 
        # notes:如果对index赋值，会导致 ValueError: 'S_INFO_WINDCODE' is both an index level and a column label, which is ambiguous.
        # df_all_adjclose.index = df_all_adjclose["S_INFO_WINDCODE"]
        obj_data["df_ashare_adjclose"] = df_all_adjclose 
        # df_all_pctchg.index = df_all_pctchg["S_INFO_WINDCODE"]
        obj_data["df_ashare_pctchg"] = df_all_pctchg
        obj_data["date_list"] = date_list
       
        ########################################################################

        return obj_data

