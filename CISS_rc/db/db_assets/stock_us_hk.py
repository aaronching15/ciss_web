# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
功能：初步定义港股和美股



数据来源： Wind，SEC
last update 181225 | since  181225
derived from stocks_funda_wash.py
===============================================
'''
import pandas as pd
import numpy as np
import json
import time
import datetime as dt
import sys
sys.path.append("..") 

class stock_foreign() :
    # last | since 181013
    
    def __init__(self, country = 'CN',market = 'nasdaq'):
        self.info = "This class has following objects: \n[countries,country,items,temp_columns,time_stamp_input,years,mmdd,index_list,path,file_name]"
        # country,market
        self.country = country #  'US' 
        self.market = market # 'nasdaq' # nyse 
    
    def get_profit_q4_es(self,path,date_periods,file_name ="symbol_list.txt" ) :
        ### step 1 13q1-18q2,计算每半年的当年净利润预测
        # last 181226 | since 181226

        ####################################################################
        ## import symbol list 

        df_symbol = pd.read_csv(path+file_name )

        df_profit_q4_es = pd.DataFrame( columns=df_symbol["Code"]  )
        for t_s in date_periods.periods_reference_change :
            print("t_s ", t_s)
            # 2014-05-31 00:00:00 ||  2018-05-31 00:00:00
            ### 根据2014-05，计算当年预测净利润，注意美股财务数据日期角度有很多坑
            # 对于给定日期，获取该日期前4个披露日期，根据"span"进行预测计算

            ######################################################################
            ### Load financial data 
            for temp_symbol in df_symbol["Code"] :
                print("Symbol ", temp_symbol )
                file_name = "18q3_13q1_"+ temp_symbol + ".csv"
                df_in= pd.read_csv(path+file_name ,encoding="gbk")
                print(df_in.columns[1:] )
                ### str to datetime 
                dt_time_str = pd.DataFrame(pd.to_datetime( df_in.columns[1:] ),index=df_in.columns[1:]  )
                dt_time_str2 = dt_time_str[ dt_time_str[0]<= t_s ]
                # print( dt_time_str2[0].index )
                span_1 = df_in.loc[1, dt_time_str2[0].index[0] ]
                span_2 = df_in.loc[1, dt_time_str2[0].index[1] ]
                print("Net profit1 ",span_1, df_in.loc[11, dt_time_str2[0].index[0] ] )
                print("Net profit2 ",span_2, df_in.loc[11, dt_time_str2[0].index[1] ] )
                if span_1[0]=="3"  and span_2[:2]=="12"  :
                    # just get financial data for q1 
                    profit_q1 = float( df_in.loc[11, dt_time_str2[0].index[0] ].replace(",","") )
                    profit_q4_pre = float( df_in.loc[11, dt_time_str2[0].index[1] ].replace(",","") )
                    profit_q1_pre = float( df_in.loc[11, dt_time_str2[0].index[4] ].replace(",","") )

                    profit_q1_pre_pct = profit_q1_pre/profit_q4_pre
                    profit_q1_yoy =profit_q1/profit_q1_pre
                    para_fi_max = 0.35 # for q1 
                    para_fi_1 = min(profit_q1_pre_pct,para_fi_max )
                    profit_q4_es =  profit_q1 + (profit_q4_pre-profit_q1_pre)*((1-para_fi_1)+para_fi_1*profit_q1_yoy)

                elif span_1[0]=="9"  and span_2[0]=="6"  :
                    # just get financial data for q2
                    profit_q3 = float( df_in.loc[11, dt_time_str2[0].index[0] ].replace(",","") )
                    profit_q4_pre = float( df_in.loc[11, dt_time_str2[0].index[3] ].replace(",","") )
                    profit_q3_pre = float( df_in.loc[11, dt_time_str2[0].index[4] ].replace(",","") )

                    profit_q3_yoy = profit_q3/profit_q3_pre # yoy
                    profit_q3_pre_pct = profit_q3_pre/profit_q4_pre
                    para_fi_max = 0.75 # for q3
                    para_fi_1 = min(profit_q3_pre_pct,para_fi_max)
                    profit_q4_es = profit_q3 + (profit_q4_pre-profit_q3_pre)*((1-para_fi_1)+para_fi_1*profit_q3_yoy)
                elif span_1[0]=="8"  and span_2[0]=="6"  :
                    # case PEP.O
                    # just get financial data for q2
                    profit_q3 = float( df_in.loc[11, dt_time_str2[0].index[0] ].replace(",","") )
                    profit_q4_pre = float( df_in.loc[11, dt_time_str2[0].index[3] ].replace(",","") )
                    profit_q3_pre = float( df_in.loc[11, dt_time_str2[0].index[4] ].replace(",","") )

                    profit_q3_yoy = profit_q3/profit_q3_pre # yoy
                    profit_q3_pre_pct = profit_q3_pre/profit_q4_pre
                    para_fi_max = 0.75 # for q3
                    para_fi_1 = min(profit_q3_pre_pct,para_fi_max)
                    profit_q4_es = profit_q3 + (profit_q4_pre-profit_q3_pre)*((1-para_fi_1)+para_fi_1*profit_q3_yoy)

                elif span_1[0]=="6"  and span_2[0]=="3"  :

                    profit_q2 = float( df_in.loc[11, dt_time_str2[0].index[0] ].replace(",","") )
                    profit_q4_pre = float( df_in.loc[11, dt_time_str2[0].index[1] ].replace(",","") )
                    profit_q2_pre = float( df_in.loc[11, dt_time_str2[0].index[4] ].replace(",","") )

                    profit_q2_pre_pct = profit_q2_pre/profit_q4_pre
                    profit_q2_yoy =profit_q2/profit_q2_pre
                    para_fi_max = 0.6 # for q1 
                    para_fi_1 = min(profit_q2_pre_pct,para_fi_max )
                    profit_q4_es =  profit_q2 + (profit_q4_pre-profit_q2_pre)*((1-para_fi_1)+para_fi_1*profit_q2_yoy)

                elif span_1[:2]=="12"  and span_2[0]=="9"  :
                    # just release past year result 
                    profit_q4 = float( df_in.loc[11, dt_time_str2[0].index[0] ].replace(",","") )
                    profit_q4_es = profit_q4

                elif span_1[:2]=="12"  and span_2[0]=="6"  :
                    # just release past year result ,HK case 
                    profit_q4 = float( df_in.loc[11, dt_time_str2[0].index[0] ].replace(",","") )
                    profit_q4_es = profit_q4
                elif span_1[0]=="6"  and span_2[:2]=="12"  :
                    # just release past year result ,HK case  3333
                    if len( dt_time_str2[0].index ) <= 2 :
                        profit_q4 = float( df_in.loc[11, dt_time_str2[0].index[0] ].replace(",","") )
                        profit_q4_es = profit_q4
                    else : 
                        profit_q2 = float( df_in.loc[11, dt_time_str2[0].index[0] ].replace(",","") )
                        profit_q4_pre = float( df_in.loc[11, dt_time_str2[0].index[1] ].replace(",","") )
                        profit_q2_pre = float( df_in.loc[11, dt_time_str2[0].index[2] ].replace(",","") )

                        profit_q2_pre_pct = profit_q2_pre/profit_q4_pre
                        profit_q2_yoy =profit_q2/profit_q2_pre
                        para_fi_max = 0.6 # for q1 
                        para_fi_1 = min(profit_q2_pre_pct,para_fi_max )
                        profit_q4_es =  profit_q2 + (profit_q4_pre-profit_q2_pre)*((1-para_fi_1)+para_fi_1*profit_q2_yoy)

                else :
                    asd

                df_profit_q4_es.loc[ t_s, temp_symbol ] = profit_q4_es  
                print("profit_q4_es ", profit_q4_es)

        df_profit_q4_es.to_csv("D:\\df_profit_q4_es_hk.csv")
 
        return df_profit_q4_es


    def get_weight_allocation(self,df_profit_q4_es):
        ### step 2 每半年计算市场价值组合的配置计划

        # df_profit_q4_es = pd.read_csv("D:\\df_profit_q4_es.csv")
        # df_profit_q4_es.index = df_profit_q4_es["Unnamed: 0"]
        # df_profit_q4_es = df_profit_q4_es.drop( ["Unnamed: 0"],axis=1 )
        # print( df_profit_q4_es.head(5)  ) 

        df_w_allo = pd.DataFrame( index=df_profit_q4_es.index ,  columns =  df_profit_q4_es.columns  )
        for temp_index in df_w_allo.index :
            ### Get rid of the negative profit estimation 
            temp_sum  = df_profit_q4_es.loc[temp_index ,:].apply(lambda x: max(x,0.0) ).sum()
            df_w_allo.loc[temp_index ,:] = df_profit_q4_es.loc[temp_index ,:].apply(lambda x: max(x,0.0) )/ temp_sum 
            # todo delete negative values!!


        df_w_allo.to_csv("D:\\df_w_allo_hk.csv")

        return df_w_allo 

    def get_quotes(self,path,file_name= "symbol_list.txt" ) :
        # 下载历史行情数据。注意wind非A股票存在很多缺失值，使用后1个交易日的成交量加权平均值
        
        df_symbol = pd.read_csv(path+file_name )
        from db.data_io import data_wind
        path0='D:\\db_wind\\'
        dw0 = data_wind( '',path0 )
        type_wsd='day_us'
        date_start= "20140101"
        date_end =  "20181224"
        file_path0 = "D:\\db_wind\\quotes_us\\"
        # symbol = "AAPL.O"
        for symbol in df_symbol["Code"] :
            wind_obj = dw0.data_wind_wsd_us(symbol,date_start,date_end,type_wsd )

            file_name =  symbol + '_day_' +date_start + '_' +date_end  
            file_json = file_name +'.json'
            with open( file_path0 + file_json ,'w') as f:
                json.dump( wind_obj.wind_head  ,f) 
            file_csv = file_name +'.csv'
            wind_obj.wind_df.to_csv(file_path0+  file_csv  )
 
        return 1 

    # def get_portfolio_unit_quick(self,date_periods,path  ) :
    #     ### 用一种比逐日计算更快地方式计算出历史净值 
 
    #     return df_unit_mdd