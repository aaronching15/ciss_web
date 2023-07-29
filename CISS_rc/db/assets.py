# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：管理股票、指数、基金、债券等资产的数据
    下载月度行情数据和核心指标：个股、指数、基金
# notes：3种资产的指标不太一样。
# reference：get_wind_api.py\def get_wss_ma_amt_mv
曾用名：assets.py
last update 220922 | since  160121
Menu :   

hierarchy of stocks design：
    Stock:a,h,us
    Index
    Fund
    Derivatives 
Notes:
1,行业类别除了计划在A股，港股，美股等不同市场股票类别下兼容分析，还计划在
股票和债券直接兼容分析，例如东方园林的股票和债券对应同一细分行业。

===============================================
'''
# from re import S
import sys,os
# 当前目录 C:\rc_202X\rc_202X\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0] 
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )

sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )

import pandas as pd
import numpy as np

#######################################################################
### 导入配置文件对象，例如path_db_wind等 
from data_io import data_io 
data_io_1 = data_io()
from times import times
times_1 = times()
 
###################################################################################
### 管理行情、估值、预期等个股数据
class quote_ashares_index_fund_month(): 
    def __init__(self ):
        ################################################
        ### Config with path 
        from config_data import config_data
        config_data1 = config_data()
        self.path_dict = config_data1.obj_config["dict"] 
        self.path_ciss_web = self.path_dict["path_ciss_web"]
        self.path_ciss_exhi = self.path_dict["path_ciss_exhi"]
        self.path_ciss_rc = self.path_dict["path_ciss_rc"]
        self.path_db = self.path_dict["path_db"] 
        self.path_db_times = self.path_dict["path_db_times"]
        self.path_db_assets = self.path_dict["path_db_assets"]


        ################################################
        ### 导入数据地址          
        self.path_data_pms = self.path_dict["path_data_pms"]
        self.path_data_adj = self.path_dict["path_data_adj"]
        self.path_fundpool = self.path_dict["path_fundpool"]
        self.path_wind_terminal = self.path_dict["path_wind_terminal"]  
        self.path_wsd = self.path_dict["path_wsd"] 
        self.path_wss = self.path_dict["path_wss"] 
        ### choice
        self.path_choice = self.path_dict["path_data_choice"]  

        ################################################
        self.nan = np.nan
        import datetime as dt  
        self.time_now = dt.datetime.now()
        ### 获取近1w、1m、3m、6m、1year日期
        self.time_now_str = dt.datetime.strftime(self.time_now , "%Y%m%d")


    def print_info(self):     
        ######################################################################
        ### 数据来源=windapi
        print("get_quote_ashares_month | 用windapi 分别下载“股票、指数、基金”的行情、估值、预期指标;基金基金净值、收益率、规模、排名。 ")  

        ######################################################################
        ### 数据来源=choice-api 
        print("get_quote_ashares_month_choice | 用choice-api 分别下载“股票、指数、基金”的行情、估值、预期指标;基金基金净值、收益率、规模、排名。 ")  


        ######################################################################
        ### manage 管理多日期、多个表格的数据下载
        print("manage_quote_ashares_month | 维护数据完整性excel和sql：不同资产、日期序列、逐期证券数量、指标 ")

        print("  |  ")   
    
    def get_quote_ashares_month(self, obj_data):
        ######################################################################
        ### 用windapi分别下载“股票、指数、基金”的行情、估值、预期指标。
        temp_date_start = obj_data["date_start"] 
        temp_date_pre_1d = obj_data["date_pre_1d"]  
        temp_date_end = obj_data["date_end"] 
        ### 
        type_asset = obj_data["type_asset"] 
        ### df_data 保存要更新的证券代码列表
        df_data = obj_data["df_data"] 
        temp_date_end =obj_data["trade_date"] 

        ####################################################################################
        ### 测试用：导入已经下载的excel数据 导入sql
        # obj_data["excel_sql"] = 1
        if "excel_sql" in obj_data.keys() :
            type_asset = "us"

            file_name = "month_"+ type_asset + "_shares_20220831.xlsx"
            df_temp = pd.read_excel( self.path_wss +file_name )
            print(df_temp.head().T)
            df_data = df_temp 
            df_data["name"] =df_data["code"] 
            ### fund
            # col_list = [ "code","nav","NAV_adj","return_m","peer_fund_return_rank_prop_per","periodreturnranking_1m","periodreturnranking_ytd"]
            ### Ashares
            # col_list =["code","pre_close","close","high","low","amt","pct_chg","ma16","ma16_pre","ma40","ma100","pe_ttm"]
            # col_list =col_list +["pb_mrq","pcf_ocf_ttm","mkt_cap","estpe_FY1","estpe_FY2","estpeg_FY1","estpeg_FY2","west_netprofit_YOY","west_avgnp_yoy","west_sales_YOY","west_sales_CAGR"]
            col_list = df_data.columns

            ### save to sql 
            from database import db_sqlite
            db_sqlite1 = db_sqlite()
            obj_db = {}
            obj_db["db_name"] = "db_quote.sqlite3"
            
            if type_asset == "fund" :
                obj_db["table_name"] = "quote_fund_month"
            else :
                obj_db["table_name"] = "quote_ashares_stock_fund_index_month"

            obj_db["insert_type"] = "df"
            df_table = df_data.loc[:,col_list]
            ##########################################
            ### 调整部分列
            df_table["type_asset"] = type_asset
            df_table["type_period"] = "m" 
            df_table["date"] = temp_date_end
            if not "name" in df_data.columns :
                if "名称" in df_data.columns :
                    df_data["name"] =df_data["名称"] 
                if "基金名称" in df_data.columns :
                    df_data["name"] =df_data["基金名称"]  

            obj_db["df_table"] = df_table  
            db_sqlite1.insert_table_data(obj_db )

            ### END 测试
            asdasd  

        #################################################################################### 
        #################################################################################### 
        ### step 2，下载行情、估值、预测数据 | wss一次只能1个代码*多个指标或多个代码*1个指标
        # code_list = df_data.loc[ 0:100, "代码"]
        # print("lenth of code_list:", len(code_list),code_list  )
        from get_wind_api import wind_api
        wind_api1 = wind_api()

        ######################################################################
        ## TEST 测试
        # obj_data["df_data"] =  df_data.head(12) 
        col_list = ["code"]
        
        # print( "df_data \n ", obj_data["df_data"] .T )

        ######################################################################################################
        ### 行情类指标，quote: | 月度频率 cycle=M 
        # dict_col_indi 是col_name和indicator_name的一一对应

        if type_asset in ["ashares","hk","index","us" ] :
            dict_col_indi = {} 
            dict_col_indi["pre_close"] = "pre_close"
            dict_col_indi["close"] = "close"
            dict_col_indi["high"] = "high"
            dict_col_indi["low"] = "low"
            dict_col_indi["amt"] = "amt"
            dict_col_indi["pct_chg"] = "pct_chg" 

            for temp_col in dict_col_indi.keys() :   
                obj_data["col_name"] = temp_col
                obj_data["indicator_name"] = dict_col_indi[ temp_col ]
                obj_data = wind_api1.get_wss_close_pctchg_amt( obj_data)
                ### 
                col_list = col_list + [temp_col]

            # print("Debug ",col_list )
            # print("Debug ",df_data )
            ### save to excel
            df_data = obj_data["df_data"]
            file_name2 = "month_"+ type_asset +"_shares_"+ temp_date_end +".xlsx"
            df_data.loc[:,col_list].to_excel( self.path_wss + file_name2  ,index=False )


        ######################################################################################################
        ### 均线类指标 |涉及月底上一个交易日， ma类：para_ma in ["16","40" ]
        if type_asset in ["ashares","hk","index","us" ] :
            dict_col_ma = {} 
            dict_col_ma["ma16"] =    ["16", temp_date_end]
            dict_col_ma["ma16_pre"] =["16", temp_date_pre_1d ]
            dict_col_ma["ma40"] =    ["40", temp_date_end]
            dict_col_ma["ma100"] =   ["100", temp_date_end]

            for temp_col in dict_col_ma.keys() :   
                obj_data["col_name"] = temp_col
                obj_data["para_ma"]  = dict_col_ma[ temp_col ][0]
                obj_data["trade_date"]  = dict_col_ma[ temp_col ][1]
                obj_data = wind_api1.get_wss_ma_n( obj_data)
                ### 
                col_list = col_list + [temp_col]
            
            ### save to excel
            df_data = obj_data["df_data"]
            file_name2 = "month_"+ type_asset +"_shares_"+ temp_date_end +".xlsx"
            df_data.loc[:,col_list].to_excel(self.path_wss + file_name2  ,index=False )
        


        ###################################################
        ### 估值类指标
        # A股指数只能提取到pe_ttm，pcf_ocf_ttm，提取不了pb_mrq，mkt_cap。感觉没什么用。 
        ''' 估值类：,"mkt_cap"
        # notes:股票市值用ev或mkt_cap、mkt_cap_ard都行，指数市值必须用mkt_cap_ard ；ev1是企业价值不等于市值。
        # notes:指数没有pb_mrq,pcf_ocf_ttm
        w.wss("000903.SH,300750.SZ", "pe_ttm,pb_mrq,pb_lf","tradeDate=20220831")
        w.wss("000903.SH,300750.SZ", "mkt_cap_ard","unit=1;tradeDate=20220831") 
        ''' 
        if type_asset in ["ashares","hk","us"] :
            obj_data["trade_date"] = temp_date_end

            dict_col_indi = {} 
            dict_col_indi["pe_ttm"] = "pe_ttm"
            dict_col_indi["pb_mrq"] = "pb_mrq"
            dict_col_indi["pcf_ocf_ttm"] = "pcf_ocf_ttm"
            dict_col_indi["mkt_cap"] = "mkt_cap" 

            for temp_col in dict_col_indi.keys() :   
                obj_data["col_name"] = temp_col
                obj_data["indicator_name"] = dict_col_indi[ temp_col ]
                obj_data = wind_api1.get_wss_close_pctchg_amt( obj_data)
                ### 
                col_list = col_list + [temp_col]

                
            ### save to excel
            df_data = obj_data["df_data"]
            file_name2 = "month_"+ type_asset +"_shares_"+ temp_date_end +".xlsx"
            df_data.loc[:,col_list].to_excel(self.path_wss + file_name2  ,index=False )


        ###################################################
        ### 预测类指标
        # notes：港股没有预期数据，除非是AH股票，不需要提取："estpe_","west_"
        '''预测类
        w.wsd("300146.SZ", "estpe_FY1,estpe_FY2,estpeg_FY1,estpeg_FY2,west_netprofit_YOY,west_avgnp_yoy,
            west_nproc_1w,west_nproc_4w,west_sales_YOY,west_sales_CAGR,", 
            "2022-08-01", "2022-08-31", "unit=1;currencyType=;year=2022;Period=M")
        1，w.wss("000903.SH,300750.SZ", "estpe_FY1","tradeDate=20220919")
        2，一致预期类：w.wss("000903.SH,300750.SZ", "west_sales_FY1","unit=1;tradeDate=20220919")
        3，w.wss("000903.SH,300750.SZ", "west_netprofit_YOY","tradeDate=20220919")
        4，盈利预测变化率1周和4周； w.wss("000903.SH,300750.SZ", "west_nproc_1w","year=2022;tradeDate=20220919")
        5，w.wss("000903.SH,300750.SZ", "west_sales_YOY,west_sales_CAGR","tradeDate=20220831")
        '''
        if type_asset in ["ashares" ] :
            obj_data["trade_date"] = temp_date_end
            dict_col_indi = {} 
            dict_col_indi["estpe_FY1"] = "estpe_FY1"
            dict_col_indi["estpe_FY2"] = "estpe_FY2"
            dict_col_indi["estpeg_FY1"] = "estpeg_FY1"
            dict_col_indi["estpeg_FY2"] = "estpeg_FY2"
            dict_col_indi["west_netprofit_YOY"] = "west_netprofit_YOY"
            dict_col_indi["west_avgnp_yoy"] = "west_avgnp_yoy"
            dict_col_indi["west_sales_YOY"] = "west_sales_YOY"
            dict_col_indi["west_sales_CAGR"] = "west_sales_CAGR"

            for temp_col in dict_col_indi.keys() :   
                obj_data["col_name"] = temp_col
                obj_data["indicator_name"] = dict_col_indi[ temp_col ]
                obj_data = wind_api1.get_wss_close_pctchg_amt( obj_data)
                ### 
                col_list = col_list + [temp_col]
                
                
            ### save to excel
            df_data = obj_data["df_data"]
            file_name2 = "month_"+ type_asset +"_shares_"+ temp_date_end +".xlsx"
            df_data.loc[:,col_list].to_excel(self.path_wss + file_name2  ,index=False )

        ######################################################################################################
        ### 基金类指标，行情quote和绩效 | 月度频率 
        # 单位净值,nav,复权单位净值,NAV_adj, 
        # 3种月收益率提取一样：近1月回报，return_1m；单月度回报，return_m,区间回报,return,2022-08-01；2022-08-31
        # 涉及"区间"的指标都要开始和结束日期：同类基金区间收益排名（百分比）peer_fund_return_rank_prop_per;
        # 近1月回报排名,periodreturnranking_1m;今年以来回报排名,periodreturnranking ytd
        # w.wss("519212.OF,166002.OF,004685.OF", "nav,NAV_adj,NAV_adj_return1,NAV_adj_chg,return_1m,return_m,return,
        # peer_fund_return_rank_prop_per,periodreturnranking_1m,periodreturnranking_ytd",
        # "tradeDate=20220831;startDate=20220801;endDate=20220831;annualized=0;fundType=2")
        # 如果不包含区间，那么 w.wss("519212.OF,166002.OF,004685.OF", "return_m,periodreturnranking_1m,periodreturnranking_ytd","tradeDate=20220831;fundType=2")
        # fundType=2 指的是基金的二级分类，

        if type_asset in ["fund" ] :
            obj_data["trade_date"] = temp_date_end
            dict_col_indi = {} 
            dict_col_indi["nav"] = "nav"
            dict_col_indi["NAV_adj"] = "NAV_adj"
            dict_col_indi["return_m"] = "return_m" 
            ### 获取基金多个代码的合并规模
            dict_col_indi["netasset_total"] = "netasset_total" 
            ### 基金排名
            dict_col_indi["peer_fund_return_rank_prop_per"] = "peer_fund_return_rank_prop_per"
            dict_col_indi["periodreturnranking_1m"] = "periodreturnranking_1m"
            dict_col_indi["periodreturnranking_ytd"] = "periodreturnranking_ytd" 
            
            for temp_col in dict_col_indi.keys() :   
                obj_data["col_name"] = temp_col
                obj_data["indicator_name"] = dict_col_indi[ temp_col ]
                obj_data = wind_api1.get_wss_fund_nav_rank( obj_data)
                ### 
                col_list = col_list + [temp_col]
                
            ##########################################        
            ### save to excel  
            df_data = obj_data["df_data"]
            file_name2 = "month_"+ type_asset +"_shares_"+ temp_date_end +".xlsx"
            df_data.loc[:,col_list].to_excel(self.path_wss + file_name2  ,index=False )


        ####################################################################################
        ### 调整部分列
        # col_list , df_data.loc[:,col_list]
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = {}
        obj_db["db_name"] = "db_quote.sqlite3"
        if type_asset in ["fund"] :
            obj_db["table_name"] ="quote_fund_month"
        else :
            obj_db["table_name"] = "quote_ashares_stock_fund_index_month"
        obj_db["insert_type"] = "df"
        df_table = df_data.loc[:,col_list]
        ##########################################
        ### 调整部分列
        df_table["type_asset"] = type_asset
        df_table["type_period"] = "m" 
        df_table["date"] = temp_date_end
        if not "name" in df_data.columns :
            if "名称" in df_data.columns :
                df_data["name"] =df_data["名称"] 
            if "基金名称" in df_data.columns :
                df_data["name"] =df_data["基金名称"] 

        ##########################################        
        ### save to excel  
        df_data = obj_data["df_data"]
        file_name2 = "month_"+ type_asset +"_shares_"+ temp_date_end +".xlsx"
        df_data.loc[:,col_list].to_excel(self.path_wss + file_name2  ,index=False )

        ##########################################
        ### save to sql 
        obj_db["df_table"] = df_table                

        db_sqlite1.insert_table_data(obj_db )


        return obj_data

   
    def get_quote_ashares_month_choice(self, obj_data):
        ######################################################################
        ### 用choice-api 分别下载“股票、指数、基金”的行情、估值、预期指标;基金基金净值、收益率、规模、排名。
        ### temp_date_start 是月初日期 如 220801
        temp_date_start = obj_data["date_start"] 
        temp_date_pre_1d = obj_data["date_pre_1d"]  
        temp_date_end = obj_data["date_end"] 
        ### 
        type_asset = obj_data["type_asset"] 
        ### df_data 保存要更新的证券代码列表
        df_data = obj_data["df_data"] 
        temp_date_end = obj_data["trade_date"] 
        ###
        col_list = ["code"]

        ####################################################################################
        ### 测试用：导入已经下载的excel数据 导入sql
        # obj_data["excel_sql"] = 1
        if "excel_sql" in obj_data.keys() :
            # type_asset = "us"

            # file_name = "month_"+ type_asset + "_shares_20220831.xlsx"
            # df_temp = pd.read_excel( self.path_choice +file_name )
            # print(df_temp.head().T)
            # df_data = df_temp 
            # df_data["name"] =df_data["code"] 
            # ### fund
            # # col_list = [ "code","nav","NAV_adj","return_m","peer_fund_return_rank_prop_per","periodreturnranking_1m","periodreturnranking_ytd"]
            # ### Ashares
            # # col_list =["code","pre_close","close","high","low","amt","pct_chg","ma16","ma16_pre","ma40","ma100","pe_ttm"]
            # # col_list =col_list +["pb_mrq","pcf_ocf_ttm","mkt_cap","estpe_FY1","estpe_FY2","estpeg_FY1","estpeg_FY2","west_netprofit_YOY","west_avgnp_yoy","west_sales_YOY","west_sales_CAGR"]
            # col_list = df_data.columns

            # ### save to sql 
            # from database import db_sqlite
            # db_sqlite1 = db_sqlite()
            # obj_db = {}
            # obj_db["db_name"] = "db_quote.sqlite3"
            
            # if type_asset == "fund" :
            #     obj_db["table_name"] = "quote_fund_month"
            # else :
            #     obj_db["table_name"] = "quote_ashares_stock_fund_index_month"

            # obj_db["insert_type"] = "df"
            # df_table = df_data.loc[:,col_list]
            # ##########################################
            # ### 调整部分列
            # df_table["type_asset"] = type_asset
            # df_table["type_period"] = "m" 
            # df_table["date"] = temp_date_end
            # if not "name" in df_data.columns :
            #     if "名称" in df_data.columns :
            #         df_data["name"] =df_data["名称"] 
            #     if "基金名称" in df_data.columns :
            #         df_data["name"] =df_data["基金名称"]  

            # obj_db["df_table"] = df_table  
            # db_sqlite1.insert_table_data(obj_db )

            ### END 测试
            a = 1 
            asdasd  

        #################################################################################### 
        ### step 2，下载行情、估值、预测数据 | wss一次只能1个代码*多个指标或多个代码*1个指标
        # code_list = df_data.loc[ 0:100, "代码"]
        # print("lenth of code_list:", len(code_list),code_list  )
        print("Debug sys.path", sys.path ) 

        from get_choice_api import choice_api
        choice_api1 = choice_api()        

        ######################################################################
        ### TEST 测试        # 
        # if not "c" in locals().keys() :
        #     from EmQuantAPI import c
        #     loginResult = c.start("ForceLogin=1", '', "")
        # para_ma=16 
        # trade_date = '20220630'
        # start_date = '20220531'
        # end_date = '20220630'
        # ### notes:,参数IsNaau=2 对应非年化的年初至今收益率。
        # para_str = "TradeDate=" + trade_date+ ",StartDate="+ start_date +",EndDate="+ end_date +",FundType=2, IsNaau=2,Ispandas=1"
        # print("para_str=", para_str )
        # code_list= ['000001.OF','000011.OF'] 
        # col_list_str = "NAVUNIT,NAVADJ,MRETURN,FUNDSCALE,NAVRETURNRANKINGP,NAVRETURNRANKINGPCTP,YTDRETURN"
        # # col_list_str = "NAVRETURNRANKINGP,NAVRETURNRANKINGPCTP,YTDRETURN"
        # df_temp = c.css( code_list , col_list_str , para_str)
        # print( df_temp )
        # ### '000300.SH','000688.SH','000852.SH','000903.SH','000905.SH','399006.SZ','399296.SZ','399324.SZ','399441.SZ','399673.SZ'
        # # 'IXIC.GI','DJI.GI','SPX.GI'
        # # 'HSCEI.HI','HSHCI.HI','HSHDYI.HI','HSI.HI','HSIII.HI','HSTECH.HI',  

        # adsasd 


        ######################################################################
        ### choice 港股代码整理
        if type_asset == "hk" :
            # Choice里港股代码"00941.HK" vs wind里的"0941.HK"
            obj_data["df_data"]["code_temp"] = obj_data["df_data"]["code"]
            obj_data["df_data"]["code"] = obj_data["df_data"]["code"].apply(lambda x : "0"+ str(x) )
            

        ######################################################################################################
        ### 行情类指标，quote: | 中港美指数是一样的
        # dict_col_indi 是col_name和indicator_name的一一对应
        ### notes: Choice里港股代码"00941.HK" vs wind里的"0941.HK"

        if type_asset in ["ashares","hk","index","us" ] :
            ### choice数据和wind数据的一一对应
            # notes：choice数据返回的df都是大写的column name ；str1.upper()
            dict_col_indi = {} 
            dict_col_indi["pre_close"] = "PreCloseM".upper()
            # dict_col_indi["pre_close"] = "PreCloseM"
            dict_col_indi["close"] = "CloseM".upper()
            dict_col_indi["high"] = "HighM".upper()
            dict_col_indi["low"] = "LowM".upper()
            ### 美股没有成交金额，指数部分有部分没有
            if not type_asset in ["us" ] : 
                dict_col_indi["amt"] = "AmountM".upper()
            dict_col_indi["pct_chg"] = "DifferRangeM".upper()
            ### 非必须
            dict_col_indi["open"] = "OpenM"
            ### ### notes:指数没有 AVGPRICEM, || col_list_str = "PRECLOSEM,OPENM,HIGHM,LOWM,CLOSEM,DIFFERRANGEM,AMOUNTM"
            if not type_asset in ["index","us" ] :
                dict_col_indi["aveprice"] = "AvgPriceM"
            

            ### col_list是导出用的wind格式列，包含'code';col_list_choice是获取api数据用的
            col_list_choice = []
            for temp_col in dict_col_indi.keys() : 
                col_list = col_list + [temp_col]
                col_list_choice = col_list_choice  + [ dict_col_indi[temp_col] ] 
            
            obj_data["dict_col"] = dict_col_indi
            ######
            col_list_str = ",".join( col_list_choice )
            obj_data["col_list_str"] = col_list_str.upper()
            
            ######################################################################
            ### 
            obj_data = choice_api1.get_css_stock_month( obj_data) 

            # print("Debug ",col_list )
            ######################################################################
            ### save to excel
            ### 输出的columns= CODES	DATES	PRECLOSEM	OPENM	HIGHM	LOWM	CLOSEM	AVGPRICEM	DIFFERRANGEM	AMOUNTM
            df_data = obj_data["df_data"]
            print( "Debug,col_list=" ,col_list)
            df_data.to_excel("D:\\df_data.xlsx")
            file_name2 = "month_"+ type_asset +"_shares_"+ temp_date_end +".xlsx"            
            print("path-output=",self.path_choice + file_name2 )
            df_data.loc[:,col_list].to_excel( self.path_choice + file_name2  ,index=False )

        ######################################################################################################
        ### 均线类指标 |涉及月底上一个交易日， ma类：para_ma in ["16","40" ] 
        ### notes: 港股没有MA指标，因为证券代码和指标适用范围不匹配所以会报这个错。10000013
        ### A股指数和港股指数有均线数据，但是美股没有
        if type_asset in ["ashares","index" ] :
            dict_col_ma = {} 
            dict_col_ma["ma16"] =    ["16", temp_date_end]
            dict_col_ma["ma16_pre"] =["16", temp_date_pre_1d ]
            dict_col_ma["ma40"] =    ["40", temp_date_end]
            dict_col_ma["ma100"] =   ["100", temp_date_end]             
            obj_data["dict_col"] = dict_col_ma
            ### notes:不需要col_name,因为指标只有1个 "MA"
            for temp_col in dict_col_ma.keys() :    
                obj_data["col_ma"] = temp_col
                obj_data["para_ma"]  = dict_col_ma[ temp_col ][0]
                obj_data["trade_date"]  = dict_col_ma[ temp_col ][1]
                obj_data = choice_api1.get_css_ma_n( obj_data)
                ### 
                col_list = col_list + [temp_col]
            
            ### save to excel
            df_data = obj_data["df_data"]
            df_data.to_excel("D:\\df_data.xlsx")
            file_name2 = "month_"+ type_asset +"_shares_"+ temp_date_end +".xlsx"            
            print("path-output=",self.path_choice + file_name2 )
            df_data.loc[:,col_list].to_excel(self.path_choice + file_name2  ,index=False )
        

        ###################################################
        ### 估值类指标 
        # A股指数只能提取到pe_ttm，pcf_ocf_ttm，提取不了pb_mrq，mkt_cap。感觉没什么用。  
        if type_asset in ["ashares"] :
            obj_data["trade_date"] = temp_date_end
            # notes：choice数据返回的df都是大写的column name ；str1.upper()
            ### notes:港股没有 PBMRQN，PCFCFOTTM,MVBYCSRC
            dict_col = {} 
            dict_col["pe_ttm"] = "PETTM".upper()
            dict_col["pb_mrq"] = "PBMRQN".upper()
            dict_col["pcf_ocf_ttm"] = "PCFCFOTTM".upper()
            dict_col["mkt_cap"] = "MVBYCSRC".upper()
            ### 非必须
            # 预测市盈率(PE,最新预测)	ESTPENEW ;总市值(证监会算法)	MVBYCSRC,区间日均总市值	PERIODAVGMV
            # 最新股息率,DIVIDENDYIELDNEW,市研率(PRR,LYR)(按最近公告日),PRRLYRN
            # 区间市盈率PETTM最高值	HIGHPETTMP , 区间市盈率PETTM最低值	LOWPETTMP 
            obj_data["dict_col"] = dict_col
            obj_data = choice_api1.get_css_mv_pe( obj_data)

            for temp_col in dict_col.keys() :   
                col_list = col_list + [temp_col]

                
            ### save to excel
            df_data = obj_data["df_data"]
            df_data.to_excel("D:\\df_data.xlsx")
            file_name2 = "month_"+ type_asset +"_shares_"+ temp_date_end +".xlsx"            
            print("path-output=",self.path_choice + file_name2 )
            df_data.loc[:,col_list].to_excel( self.path_choice + file_name2  ,index=False )

        ###################################################
        ### 预测类指标
        # notes：港股没有预期数据，除非是AH股票，不需要提取："estpe_","west_" 
        if type_asset in ["ashares" ] :
            obj_data["trade_date"] = temp_date_end
            dict_col = {} 
            # notes：choice数据返回的df都是大写的column name ；str1.upper()
            ### 一致预测归属母公司净利润(FY1) ; 一致预测归属母公司净利润(FY2)
            dict_col["estpe_FY1"] = "SESTNIFY1"
            dict_col["estpe_FY2"] = "SESTNIFY2"
            dict_col["estpe_FY3"] = "SESTNIFY3"
            ### 一致预测盈利增长同比
            dict_col["west_netprofit_YOY"] = "SESTNIYOY"
            ### 一致预测盈利2年复合增长率
            dict_col["west_avgnp_yoy"] = "SESTNICGR2"
            ### 一致预测营业收入同比
            dict_col["west_sales_YOY"] = "SESTGRYOY"
            ### 一致预测营业收入2年符合增长率
            dict_col["west_sales_CAGR"] = "SESTGRCOMPOUNDGROWTHRATE2"
            ### 其他 一致预测归属母公司净利润1周变化率	SESTNICHGPCT1W; 一致预测归属母公司净利润4周变化率	SESTNICHGPCT4W
            ### 一致预测ROE(FY1)	SESTROEFY1 , 一致预测ROE(FY2)	SESTROEFY2
            ### 非必须
            # 预测市盈率(PE,最新预测)	ESTPENEW ;总市值(证监会算法)	MVBYCSRC,区间日均总市值	PERIODAVGMV
            # 最新股息率,DIVIDENDYIELDNEW,市研率(PRR,LYR)(按最近公告日),PRRLYRN
            # 区间市盈率PETTM最高值	HIGHPETTMP , 区间市盈率PETTM最低值	LOWPETTMP 


            ###################################################
            ### 其中 "estpeg_FY1","estpeg_FY2" 需要进一步计算
            temp_list = ["estpe_FY1","estpe_FY2","estpeg_FY1","estpeg_FY2","west_netprofit_YOY","west_avgnp_yoy","west_sales_YOY","west_sales_CAGR" ]
            for temp_col in temp_list :   
                col_list = col_list + [temp_col]
            ###################################################
            ### 
            obj_data["dict_col"] = dict_col
            obj_data = choice_api1.get_css_estimate( obj_data)
            df_data = obj_data["df_data"]

            ###################################################
            ### 需要二次计算的指标， choice没有现成的数据
            ### peg没有数据~~  |notes： peg=pe/growth , PEG=PE/(企业年盈利增长率/100)
            df_data["estpeg_FY1"] = df_data["pe_ttm"]/ df_data["west_netprofit_YOY"] 
            df_data["estpeg_FY2"] = df_data["pe_ttm"]/ df_data["west_avgnp_yoy"] 
            obj_data["df_data"] = df_data

            ### save to excel            
            df_data.to_excel("D:\\df_data.xlsx")
            file_name2 = "month_"+ type_asset +"_shares_"+ temp_date_end +".xlsx"            
            print("path-output=",self.path_choice + file_name2 )
            df_data.loc[:,col_list].to_excel( self.path_choice + file_name2  ,index=False )


        ######################################################################################################
        ### 基金类指标，行情quote和绩效 | 月度频率 
        # 单位净值,nav,复权单位净值,NAV_adj, ；3种月收益率提取一样：近1月回报，return_1m；单月度回报，return_m,区间回报,return,2022-08-01；2022-08-31
        # 涉及"区间"的指标都要开始和结束日期： 

        if type_asset in ["fund" ] :
            obj_data["trade_date"] = temp_date_end
            ### 需要输入上个月末日期：temp_date_start 
            obj_data["start_date"]= temp_date_start 
            obj_data["end_date"]  = temp_date_end
            dict_col_indi = {} 
            dict_col_indi["nav"] = "NAVUNIT"
            dict_col_indi["NAV_adj"] = "NAVADJ"
            # 单月度回报	MRETURN || 近1月回报,MONTHLYRETURN,需要参数 IsNaau=2
            dict_col_indi["return_m"] = "MRETURN" 

            ### 获取基金多个代码的合并规模 |基金规模,FUNDSCALE,EndDate=2022-10-11 | 基金资产净值	PRTNETASSET	ReportDate=2021-12-31
            dict_col_indi["netasset_total"] = "FUNDSCALE" 
            ### 基金排名: 同类基金区间收益排名（百分比）,peer_fund_return_rank_prop_per;
            # 同类基金区间收益排名(百分比),NAVRETURNRANKINGPCTP 
            # NAVRETURNRANKINGPCTP      41.9535      22.3154
            dict_col_indi["peer_fund_return_rank_prop_per"] = "NAVRETURNRANKINGPCTP"
            ### wind：近1月回报排名,periodreturnranking_1m;今年以来回报排名,periodreturnranking ytd
            ### choice：同类基金区间收益排名,NAVRETURNRANKINGP, StartDate=2022-08-31,EndDate=2022-09-30,FundType=2
            # NAVRETURNRANKINGP       2852/6798    1517/6798
            dict_col_indi["periodreturnranking_1m"] = "NAVRETURNRANKINGP"
            ### TODO 今年以来回报,YTDRETURN,参数IsNaau=2 
            dict_col_indi["periodreturnranking_ytd"] = "YTDRETURN" 

            ### todo:年初至今同类排名 periodreturnranking_ytd；需要进一步计算
            ### choice其他：区间单位净值增长	NAVUNITCHGP;复权单位净值增长率	NAVADJRETURN 	
            ################################################
            ###  
            obj_data["dict_col"] = dict_col_indi
            obj_data = choice_api1.get_css_fund_nav_rank( obj_data)

            ################################################
            ### 添加到全部的列表里
            for temp_col in dict_col_indi.keys() :   
                col_list = col_list + [temp_col]

            ################################################
            ### save to excel
            df_data = obj_data["df_data"]
            df_data.to_excel("D:\\df_data.xlsx")
            file_name2 = "month_"+ type_asset +"_shares_"+ temp_date_end +".xlsx"            
            print("path-output=",self.path_choice + file_name2 )
            df_data.loc[:,col_list].to_excel( self.path_choice + file_name2  ,index=False )



        ####################################################################################
        ### 调整部分列
        # col_list , df_data.loc[:,col_list]
        from database import db_sqlite
        db_sqlite1 = db_sqlite()
        obj_db = {}
        obj_db["db_name"] = "db_quote.sqlite3"
        if type_asset in ["fund"] :
            obj_db["table_name"] ="quote_fund_month"
        else :
            obj_db["table_name"] = "quote_ashares_stock_fund_index_month"
        obj_db["insert_type"] = "df"
        df_table = df_data.loc[:,col_list]
        ##########################################
        ### 调整部分列
        df_table["type_asset"] = type_asset
        df_table["type_period"] = "m" 
        df_table["date"] = temp_date_end
        if not "name" in df_data.columns :
            if "名称" in df_data.columns :
                df_data["name"] =df_data["名称"] 
            if "基金名称" in df_data.columns :
                df_data["name"] =df_data["基金名称"] 

        ######################################################################
        ### choice 港股代码整理,再转会wind格式
        if type_asset == "hk" :
            # Choice里港股代码"00941.HK" vs wind里的"0941.HK"
            obj_data["df_data"]["code"] = obj_data["df_data"]["code_temp"]
            
        ##########################################        
        ### save to excel  
        df_data = obj_data["df_data"]
        df_data.to_excel("D:\\df_data.xlsx")
        file_name2 = "month_"+ type_asset +"_shares_"+ temp_date_end +".xlsx"            
        print("path-output=",self.path_choice + file_name2 )

        ##########################################
        ### save to sql | 先不自动存入sql，怕覆盖了正确的数据
        obj_db["df_table"] = df_table                

        # db_sqlite1.insert_table_data(obj_db ) 

        return obj_data


    def manage_quote_ashares_month(self, obj_data):
        ##########################################################################
        ### 维护数据完整性excel和sql：不同资产、日期序列、逐期证券数量、指标
        ### 数据来源类型： wind_api, choice_api, wind_wds
        if not "data_source" in obj_data.keys():
            ### 默认wind_api
            data_source = "wind_api"
        else :
            ### choice_api
            data_source = obj_data["data_source"]
        ############################################ 
        ### 输入项：
        # month_start = 20190101 |   month_end = int( self.time_now_str )
        month_start = int( obj_data["date_begin"] )
        month_end =   int( obj_data["date_end"] )
        if "type_asset" in obj_data.keys() :
            type_asset_list = [ obj_data["type_asset"] ]
        else :
            ### 默认对所有资产下载数据
            type_asset_list =  ["ashares","hk","index","us","fund" ] 


        ##########################################################################   
        ### step 0，月末日期的维护，从202208 to 201908 
        ### 导入月末日期数据，file_dt=date_trade.xlsx; path= C:\rc_202X\rc_202X\ciss_web\CISS_rc\db\db_times
        file_dt="date_trade.xlsx"
        # path_dt = "C:\\rc_202X\\rc_202X\\ciss_web\\CISS_rc\\db\\db_times\\"
        df_dt = pd.read_excel( self.path_db_times + file_dt )
        df_dt = df_dt[ df_dt["exchange"] =="SSE" ]
        df_dt = df_dt[ df_dt["type_date"] =="m" ]
        ### 
        print( df_dt.head() ) 

        df_dt_sub = df_dt[ df_dt["date"] >= month_start  ]
        df_dt_sub = df_dt_sub[ df_dt_sub["date"] <= month_end  ]
        ### 最新的日期在前
        df_dt_sub = df_dt_sub.sort_values(by="date",ascending=False  )

        print("Debug month_end=",month_start, month_end )
        print( df_dt_sub.head() )
        
        ##########################################################################   
        ### 日期 * 资产
        for temp_date_month in df_dt_sub["date"] :
            ### temp_date_month : int to str 
            temp_date_month = str( temp_date_month)
            
            ##############################################
            ### 生成月初日期
            temp_date_start = temp_date_month[:6] +"01"
            temp_date_end   = temp_date_month
            ### 获取给定月末日期的前1个交易日，为了计算动量ma指标
            df_temp = df_dt[ df_dt["date"]<int( temp_date_month )  ]
            temp_date_pre_1d = df_temp["date"].values[-1]
            temp_date_pre_1d = str( temp_date_pre_1d )
            print("temp_date_month ",temp_date_start, temp_date_pre_1d , temp_date_end )

            obj_data["date_start"] = temp_date_start
            obj_data["date_pre_1d"] = temp_date_pre_1d
            obj_data["date_end"] = temp_date_end

            ##########################################################################
            ### 对于不同类型的资产
            for type_asset in type_asset_list :
                print("type_asset:", type_asset ,temp_date_month ) 
                obj_data["type_asset"] = type_asset

                ######################################################################
                ### step 1,确定要获取数据的股票列表 | 每3个月从Wind导出一次股票列表
                ### 导入给定日期前最近一期的A股列表、港股通港股列表、指数列表、基金列表  
                ###################################
                ### A股列表 
                # 剔除A股总市值后20%、小于90亿元。名称里带有st，但是有的st是历史包袱已经重组了。 
                # 202201以后的每个月导出A股数据，202201之前的用"全部A股-上市日期_20220922.xlsx"
                if type_asset == "ashares" :
                    ### 寻找日期名称匹配 "a_shares_202201" 的文件 
                    import os 
                    file_list = os.listdir( self.path_data_adj )
                    temp_str =  "a_shares_" + str(temp_date_end )[:6]
                    file_list2 = [ i for i in file_list if temp_str in i ]
                    # 例如 ['a_shares_20220121.xlsx']
                    if len( file_list2) > 0 :
                        file_name = file_list2[-1 ]
                        df_data = pd.read_excel( self.path_data_adj + file_name )
                        ### 以总市值90亿为标准，A股股票数量从3500 to 1655，2022-8
                        # 获取前95%最大的值， np.percentile( df1[col_name],95)
                        df_data = df_data[ df_data["总市值1"] > 90*10000*10000 ]
                        
                    else :
                        ### 历史数据统一用 "全部A股-上市日期_20220922.xlsx"里市值大于100亿元，且上市日期小于月末日期的公司
                        file_name = "全部A股-上市日期_20220922.xlsx"
                        df_data = pd.read_excel( self.path_wind_terminal + file_name )
                        ### 以总市值90亿为标准，A股股票数量从3500 to 1655，2022-8
                        # 获取前95%最大的值， np.percentile( df1[col_name],95)
                        df_data = df_data[ df_data["总市值1"] > 90*10000*10000 ]
                        df_data = df_data[ df_data["上市日期"] < int( temp_date_end ) ]
                        df_data = df_data.sort_values( by="上市日期",ascending=True  )
                
                ###################################
                ### 港股列表：港股通  
                # notes：港股没有预期数据，除非是AH股票，不需要提取："estpe_","west_"
                # w.wsd("0700.HK", "pre_close,close,high,low,amt,west_pe,est_peg,west_netprofit_YOY,west_avgnp_yoy,west_nproc_1w,west_nproc_4w,west_sales_YOY,west_sales_CAGR,ev,pe_ttm,pcf_ocf_ttm,estpe_FY1,estpe_FY2", "2022-08-31", "2022-08-31", "year=2022;westPeriod=180;rptYear=2022;unit=1;Period=M")

                if type_asset == "hk" :
                    ### 寻找日期名称匹配 "a_shares_202201" 的文件 
                    import os 
                    file_list = os.listdir( self.path_data_adj )
                    temp_str =  "h_shares_" + str(temp_date_end )[:6]
                    file_list2 = [ i for i in file_list if temp_str in i ]
                    # 例如 ['a_shares_20220121.xlsx']
                    if len( file_list2) > 0 :
                        file_name = file_list2[-1 ]
                        df_data = pd.read_excel( self.path_data_adj + file_name )
                        ### 以总市值90亿为标准，A股股票数量从3500 to 1655，2022-8
                        # 获取前95%最大的值， np.percentile( df1[col_name],95)
                        df_data = df_data[ df_data["总市值1"] > 90*10000*10000 ]
                        
                    else :
                        ### 历史数据统一用 "全部A股-上市日期_20220922.xlsx"里市值大于100亿元，且上市日期小于月末日期的公司
                        file_name = "全部港股-上市日期_20220922.xlsx"
                        df_data = pd.read_excel( self.path_wind_terminal + file_name )
                        ### 以总市值90亿为标准，A股股票数量从3500 to 1655，2022-8
                        # 获取前95%最大的值， np.percentile( df1[col_name],95)
                        df_data = df_data[ df_data["总市值1"] > 90*10000*10000 ]
                        df_data = df_data[ df_data["上市日期"] < int( temp_date_end ) ]
                        df_data = df_data.sort_values( by="上市日期",ascending=True  )
                
                ###################################
                ### 美股列表：标普500成分股 TODO
                # w.wss("ABMD.O,AAPL.O", "close,ev,amt,MA,val_pettm_low,val_pettm_high","tradeDate=20220831;priceAdj=F;cycle=D;unit=1;MA_N=40;startDate=20220801;endDate=20220831")
                # type_asset = "us" | file_name = SP500成份_20220920.xlsx 

                if type_asset == "us" :
                    ### 寻找日期名称匹配 "a_shares_202201" 的文件 
                    import os 
                    file_list = os.listdir( self.path_data_adj )
                    temp_str =  "SP500成份_" + str(temp_date_end )[:6]
                    file_list2 = [ i for i in file_list if temp_str in i ]
                    # 例如 ['a_shares_20220121.xlsx']
                    if len( file_list2) > 0 :
                        file_name = file_list2[-1 ]
                        df_data = pd.read_excel( self.path_data_adj + file_name )
                        ### 以总市值90亿为标准，A股股票数量从3500 to 1655，2022-8
                        # 获取前95%最大的值， np.percentile( df1[col_name],95)
                        df_data = df_data[ df_data["总市值1"] > 90*10000*10000 ]
                        
                    else :
                        ### 历史数据统一用 "全部A股-上市日期_20220922.xlsx"里市值大于100亿元，且上市日期小于月末日期的公司
                        file_name = "SP500成份-上市日期_20220922.xlsx"
                        df_data = pd.read_excel( self.path_wind_terminal + file_name )
                        ### 以总市值90亿为标准，A股股票数量从3500 to 1655，2022-8
                        # 获取前95%最大的值， np.percentile( df1[col_name],95) 
                        df_data = df_data[ df_data["上市日期"] < int( temp_date_end ) ]
                        df_data = df_data.sort_values( by="上市日期",ascending=True  ) 

                ###################################
                ### 指数列表：每半年确定一次 |notes:如果不是wind数据需要简化指数列表
                # 导入定期梳理的指数列表，sheet=index_list,file=db_manage.xlsx
                # 只筛选A股指数 type=ashares  ;code=000300.SH ; name=沪深300
                # A股指数只能提取到pe_ttm，pcf_ocf_ttm，提取不了pb_mrq，mkt_cap。感觉没什么用。 
                if type_asset == "index" : 
                    file_import = "db_manage.xlsx"
                    df_data = pd.read_excel( self.path_ciss_exhi + file_import,sheet_name="index_list" )
                    # if not data_source == "wind_api" :
                    #     df_data =df_data[ df_data["if_wind"] ==1  ]
                    if data_source == "choice_api" :    
                        ### 去除 code_choice 列中的空值        
                        df_data["code"] = df_data["code_choice"]  
                        df_data["code"] = df_data["code"].apply( lambda x : x if len( str(x) )>5 else 0)
                        df_data =df_data[ df_data["code"] !=0  ]
                        print("Index list = \n", df_data )  

                ###################################
                ### 基金列表：最近4个季度的基础池 |2022年以前采用前一年末的数据,如"基金池rc_主动股票_20201231.xlsx"" 
                # 核心池100个、基础池400个或前20%基金数量。
                if type_asset == "fund" : 
                    ### 获取上一年份数据
                    temp_year = str(temp_date_end)[:4]
                    temp_year_pre = str( int(temp_year)-1 )
                    count_f = 0 
                    fund_list=["主动股票","偏债混合" ]
                    ### 判断是否 20161231 之前的日期,如果是之前，需要判断基金的上市日期
                    if int(temp_date_end) < 20161231 :
                        temp_year_pre = "2016"
                        ### 假设需要2011年的基金，年份差异 5=2016-2011
                        year_diff = 2016 - int(str(temp_date_end)[:4] ) 
                        ### 导入的sheet改为 "raw_data",并提取成立年限
                        for temp_fund in fund_list : 
                            file_name = "基金池rc_" + temp_fund+ "_" + temp_year_pre +"1231.xlsx"
                            df_temp = pd.read_excel( self.path_fundpool + file_name,sheet_name="raw_data" )
                            ### 选取前25%数量的基金，默认综合指标得分降序排列
                            num_code = round( len(df_temp.index)*0.25 )
                            df_temp = df_temp.iloc[ :num_code, : ]
                            ### 成立年限，筛选成立时间满足要求的品种。
                            df_temp = df_temp[ df_temp["成立年限"] > year_diff+1 ]

                            if count_f == 0 :
                                df_data = df_temp 
                                count_f  =1 
                            else :
                                df_data = df_data.append(df_temp  , ignore_index=True ) 


                    else :                     
                        for temp_fund in fund_list : 
                            file_name = "基金池rc_" + temp_fund+ "_" + temp_year_pre +"1231.xlsx"
                            df_temp = pd.read_excel( self.path_fundpool + file_name,sheet_name="基础池" )
                            if count_f == 0 :
                                df_data = df_temp
                                count_f  =1 
                            else :
                                df_data = df_data.append(df_temp,ignore_index=True ) 

                ######################################################################
                ### 保存 df_data 到 obj_data          
                if not "code" in df_data.columns :
                    if "代码" in df_data.columns :
                        df_data["code"] =df_data["代码"] 
                    if "基金代码" in df_data.columns :
                        df_data["code"] =df_data["基金代码"] 

                obj_data["df_data"] =  df_data
                obj_data["trade_date"] = temp_date_end 

                ##########################################################################
                ### 从wind-api获取数据,并存入Excel和存入sql
                # ntoes:不能用同一个名字，否则会报错
                if data_source == "wind_api" :
                    obj_data_output = self.get_quote_ashares_month( obj_data )
                elif data_source == "choice_api" :
                    obj_data_output = self.get_quote_ashares_month_choice( obj_data )

                ##########################################################################
                ###  


        ##########################################################################
        ### 
        obj_data = obj_data_output


        return obj_data




