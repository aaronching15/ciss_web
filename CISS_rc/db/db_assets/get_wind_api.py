# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
=============================================== 

功能：用WindPy模块获取API数据;
    1,class wind_api : 获取wsq,wss,wset,wsd等数据
    2,class wind_api_pms :获取wpf等数据

数据来源： Wind-API 万得量化数据接口
last update 230421 | since  160121
derived from  rC_Data_Initial.py with get-Wind.py gradually.
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

class wind_api():
    ### 获取Wind的PMS相关数据:wpf,wps,wpd,wupf
    def __init__(self):
        ### 获取wpf相关数据
        ##########################################
        ### 导入配置文件对象，例如path_db_wind等
        from config_data import config_data 
        self.obj_config = config_data().obj_config

        self.nan = np.nan 
        self.path_ciss_web = os.getcwd().split("ciss_web")[0]+"ciss_web\\"
        self.path_ciss_rc = self.path_ciss_web +"CISS_rc\\"
        self.path_dt = self.path_ciss_rc + "db\\db_times\\"
        ### 
        self.path_pms =  os.getcwd().split("ciss_web")[0]+"\\data_pms\\"  
        self.path_wpf = self.path_pms + "wpf\\"
        self.path_wpd = self.path_pms + "wpd\\"
        self.path_wsd = self.path_pms + "wsd\\"

        ##########################################
        ### 时间相关变量
        import datetime as dt  
        self.time_now = dt.datetime.now()
        self.time_pre =  self.time_now - dt.timedelta(days=1) , "%Y%m%d" 
        self.time_pre10 =  self.time_now - dt.timedelta(days=10) , "%Y%m%d" 
        ### 获取近1w、1m、3m、6m、1year日期
        self.time_now_str = dt.datetime.strftime(self.time_now   , "%Y%m%d")
        self.time_now_str2 = dt.datetime.strftime(self.time_now   , "%Y-%m-%d")
        ###
        self.time_pre_str = dt.datetime.strftime(self.time_now - dt.timedelta(days=1) , "%Y%m%d")
        self.time_pre10_str = dt.datetime.strftime(self.time_now - dt.timedelta(days=10) , "%Y%m%d")
        
        ##########################################
        ### 导入日期数据  获取日期参数：如最近月末、季度末、半年度末数据
        from times import times
        times1 = times()
        self.obj_date = times1.get_date_pre_post( self.time_now_str ) 
        ### 如 self.obj_date["date_pre_3y_str"]        
        
        ### output: pre 给定日期之前的最近月末和2个季末、2个半年末；str和dt格式
        # obj_date["date_pre_1m_end"] obj_date["date_pre_1m_end_str"]
        # obj_date["date_pre_1q_end_str"] obj_date["date_pre_1q_end"] 
        # obj_date["date_pre_2q_end_str"] obj_date["date_pre_2q_end"] 
        # obj_date["date_pre_1halfyear_end_str"] obj_date["date_pre_1halfyear_end"]
        # obj_date["date_pre_2halfyear_end_str"] obj_date["date_pre_2halfyear_end"]
        

        ##########################################
        ### 每次调用API，要保存数据提取数量，保存到外部文件 
        self.api_count ={} 

    def print_info(self):
        ### print all modules for current class
        ###################################################
        ### WSS 多维数据 | 单一时点，多代码多指标
        print("get_wss_ma_n | 均线：给定日期、均线参数和代码列表，获取股票和指数的均线数据 ")
        print("get_wss_close_pctchg_amt | 行情和估值：给定日期、周期参数和代码列表，获取股票和指数的收盘价、涨跌幅、成交额、市值和PE_ttm")
        print("get_wss_estimate | 预测指标：给定日期、和代码列表，获取股票的FY1,FY2的一致预测指标 ")
        print("get_wss_fund_nav_rank | 预测指标：给定日期、和代码列表，获取基金净值、区间收益率、区间排名 ")

        ###################################################
        ### portfolio
        print("get_wpf | 获取PMS组合持仓数据。")
        print("get_wps | 获取PMS组合区间涨跌幅、回撤、Alpha、Sharpe等绩效指标。")
        print("get_wpd | 获取PMS组合日期序列的总资产和盈亏等 ")
        print("get_wupf |  ")

        ###################################################
        ### quote and indicators
        print("---------------------------------------------------------------------- ")
        print("get_wss_ma_amt_mv | 给定含代码的df，获取特定价量数据 ")
        print("get_wss_pct_chg_period | 给定含代码的df，获取多个区间涨跌幅 ")
        print("get_wss_fund_1date | 给定1个基金代码及1个日期，获取多个不同基金指标 ")
        print("---------------------------------------------------------------------- ")
        print("get_wsd_period | 给代码和收盘价等指标，获取区间内每个交易日的指标数据,并合并保存到xlsx文件 ")

        ###################################################
        ### fund performance | 基金相关
        print("---------------------------------------------------------------------- ")
        print("get_wss_fund_perf | 给定基金代码、区间、获取基金和基金经理绩效指标 ")
        print("get_wsd_fund_unit | 给定基金代码、区间、获取基金净值 ") 
        print("---------------------------------------------------------------------- ")
        
        ###################################################
        ### Index benchmark performance | 指数，基准相关
        print("get_index_indi_data |给定基金基准指数代码、日期，获取月度收益率") 


        ###################################################
        ### time  
        print("get_tdays | 获取日期 ")

        ###################################################
        print("save_api_count | 保存api_count 指标到excel ")
    
    ######################################################################################################
    def save_api_count(self):
        ### 保存api_count 指标到excel 

        ##########################################
        ### 每次调用API，要保存数据提取数量，保存到外部文件 
        file_name = "api_count.xlsx"
        sheet = "api_count" 
        df_api = pd.read_excel(self.path_pms + file_name,sheet_name= sheet ) 
        ### columns date wds	wss	wset；wps；wpd
        df_api = df_api.drop_duplicates(subset=["date"], keep="last"  )
        df_api.index = df_api["date"] 

        ##########################################
        ###
        for temp_key in self.api_count.keys() :
            if self.time_now_str in df_api.index :
                ### 判断 "wsd" 是否在columns里
                if temp_key in df_api.columns :
                    df_api.loc[self.time_now_str,temp_key] = df_api.loc[self.time_now_str,temp_key] + self.api_count[temp_key]
                else :
                    df_api[temp_key] = 0 
                    df_api.loc[self.time_now_str,temp_key] = df_api.loc[self.time_now_str,temp_key] + self.api_count[temp_key]
                
            else :
                if temp_key in df_api.columns :
                    ### define new index  
                    df_api.loc[self.time_now_str, : ] = 0 
                    df_api.loc[self.time_now_str,temp_key] = self.api_count[temp_key]
                else :
                    ### define new index and new column
                    df_api.loc[self.time_now_str, : ] = 0 
                    df_api[temp_key] = 0 
                    df_api.loc[self.time_now_str,temp_key] =  self.api_count[temp_key]
        
        ### save date 
        df_api.loc[self.time_now_str, "date"] = self.time_now_str

        df_api.to_excel( self.path_pms + "api_count.xlsx",sheet_name="api_count" ,index=False)


        return 1
    
    ######################################################################################################
    ### WSS
    def get_wss_ma_n(self, obj_data ):
        ### 给定日期、均线参数和代码列表，获取股票和指数的均线数据
        ### trade_date是给定的交易日期、para_ma是均线的参数、df_data是包含名为"code"代码列的df
        ### col_name是保存获取的指标值的列名称，如ma100，ma40-pre等。
        trade_date = obj_data["trade_date"]
        para_ma = obj_data["para_ma"]
        df_data = obj_data["df_data"] 
        col_name = obj_data["col_name"] 
        ### 要把index都设置为code
        df_data.index = df_data["code"]

        ###################################################
        ### 每次下载100个 | 测试的时候把单次数量改为2个
        para_num_code =  100
        num_100 = len( df_data.index )//para_num_code +1

        ### TODO 测试用
        # para_num_code = 3
        # num_100 = 2  

        from WindPy import w
        w.start() 
        count_api = 0 
        ###################################################
        for temp_i in range( num_100 ) :
            print("Indicator name= ma"," Working on codes: ",temp_i*para_num_code ,(temp_i+1)*para_num_code  )
            ### 0,1,...,46
            sub_index = df_data.index[ temp_i*para_num_code :(temp_i+1)*para_num_code ]
            code_list = list( df_data.loc[ sub_index , "code"   ] )

            ### 获取均线数据,短期
            # str_para = "tradeDate="+ date_latest +";MA_N="+ para_ma +";priceAdj=F;cycle=D"
            str_para = "tradeDate="+ str(trade_date) +";MA_N="+ para_ma +";priceAdj=F;cycle=D"  
            obj1 = w.wss(code_list, "MA", str_para ) 

            ### 判断是否报错
            if obj1.ErrorCode == 0 :
                ### 合并数据
                if temp_i == 0 :
                    code_list_all = obj1.Codes
                    data_list = obj1.Data[0]
                else :
                    code_list_all = code_list_all + obj1.Codes
                    data_list = data_list + obj1.Data[0]
                ### 
                count_api = count_api + para_num_code
            else :
                # 220220:A股在3600后出现 “ invalid windcodes.”的报错；不知道是不是有股票退市？
                print("Error:obj1 \n ", obj1 )
                print("code_list"  )
            ### 
            time.sleep(0.3)

            df_temp = pd.DataFrame( data_list )  
            
        ###################################################
        ### 都下载后存入df 
        df_temp = pd.DataFrame( data_list ) 
        df_temp.index= code_list_all  
        ### index=codes, columns=0 
        df_data.loc[code_list_all, col_name ] = df_temp.loc[code_list_all,0 ]  

        ### save to excel
        df_data.to_excel("D:\\df_data.xlsx")
        obj_data["df_data"] = df_data


        #################################################### 
        ### 计算wind-api用量,并保存到excel
        self.api_count["wss"] =  1 * count_api
        result = self.save_api_count()        
        return obj_data

    def get_wss_close_pctchg_amt(self, obj_data ):
        ### 给定日期、周期参数和代码列表，获取股票和指数的收盘价、涨跌幅、成交额
        ### trade_date是给定的交易日期、para_ma是均线的参数、df_data是包含名为"code"代码列的df
        ### col_name是保存获取的指标值的列名称，如ma100，ma40-pre等。
        trade_date = obj_data["trade_date"]
        # para_ma 暂时用不上
        # para_ma = obj_data["para_ma"]
        df_data = obj_data["df_data"] 
        col_name = obj_data["col_name"] 
        ### 要把index都设置为code
        df_data.index = df_data["code"]

        ### indicator_name 是api对应的指标，col_name是df的列名，后者往往多了参数或周期有关的后缀。两者有可能一样，
        indicator_name = obj_data["indicator_name"]

        ###################################################
        ### 每次下载100个 | 测试的时候把单次数量改为2个
        para_num_code =  100
        num_100 = len( df_data.index )//para_num_code +1

        ### TODO 测试用
        # para_num_code = 3
        # num_100 = 2  

        from WindPy import w
        w.start() 
        count_api = 0 
        ###################################################
        for temp_i in range( num_100 ) :
            print("Indicator name=",indicator_name," Working on codes: ",temp_i*para_num_code ,(temp_i+1)*para_num_code  )
            ### 0,1,...,46
            sub_index = df_data.index[ temp_i*para_num_code :(temp_i+1)*para_num_code ]
            code_list = list( df_data.loc[ sub_index , "code"   ] )

            ### 一次 多个代码*1个指标 | notes:unit=1 对应的是市值等指标以1元为单位。
            # 前收盘价：w.wss("600036.SH,300750.SZ", "pre_close","tradeDate=20220831;priceAdj=F;cycle=M")
            # 市值：w.wss("600036.SH,300750.SZ", "mkt_cap,ev","unit=1;tradeDate=20220919")
            # notes:股票市值用ev或mkt_cap都行，指数市值必须用mkt_cap_ard ；ev1是企业价值不等于市值。
            str_para = "tradeDate="+ trade_date +";priceAdj=F;cycle=M;unit=1"  
            obj1 = w.wss(code_list, indicator_name , str_para ) 

            ### 判断是否报错
            if obj1.ErrorCode == 0 :
                ### 合并数据
                if temp_i == 0 :
                    code_list_all = obj1.Codes
                    data_list = obj1.Data[0]
                else :
                    code_list_all = code_list_all + obj1.Codes
                    data_list = data_list + obj1.Data[0]
                ### 
                count_api = count_api + para_num_code
            else :
                # 220220:A股在3600后出现 “ invalid windcodes.”的报错；不知道是不是有股票退市？
                print("Error:obj1 \n ", obj1 )
                print("code_list"  )
            ### 
            time.sleep(0.3)

            df_temp = pd.DataFrame( data_list )  
            
        ###################################################
        ### 都下载后存入df 
        df_temp = pd.DataFrame( data_list ) 
        df_temp.index= code_list_all  
        ### index=codes, columns=0 
        df_data.loc[code_list_all, col_name ] = df_temp.loc[code_list_all,0 ]  

        ### save to excel
        df_data.to_excel("D:\\df_data.xlsx")
        obj_data["df_data"] = df_data


        #################################################### 
        ### 计算wind-api用量,并保存到excel
        self.api_count["wss"] =  1 * count_api
        result = self.save_api_count()        
        return obj_data

    def get_wss_estimate(self, obj_data ):
        ### 预测指标：给定日期、和代码列表，获取股票的FY1,FY2的一致预测指标
        ### trade_date是给定的交易日期、para_ma是均线的参数、df_data是包含名为"code"代码列的df
        ### col_name是保存获取的指标值的列名称，如ma100，ma40-pre等。
        trade_date = obj_data["trade_date"] 
        df_data = obj_data["df_data"] 
        col_name = obj_data["col_name"] 
        ### 要把index都设置为code
        df_data.index = df_data["code"]

        ### indicator_name 是api对应的指标，col_name是df的列名，后者往往多了参数或周期有关的后缀。两者有可能一样，
        indicator_name = obj_data["indicator_name"]

        ###################################################
        ### 每次下载100个 | 测试的时候把单次数量改为2个
        para_num_code =  100
        num_100 = len( df_data.index )//para_num_code +1

        # ### TODO 测试用
        # para_num_code = 3
        # num_100 = 2  

        from WindPy import w
        w.start() 
        count_api = 0 
        ###################################################
        for temp_i in range( num_100 ) :
            print("Indicator name=",indicator_name," Working on codes: ",temp_i*para_num_code ,(temp_i+1)*para_num_code  )
            ### 0,1,...,46
            sub_index = df_data.index[ temp_i*para_num_code :(temp_i+1)*para_num_code ]
            code_list = list( df_data.loc[ sub_index , "code"   ] )

            ### 1，w.wss("000903.SH,300750.SZ", "estpe_FY1","tradeDate=20220919")
            # 2，一致预期类：w.wss("000903.SH,300750.SZ", "west_sales_FY1","unit=1;tradeDate=20220919")
            # 3，w.wss("000903.SH,300750.SZ", "west_netprofit_YOY","tradeDate=20220919")
            # 4，盈利预测变化率1周和4周； w.wss("000903.SH,300750.SZ", "west_nproc_1w","year=2022;tradeDate=20220919")
            # 5，w.wss("000903.SH,300750.SZ", "west_sales_YOY,west_sales_CAGR","tradeDate=20220831")
            str_para = "tradeDate="+ trade_date   
            if "west_nproc" in indicator_name : 
                ### 需要增加 year=2022
                str_para =str_para +";year=" + trade_date[:4] 
            
            obj1 = w.wss(code_list, indicator_name , str_para ) 

            ### 判断是否报错
            if obj1.ErrorCode == 0 :
                ### 合并数据
                if temp_i == 0 :
                    code_list_all = obj1.Codes
                    data_list = obj1.Data[0]
                else :
                    code_list_all = code_list_all + obj1.Codes
                    data_list = data_list + obj1.Data[0]
                ### 
                count_api = count_api + para_num_code
            else :
                # 220220:A股在3600后出现 “ invalid windcodes.”的报错；不知道是不是有股票退市？
                print("Error:obj1 \n ", obj1 )
                print("code_list"  )
            ### 
            time.sleep(0.3)

            df_temp = pd.DataFrame( data_list )  
            
        ###################################################
        ### 都下载后存入df 
        df_temp = pd.DataFrame( data_list ) 
        df_temp.index= code_list_all  
        ### index=codes, columns=0 
        df_data.loc[code_list_all, col_name ] = df_temp.loc[code_list_all,0 ]  

        ### save to excel
        df_data.to_excel("D:\\df_data.xlsx")
        obj_data["df_data"] = df_data


        #################################################### 
        ### 计算wind-api用量,并保存到excel
        self.api_count["wss"] =  1 * count_api
        result = self.save_api_count()        
        return obj_data 
    
    def get_wss_fund_nav_rank(self, obj_data ):
        ### 多个基金每次1个指标；给定日期、和代码列表，获取基金净值、区间收益率、区间排名
        # 单位净值,nav,复权单位净值,NAV_adj, 
        # 3种月收益率提取一样：近1月回报，return_1m；单月度回报，return_m,区间回报,return,2022-08-01；2022-08-31
        # 涉及"区间"的指标都要开始和结束日期：同类基金区间收益排名（百分比）peer_fund_return_rank_prop_per;近1月回报排名periodreturnranking_1m;今年以来回报排名periodreturnranking ytd
        # w.wss("519212.OF,166002.OF,004685.OF", "nav,NAV_adj,NAV_adj_return1,NAV_adj_chg,return_1m,return_m,return,
        # peer_fund_return_rank_prop_per,periodreturnranking_1m,periodreturnranking_ytd",
        # "tradeDate=20220831;startDate=20220801;endDate=20220831;annualized=0;fundType=2")
        # 如果不包含区间:w.wss("519212.OF,166002.OF,004685.OF", "return_m,periodreturnranking_1m,periodreturnranking_ytd","tradeDate=20220831;fundType=2")
        # fundType=2 指的是基金的二级分类，

        trade_date = obj_data["trade_date"] 
        if "date_start" in obj_data.keys(): 
            date_start= obj_data["date_start"]
            date_end  = obj_data["date_end"] 
        df_data = obj_data["df_data"] 
        col_name = obj_data["col_name"] 
        ### 要把index都设置为code
        df_data.index = df_data["code"]
        ### indicator_name 是api对应的指标，col_name是df的列名，后者往往多了参数或周期有关的后缀。两者有可能一样，
        indicator_name = obj_data["indicator_name"] 

        ###################################################
        ### 每次下载100个 | 测试的时候把单次数量改为2个 
        para_num_code =  100
        num_100 = len( df_data.index )//para_num_code +1

        # ### TODO 测试用
        # para_num_code = 3
        # num_100 = 2  

        from WindPy import w
        w.start() 
        count_api = 0 
        ###################################################
        for temp_i in range( num_100 ) :
            print("Indicator name=",indicator_name," Working on codes: ",temp_i*para_num_code ,(temp_i+1)*para_num_code  )
            ### 0,1,...,46
            sub_index = df_data.index[ temp_i*para_num_code :(temp_i+1)*para_num_code ]
            code_list = list( df_data.loc[ sub_index , "code"   ] ) 

            ### 不包含区间:w.wss("519212.OF,166002.OF,004685.OF", "return_m,periodreturnranking_1m,periodreturnranking_ytd","tradeDate=20220831;fundType=2")
            # w.wss("159967.OF,007475.OF,012238.OF,001171.OF", "fund_fundscale,netasset_total","unit=1;tradeDate=20220831")
            ### 包含区间: w.wss("519212.OF,166002.OF,004685.OF", "NAV_adj_return1,NAV_adj_chg,return,
                # peer_fund_return_rank_prop_per,periodreturnranking_1m,periodreturnranking_ytd",
                # ";startDate=20220801;endDate=20220831;annualized=0;fundType=2")
            
            ######################################################################################################
            ### 分类设置stra_para
            ###################################################
            ### Part1 不包含区间的指标:如净值：nav,NAV_adj,return_1m,return_m, 基金规模,fund_fundscale;基金规模(合计),netasset_total
            # para：tradeDate=20220831; return_1m指标需要annualized=0，表示不需要年化计算
            list_indi_1date = ["nav","NAV_adj","return_1m","return_m","fund_fundscale","netasset_total"]
            if indicator_name in list_indi_1date :
                str_para = "tradeDate="+ str(trade_date) +";"  
                ###################################################
                ### sub 细分要求
                if indicator_name in ["return_1m" ]:
                    str_para = str_para + "annualized=0;"
                if indicator_name in ["fund_fundscale","netasset_total"] :
                    str_para = str_para +";unit=1;"
                    
            ###################################################
            ### 包含区间的指标
            list_indi_period = ["NAV_adj_return1","NAV_adj_chg","return","peer_fund_return_rank_prop_per","periodreturnranking_1m","periodreturnranking_ytd"]
            if indicator_name in list_indi_period :
                str_para = ";startDate="+ str(date_start) + ";endDate="+ str(date_end) +";"  
                ###################################################
                ### sub 细分要求
                if "peer_" in indicator_name :
                    ### 同类排名的计算，设置;fundType=2 对应稍微细分一点的基金分类，但也不够用
                    str_para = str_para + "fundType=2;"

            ######################################################################################################
            ### RUN w.wss
            print("str_para:",str_para )
            obj1 = w.wss(code_list, indicator_name , str_para ) 

            ### 判断是否报错peer_fund_return_rank_prop_per
            if obj1.ErrorCode == 0 :
                ### 合并数据
                if temp_i == 0 :
                    code_list_all = obj1.Codes
                    data_list = obj1.Data[0]
                else :
                    code_list_all = code_list_all + obj1.Codes
                    data_list = data_list + obj1.Data[0]
                ### 
                count_api = count_api + para_num_code
            else :
                # 220220:A股在3600后出现 “ invalid windcodes.”的报错；不知道是不是有股票退市？
                print("Error:obj1 \n ", obj1 )
                print("code_list"  )
            ### 
            time.sleep(0.3)

            df_temp = pd.DataFrame( data_list )  
            
        ###################################################
        ### 都下载后存入df 
        df_temp = pd.DataFrame( data_list ) 
        df_temp.index= code_list_all  
        ### index=codes, columns=0 
        df_data.loc[code_list_all, col_name ] = df_temp.loc[code_list_all,0 ]  

        ### save to excel
        df_data.to_excel("D:\\df_data.xlsx")
        obj_data["df_data"] = df_data













        #################################################### 
        ### 计算wind-api用量,并保存到excel
        self.api_count["wss"] =  1 * count_api
        result = self.save_api_count()        
        return obj_data 
    


    ######################################################################################################
    ### WPF
    def get_wpf(self, dict_in ):
        ### 获取PMS组合持仓数据
        print( dict_in["pms_name"] )
        from WindPy import w
        w.start()
        '''期初持仓市值	BeginHoldingValue
        持仓市值	NetHoldingValue
        期初持仓成本	BeginTotalCost
        持仓成本	TotalCost
        期初持仓数量	BeginPosition
        持仓数量	Position
        浮动盈亏	EUnrealizedPL
        '''
        ####################################################################
        ### 
        pms_name = dict_in["pms_name"] # "FOF期权9901" 
        date_start = dict_in["date_start"].replace("-","")
        date_end = dict_in["date_end"].replace("-","")
        
        ### 统一格式 
        # "BeginHoldingValue,NetHoldingValue,BeginTotalCost,TotalCost,BeginPosition,Position,EUnrealizedPL",
        # "startDate=20211231;endDate=20220105;Currency=BSY;sectorcode=108;displaymode=1" 
        para_str_col = "BeginHoldingValue,NetHoldingValue,BeginTotalCost,TotalCost,BeginPosition,Position,EUnrealizedPL"
        ### para_str_date = "startDate="+"20211231"+";"+"endDate="+"20220105"
        para_str_date = "startDate="+ date_start +";"+"endDate="+ date_end +";"
        
        ### Currency=BSY，各类资产本币；Currency=CNY,人民币
        if dict_in["col_type"] in[1,"1","stock","股票"] :
            ### sectorcode=108,中信一级行业分类，
            type_num = "108" 
        if dict_in["col_type"] in[2,"2","fund","基金"] :
            ### 244;Wind基金二级分类
            type_num = "244"
        if dict_in["col_type"] in[3,"3","bond","债券"] :
            ### 218,债券久期;200，债券分类；
            type_num = "218" 
        else :
            type_num = "108" 
        # if dict_in["col_type"] in["4","future","期货","option","期权"] : 
            ### 期权暂时没找到匹配的，尝试期货的233和多资产的302，但是没有结果。
        ###
        para_str_end = "view=PMS;Currency=CNY;sectorcode="+ type_num + ";displaymode=1;"

        ####################################################################
        ### 提取数据 
        print("pms_name , para_str_col, para_str_end+para_str_date")
        print(pms_name , para_str_col, para_str_end+para_str_date )        
        obj1 = w.wpf( pms_name , para_str_col, para_str_end+para_str_date )

        ### 判断返回数据是否出错
        if obj1.Codes[0] == "ErrorReport" :
            print("ErrorReport obj1",obj1)
            print("check=",  para_str_col+ para_str_date + para_str_end  )
            df_data = dict_in
            num_index_count = 1 
        else :
            ### 赋值columns
            df1=pd.DataFrame(obj1.Data, index=obj1.Fields)
            # 转置
            df_data =df1.T

            ####################################################################
            ### 保存到目录
            if not os.path.exists( self.path_wpf ) :
                os.makedirs( self.path_wpf )

            ### 需要增加一列，组合名称
            df_data["port_name"] =  pms_name
            file_name = "wpf_" + pms_name + "_" + dict_in["date_end"] + ".xlsx"
            df_data.to_excel(self.path_wpf +file_name,index=False ) 

            ###不一定是最新日期阿
            # file_name = "wpf_" + pms_name +  ".xlsx"
            # df_data.to_excel(self.path_wpf +file_name,index=False ) 

            num_index_count = len( df_data["port_name"] )

        ####################################################################
        ### 暂时无用的：
        # len_1 = len( wind_data.Fields)
        # import pandas as pd 
        # # notes input items might be the same as wind_data.Fields
        # # wind_df = pd.dataframe(columns= wind_data.Fields)
        # wind_df = pd.DataFrame(columns= wind_head['items']  )
        # for i in range( len_1 ):
        #     wind_df[wind_head['items'][i]] = wind_data.Data[i]

        # # assign dates to wind_df 
        # wind_df['date'] = wind_data.Times
        # # print( wind_df )
        # self.wind_df = wind_df

        #################################################### 
        ### 计算wind-api用量,并保存到excel
        self.api_count["wpf"] =  7 * num_index_count
        result = self.save_api_count()

        
        return df_data

    ######################################################################################################
    ### WPS
    def get_wps(self, df_perf) :
        ### 获取PMS组合区间df_perf涨跌幅、回撤、Alpha、Sharpe等绩效指标。
        ### notes:input 有可能是多个组合也有可能是单个组合
        ### notes: wps只能1个1个组合地获取数据；"startDate"指标决定了部分如"历史最大回撤"、"历史月胜率"的开始区间，尽量选则2年以前
        #################################################### 
        ### 最新日期 self.time_now_str ；近1个月日期 self.time_str_pre_1m 
        # 近1年、3年日期： self.time_str_pre_1y   self.time_str_pre_3y
        #################################################### 
        ### w.wps("行业成长价值精选", "Return_w,Return_m,Return_y,Return_1m,Return_3m,Return_6m,Alpha_std,Sharpe_std,MaxDrawdown_std,MaxDrawdown_6m,MaxDrawdown_y,Winning_Rate_m","view=PMS;startDate=20210101;endDate=20220211;Currency=BSY;returntype=1;fee=1")
        ### WindCodes	Return_w	Return_m	Return_y	Return_1m	Return_3m	Return_6m	Alpha_std	Sharpe_std	MaxDrawdown_std	MaxDrawdown_6m	MaxDrawdown_y	Winning_Rate_m
        # 	本周收益率	本月收益率	本年收益率	近1个月收益率	近3个月收益率	近6个月收益率	Alpha成立至今	Sharpe成立至今	最大回撤：成立至今	最大回撤：近6个月	最大回撤：本年	月胜率
        # 行业成长价值精选	0.003032		-0.001744	-0.001744	-0.001744	-0.001744	0.047268	-1.769173	-0.007046	-0.007046	-0.007046	0
        #################################################### 
        #  obj_w =  .ErrorCode=0
        # .Codes=[行业成长价值精选]
        # .Fields=[Return_w,Return_m,Return_y,Return_1m,Return_3m,Return_6m,Alpha_std,Sharpe_std,MaxDrawdown_std,MaxDrawdown_6m,...]
        # .Times=[20220212 23:56:03]
        # .Data=[[0.0030316393209304238],[0.0031390117526644],[-0.0017444554661211198],[-0.0017444554661211198],[-0.0017444554661211198],[-0.0017444554661211198],[0.04726793110654877],[-1.769173469583503],[-0.007046337585139284],[-0.007046337585139284],...]
        #################################################### 
        ### df_perf["port_name"] 是组合中文名称
        #################################################### 
        col_list_str = "Return_w,Return_m,Return_y,Return_1m,Return_3m,Return_6m"
        col_list_str = col_list_str + "," + "Alpha_std,Sharpe_std,MaxDrawdown_std,MaxDrawdown_6m,MaxDrawdown_y,Winning_Rate_m"
        
        count_col = 6 + 6
        # "view=PMS;startDate=20210101;endDate=20220211;Currency=BSY;returntype=1;fee=1"
        time_str_pre_3y = self.obj_date["date_pre_3y_str"]
        para_end_str = "view=PMS;startDate=" + time_str_pre_3y + ";endDate=" + self.time_now_str + ";Currency=BSY;returntype=1;fee=1"
        from WindPy import w
        w.start()
        for temp_i in df_perf.index :
            port_name = df_perf.loc[temp_i,"port_name"]

            obj_w = w.wps(port_name, col_list_str, para_end_str)
            print("Debuig=",port_name, col_list_str, para_end_str)
            print( obj_w )
            if obj_w.ErrorCode == 0 :
                ### df1 index是各个指标，columns=0 
                df1=pd.DataFrame(obj_w.Data, index=obj_w.Fields)
                for temp_indi in df1.index :
                    df_perf.loc[temp_i,temp_indi] = df1.loc[temp_indi, 0 ]
                    col_list_indi = obj_w.Fields
        #################################################### 
        ### exhi，将百分比数值控制百分比,2位小数点
        for temp_col in col_list_indi: 
            ### 前缀 "exhi_"表示
            df_perf[ "exhi_"+ temp_col ] = df_perf[ temp_col ]*100
            df_perf[ "exhi_"+ temp_col ] = df_perf[ "exhi_"+ temp_col ].round(decimals=2)

        #################################################### 
        ### 计算wind-api用量,并保存到excel
        count_port = 1
        self.api_count["wps"] =  count_col * count_port
        result = self.save_api_count() 

        ### debug
        # df_perf.to_excel("D:\\df_perf.xlsx")

        return df_perf
    ######################################################################################################
    ### WPD
    def get_wpd(self,obj_p ) :
        ### 获取PMS组合日期序列的总资产和盈亏等;基准的区间收益率、净值
        port_name = obj_p["port_name"]  
        # benchmark_code = obj_p["benchmark_code"]      
        ### 净值类型：默认 week
        unit_type = obj_p["unit_type"] 
        ### 获取区间开始和结束日期   
        #  输入日期是int格式,会报错
        date_begin = str( obj_p["date_begin"] )
        date_end = str( obj_p["date_end"] )
        
        ####################################################
        ### 净值类型：默认 week
        if unit_type == "day" :
            para_end = "view=PMS;Currency=BSY;period=D;Fill=Blank"
        else :
            ### week
            para_end = "view=PMS;Currency=BSY;period=W;Fill=Blank"
        ###
        col_list_str =  "NetAsset,TotalPL"
        count_col =2 
        ###
        print("Debug20220225:\n",port_name ,col_list_str, date_begin,date_end,para_end  )

        ####################################################
        ### 计算交易日数量
        ### 输入日期是int格式
        import datetime as dt  
        date_begin_adj =  dt.datetime.strptime( str(date_begin) , "%Y%m%d")
        date_end_adj =  dt.datetime.strptime( str(date_end) , "%Y%m%d")
        obj_date= w.tdayscount( date_begin_adj ,date_end_adj, "")
        count_date = obj_date.Data[0][0] 

        ####################################################
        ###
        from WindPy import w
        w.start()
        
        # obj_date .ErrorCode=0 .Codes=[] .Fields=[] .Times=[20220225] .Data=[[145]]
        
        ### w.wpd("行业成长价值精选", "NetAsset,TotalPL", "20220113", "20220213","view=PMS;Currency=BSY;period=D;Fill=Blank")
        ####################################################
        ### notes:如果是周week，只会返回结束日期；如果是交易日day，会正常返回序列
        ### 返回NetAsset，TotalPL 两列，T日盈亏除以T-1日净资产，可以得到收益率百分比！！！
        # Error：ctypes.ArgumentError: argument 3: <class 'TypeError'>: wrong type
        obj_w = w.wpd( port_name ,col_list_str, date_begin,date_end,para_end )
        print( obj_w )
        if obj_w.ErrorCode == 0 :
            ### df1 index是各个指标，columns=0 
            df1= pd.DataFrame(obj_w.Data, index=obj_w.Fields,columns=obj_w.Times  )
            df_port = df1.T

            ### .Fields  = [ NetAsset,TotalPL ] 
            ####################################################
            ### save to output
            obj_p["df_port"] = df_port
            obj_p["obj_w"] = obj_w
            obj_p["col_list_indi"] =  obj_w.Fields
        else : 
            print("Error ", obj_w)

        #################################################### 
        ### 计算wind-api用量,并保存到 excel 
        self.api_count["wpd"] = count_col * count_date 
        result = self.save_api_count()  

        #################################################### 
        ###
        print("Debug== df-unit \n", df_port )

        return obj_p


    ######################################################################################################
    ### WSS 
    def get_wss_ma_amt_mv(self, input_date ):
        ### 给定含代码的df，获取特定价量数据
        ### notes:2022-11区间日均成交额改名了：m_ave_amt -->avg_amt_per,avg_MV_per从9月份开始就无法提取数据了
        #################################################### 
        path_wpf = self.path_pms+ "wpf\\" 
        path_data = self.path_pms+ "wind_terminal\\" 
        path_adj = self.path_pms+  "data_adj\\"
        #####################################################
        from WindPy import w
        w.start() 
        import time  
        
        '''提取的数据格式：
        obj1 = w.wss(code_list, "MA","tradeDate=20220107;MA_N=16;priceAdj=F;cycle=D")
        >>> obj1
        .ErrorCode=0
        .Codes=[600519.SH,601398.SH,300750.SZ,600036.SH,601288.SH]
        .Fields=[MA]
        .Times=[20220108 18:45:27]
        .Data=[[2066.524375,4.657500000000001,590.2837499999999,49.485625,2.9462499999999996]]

        '''

        if len(input_date) == 6 :
            # 220107 to 20220107
            date_latest = "20" +input_date
        elif len(input_date) == 8 and input_date[:2] =="20" :
            date_latest =  input_date
        else :
            print("Error input date...")
            date_latest = input("Input date such as 20220220:")
        
        dict_ah = {} 
        dict_ah["a"] = {}
        dict_ah["a"]["name"] = "全部A股-"
        dict_ah["a"]["num"] = 3500
        dict_ah["h"] = {}
        dict_ah["h"]["name"] = "全部港股-"
        dict_ah["h"]["num"] = 800
        ### 全部港股-220107.xlsx；"全部A股-"；
        ####################################################################################
        ### 分别下载 a 和 h 数据
        for temp_key in dict_ah.keys() : 
            
            file_name = dict_ah[temp_key]["name"] + input_date +".xlsx"

            ### 只要是数字的columns，type都是float
            df_shares = pd.read_excel(path_data + file_name  )
            ### 取流通市值前4000的公司
            df_shares = df_shares.sort_values(by="流通市值",ascending=False )
            len_shares = len( df_shares.index )
            
            ### A股 ;港股  
            df_shares = df_shares.iloc[: dict_ah[temp_key]["num"], : ]
            
            df_shares.index = df_shares[ "代码"]

            ################################################
            ### 从Wind提取指标；均价数据
            
            ################################################
            ### 获取最近2个交易日
            # time_now_str="20220109" , time_pre10_str = "20211230"
            print("Debug=input_date",input_date )
            print("Debug=date_latest",date_latest )
            obj_d = w.tdaysoffset(-1,date_latest, "")
            date_pre_1d = obj_d.Times[0]
            ### datetime.date") to str
            import datetime as dt  
            date_pre_1d = dt.datetime.strftime(date_pre_1d   , "%Y%m%d")
            print("Debug=date_pre_1d",date_pre_1d )
            ### 获取前1个月日期 | pre_version
            # obj_d = w.tdaysoffset(-22,date_latest, "")
            # date_pre_1m = obj_d.Times[0]
            # date_pre_1m = dt.datetime.strftime(date_pre_1m   , "%Y%m%d")
            ### version 2 
            import dateutil.relativedelta as rd
            date_pre_1m = dt.datetime.strftime( obj_d.Times[0] - rd.relativedelta(months=1) , "%Y%m%d")
            '''obj_d = w.tdaysoffset(-1, "2022-01-07", "") 或者
            obj_d = w.tdaysoffset(-1, "20220107", "")
            >>> obj_d
            .ErrorCode=0
            .Codes=[]
            .Fields=[]
            .Times=[20220106]
            .Data=[[2022-01-06 00:00:00]]
            '''
            ################################################
            ### Wind单指标，一次最多100个股票。num_100=46,对应4692个股票
            ### 共需要6个指标：
            indi_list = [  ]
            df_shares["ma_short"] = 0.0 
            df_shares["ma_short_pre"] = 0.0 
            df_shares["pre_close"] = 0.0 
            df_shares["ma_mid"] = 0.0 
            ### 月均成交额和市值 | 2022-11区间日均成交额改名了：m_ave_amt -->avg_amt_per,avg_MV_per从9月份开始就无法提取数据了
            # df_shares["avg_amt_per"] = 0.0 
            # df_shares["avg_MV_per"] = 0.0 

            ### 每次下载100个 | 测试的时候把单次数量改为2个
            para_num_code =  100
            num_100 = len( df_shares.index )//para_num_code +1

            ################################################
            ## 需要的指标1：MA16_前复权，MA40_前复权	
            for para_ma in [ "16","40"  ] :
                for temp_i in range( num_100 ) :
                    print("Working on codes: ",temp_i*para_num_code ,(temp_i+1)*para_num_code  )
                    ### 0,1,...,46
                    sub_index = df_shares.index[ temp_i*para_num_code :(temp_i+1)*para_num_code ]
                    code_list = list( df_shares.loc[ sub_index , "代码"   ] )
                    
                    ### 获取均线数据,短期
                    # str_para = "tradeDate=20220107;MA_N=16;priceAdj=F;cycle=D"
                    str_para = "tradeDate="+ date_latest +";MA_N="+ para_ma +";priceAdj=F;cycle=D"
                    obj1 = w.wss(code_list, "MA",str_para )
                    indi_list = indi_list + [ "MA" + "_" + para_ma ]
                    
                    ### 判断是否报错
                    if obj1.ErrorCode == 0 :
                        ### 合并数据
                        if temp_i == 0 :
                            code_list_all = obj1.Codes
                            data_list = obj1.Data[0]
                        else :
                            code_list_all = code_list_all + obj1.Codes
                            data_list = data_list + obj1.Data[0] 
                    else :
                        # 220220:A股在3600后出现 “ invalid windcodes.”的报错；不知道是不是有股票退市？
                        print("Error:obj1 \n ", obj1 )
                        print("code_list"  )
                    ### 
                    time.sleep(0.3)

                # print( len(data_list),len(code_list_all)   )
                ### 都下载后存入df 
                df_temp = pd.DataFrame( data_list ) 
                df_temp.index=code_list_all 
                ### index=codes, columns=0 
                if para_ma == "16" :
                    df_shares.loc[code_list_all,"ma_short"] = df_temp.loc[code_list_all,0 ]  
                if para_ma == "40" :
                    df_shares.loc[code_list_all,"ma_mid"] = df_temp.loc[code_list_all,0 ]  

                ### save to excel 
                file_name2 = temp_key+"_shares_"+ date_latest +".xlsx"
                df_shares.to_excel(path_data + file_name2  ,index=False )

            
            ################################################
            ### 需要的指标2：MA16pre_前复权	
            for temp_i in range( num_100 ) :
                ### 0,1,...,46
                print("Working on codes: ",temp_i*para_num_code ,(temp_i+1)*para_num_code  )
                sub_index = df_shares.index[ temp_i*para_num_code :(temp_i+1)*para_num_code ]
                code_list = list( df_shares.loc[ sub_index , "代码"   ] )
                
                ### 获取均线数据,短期
                # str_para = "tradeDate=20220107;MA_N=16;priceAdj=F;cycle=D"
                str_para = "tradeDate="+ date_pre_1d +";MA_N="+ para_ma +";priceAdj=F;cycle=D"
                obj1 = w.wss(code_list, "MA",str_para )
                indi_list = indi_list + [ "MA" + "_" + para_ma + "_pre" ]
                
                ### 判断是否报错
                if obj1.ErrorCode == 0 :
                    ### 合并数据
                    if temp_i == 0 :
                        code_list_all = obj1.Codes
                        data_list = obj1.Data[0]
                    else :
                        code_list_all = code_list_all + obj1.Codes
                        data_list = data_list + obj1.Data[0]
                else :
                    # 220220:A股在3600后出现 “ invalid windcodes.”的报错；不知道是不是有股票退市？
                    print("Error:obj1 \n ", obj1 )
                    print("code_list"  )
                ### 
                time.sleep(0.3)

            ### 都下载后存入df 
            df_temp = pd.DataFrame( data_list ) 
            df_temp.index=code_list_all  
            ### index=codes, columns=0 

            df_shares.loc[code_list_all,"ma_short_pre"] = df_temp.loc[code_list_all,0 ]   
            
            ### save to excel
            file_name2 = temp_key+"_shares_"+ date_latest +".xlsx"
            df_shares.to_excel(path_data + file_name2  ,index=False )

            ################################################
            ### 需要的指标3：前收盘价	
            # w.wss("600036.SH", "pre_close","tradeDate=20220108;priceAdj=U;cycle=D")
            for temp_i in range( num_100 ) :
                ### 0,1,...,46
                print("Working on codes: ",temp_i*para_num_code ,(temp_i+1)*para_num_code  )
                sub_index = df_shares.index[ temp_i*para_num_code :(temp_i+1)*para_num_code ]
                code_list = list( df_shares.loc[ sub_index , "代码"   ] )
                
                ### 获取均线数据,短期
                # str_para = "tradeDate=20220108;priceAdj=U;cycle=D"
                str_para = "tradeDate="+ date_pre_1d  +";priceAdj=F;cycle=D"
                obj1 = w.wss(code_list, "pre_close",str_para )
                indi_list = indi_list + [ "pre_close" + "_" + para_ma   ]
                
                ### 判断是否报错
                if obj1.ErrorCode == 0 :
                    ### 合并数据
                    if temp_i == 0 :
                        code_list_all = obj1.Codes
                        data_list = obj1.Data[0]
                    else :
                        code_list_all = code_list_all + obj1.Codes
                        data_list = data_list + obj1.Data[0]
                else :
                    # 220220:A股在3600后出现 “ invalid windcodes.”的报错；不知道是不是有股票退市？
                    print("Error:obj1 \n ", obj1 )
                    print("code_list"  )
                ### 
                time.sleep(0.3)

            ### 都下载后存入df 
            df_temp = pd.DataFrame( data_list ) 
            df_temp.index=code_list_all  
            ### index=codes, columns=0 

            df_shares.loc[code_list_all,"pre_close"] = df_temp.loc[code_list_all,0 ]   

            ### save to excel
            file_name2 = temp_key+"_shares_"+ date_latest +".xlsx"
            df_shares.to_excel(path_data + file_name2  ,index=False )

            ################################################################################################
            ### 区间成交额=amt_per,输出指标=amt_per_1m,m_ave_amt | since 20230607
            # before： 区间成交额（含大宗交易）；区间日均总市值
            # w.wss("600036.SH,300760.SZ", "pq_blocktradeamounts,avg_MV_per","unit=1;startDate=20211209;endDate=20220109;currencyType=")
            
            for temp_i in range( num_100 ) :
                ### 0,1,...,46
                print("Working on codes: ",temp_i*para_num_code ,(temp_i+1)*para_num_code  )
                sub_index = df_shares.index[ temp_i*para_num_code :(temp_i+1)*para_num_code ]
                code_list = list( df_shares.loc[ sub_index , "代码"   ] )
                
                ### 
                str_para = "startDate="+ date_pre_1m + ";endDate="+ date_latest +";unit=1;"
                ### 用"amt_per"； 港股也可以获得数据
                obj1 = w.wss(code_list,"amt_per" ,str_para )
                indi_list = indi_list + [ "amt_per" + "_" + para_ma   ]
                
                ### 判断是否报错
                if obj1.ErrorCode == 0 :
                    ### 合并数据
                    if temp_i == 0 :
                        code_list_all = obj1.Codes
                        data_list = obj1.Data[0]
                    else :
                        code_list_all = code_list_all + obj1.Codes
                        data_list = data_list + obj1.Data[0]
                else :
                    # 220220:A股在3600后出现 “ invalid windcodes.”的报错；不知道是不是有股票退市？
                    print("Error:obj1 \n ", obj1 )
                    print("code_list"  )
                ### 
                time.sleep(0.3)

            ### 都下载后存入df 
            df_temp = pd.DataFrame( data_list ) 
            df_temp.index=code_list_all  
            ### index=codes, columns=0 
            ### m_ave_amt=日均成交额，amt_per_1m=近1个月成交额
            df_shares.loc[code_list_all,"amt_per_1m"] = df_temp.loc[code_list_all,0 ]   
            df_shares.loc[code_list_all,"m_ave_amt"] =  df_shares.loc[code_list_all,"amt_per_1m"] /22
                        
            ################################################################################################
            ### 区间日均市值 avg_MV_per,输出指标=m_ave_mv | since 20230607
            ### 区间日均成交额，区间日均市值，区间日均流通市值="avg_amt_per,avg_MV_per,pq_avgmv_nonrestricted"
            # w.wss("0939.HK,301030.SZ", "avg_amt_per,avg_MV_per,pq_avgmv_nonrestricted","unit=1;startDate=20230507;endDate=20230607;currencyType=")
            ## 非必须，暂时不提取了| avg_MV_per从2022.9月份开始无法提取数据
            for temp_i in range( num_100 ) :
                ### 0,1,...,46
                print("Working on codes: ",temp_i*para_num_code ,(temp_i+1)*para_num_code  )
                sub_index = df_shares.index[ temp_i*para_num_code :(temp_i+1)*para_num_code ]
                code_list = list( df_shares.loc[ sub_index , "代码"   ] )
                
                # str_para = "tradeDate=20220108;priceAdj=U;cycle=D" ;
                str_para = "startDate="+ date_pre_1m + ";endDate="+ date_latest +";unit=1;;currencyType="
                obj1 = w.wss(code_list, "avg_MV_per",str_para )
                indi_list = indi_list + [ "avg_MV_per" + "_" + para_ma   ]
                
                ### 判断是否报错
                if obj1.ErrorCode == 0 :
                    ### 合并数据
                    if temp_i == 0 :
                        code_list_all = obj1.Codes
                        data_list = obj1.Data[0]
                    else :
                        code_list_all = code_list_all + obj1.Codes
                        data_list = data_list + obj1.Data[0]
                else :
                    # 220220:A股在3600后出现 “ invalid windcodes.”的报错；不知道是不是有股票退市？
                    print("Error:obj1 \n ", obj1 )
                    print("code_list"  )
                ### 
                time.sleep(0.3)
            
            ### 都下载后存入df 
            df_temp = pd.DataFrame( data_list ) 
            df_temp.index=code_list_all  
            ### index=codes, columns=0 
            df_shares.loc[code_list_all,"m_ave_mv"] = df_temp.loc[code_list_all,0 ]   

            file_name2 = temp_key+"_shares_"+ date_latest +".xlsx"
            df_shares.to_excel(path_adj + file_name2  ,index=False )
            
            #####################################################
            ### 把成交额和市值单位从元改为亿元
            # df_shares["avg_amt_per"] =df_shares["avg_amt_per"]/100000000

            # df_shares["m_ave_mv"] =df_shares["m_ave_mv"]/100000000

            #####################################################
            ### save to excel
            file_name2 = temp_key+"_shares_"+ date_latest +".xlsx"
            df_shares.to_excel(path_adj + file_name2  ,index=False )
            file_name2 = temp_key+"_shares" +".xlsx"
            df_shares.to_excel(path_adj + file_name2  ,index=False )
        
            #################################################### 
            ### 计算wind-api用量,并保存到excel
            count_col = len( indi_list  )
            count_code = len( df_shares.index ) 
            self.api_count["wss"] =  count_col * count_code 
            result = self.save_api_count() 

        return df_shares

    def get_wss_pct_chg_period(self, obj_m) :
        ### 给定含代码的df，获取多个区间涨跌幅 | notes:单次不超过100个代码
        ### 假设 df_chg 至少包括 columns:"code",
        df_chg = obj_m["df_chg"] 
        date_begin = obj_m["date_begin"] 
        date_end = obj_m["date_end"] 

        ### 每次下载100个 | 测试的时候把单次数量改为2个
        para_num_code =  100
        num_100 = len( df_chg.index )//para_num_code +1

        ####################################################################################
        ### 
        import datetime as dt  
        time_now = dt.datetime.now()
        ### 获取近1w、1m、3m、6m、1year日期
        time_now_str = dt.datetime.strftime(time_now   , "%Y%m%d")
        ### dt.timedelta() 只能获取周、日、小时的差异 
        dict_period = {} 
        dict_period[ "col_list"] = ["1w","1m","3m","6m","1y" ]
        dict_period[ "1w" ] = {}
        dict_period[ "1w" ]["name_exhi"] = "近1周涨跌幅"
        dict_period[ "1w" ]["date_begin"] = dt.datetime.strftime(time_now - dt.timedelta(weeks =1) , "%Y%m%d")
        dict_period[ "1w" ]["date_end"] = time_now_str
        ### 月、年差异用 dateutil.relativedelta 
        import dateutil.relativedelta as rd 
        dict_period[ "1m" ] = {}
        dict_period[ "1m" ]["name_exhi"] = "近1月涨跌幅"
        dict_period[ "1m" ]["date_begin"] = dt.datetime.strftime(time_now - rd.relativedelta(months=1) , "%Y%m%d")
        dict_period[ "1m" ]["date_end"] = time_now_str
        dict_period[ "3m" ] = {}
        dict_period[ "3m" ]["name_exhi"] = "近3月涨跌幅"
        dict_period[ "3m" ]["date_begin"] = dt.datetime.strftime(time_now - rd.relativedelta(months=3) , "%Y%m%d")
        dict_period[ "3m" ]["date_end"] = time_now_str
        dict_period[ "6m" ] = {}
        dict_period[ "6m" ]["name_exhi"] = "近6月涨跌幅"
        dict_period[ "6m" ]["date_begin"] = dt.datetime.strftime(time_now - rd.relativedelta(months=6) , "%Y%m%d")
        dict_period[ "6m" ]["date_end"] = time_now_str
        dict_period[ "1y" ] = {}
        dict_period[ "1y" ]["name_exhi"] = "近1年涨跌幅"
        dict_period[ "1y" ]["date_begin"] = dt.datetime.strftime(time_now - rd.relativedelta(years=1) , "%Y%m%d")
        dict_period[ "1y" ]["date_end"] = time_now_str

        ####################################################################################
        ### 
        from WindPy import w
        w.start()
        ### input：obj_w =w.wss(["000300.SH","HSI.HI"], "pct_chg_per","startDate=20201231;endDate=20211231")
        ### obj_w= .ErrorCode=0 ; .Codes=[000300.SH,HSI.HI] ; .Fields=[PCT_CHG_PER]
        ### .Times=[20220211 17:00:27] ; .Data=[[-3.389656101963534,-13.811562262060317]]

        ####################################################################################
        ### 获取区间的涨跌幅  :近一周	近一个月	近6个月	近一年	年初至今
        for temp_period in dict_period[ "col_list"] :
        # for temp_period in ["1w","1m"] :
            ##########################################
            ###
            for temp_i in range( num_100 ) : 
                print("Working on codes: ",temp_i*para_num_code ,(temp_i+1)*para_num_code  )
                sub_index = df_chg.index[ temp_i*para_num_code :(temp_i+1)*para_num_code ]
                code_list = list( df_chg.loc[ sub_index , "code"] )
                
                ### "startDate=20201231;endDate=20211231"
                date_begin= dict_period[ temp_period ]["date_begin"] 
                date_end  = dict_period[ temp_period ]["date_end"] 
                str_date = "startDate=" + str( date_begin ) +";endDate=" + str( date_end )
                obj_w = w.wss( code_list , "pct_chg_per",str_date )

                print("Debug=", code_list  )
                print( obj_w.Data[0] )
                ### 判断是否报错
                if obj_w.ErrorCode == 0 :
                    ### 合并数据
                    if temp_i == 0 :
                        code_list_all = obj_w.Codes
                        data_list = obj_w.Data[0]
                    else :
                        code_list_all = code_list_all + obj_w.Codes
                        data_list = data_list + obj_w.Data[0]
                ### 
                time.sleep(0.3)
            
            ### 都下载后存入df 
            df_temp = pd.DataFrame( data_list ) 
            df_temp.index=code_list_all  
            ### index=codes, columns=0 
            ### notes:df_chg.index 是数字0，1，2； 代码在"code"
            df_chg["temp"] = df_chg.index
            df_chg.index = df_chg["code"]
            ### name_exhi 用于展示的名字
            # name_exhi = dict_period[ temp_period ]["name_exhi"]
            df_chg.loc[ : , temp_period ] = df_temp.loc[code_list_all,0 ]   
            ### 恢复原始的index
            df_chg.index = df_chg["temp"] 
            df_chg = df_chg.drop(["temp"] ,axis=1 )
            print("df_chg \n",  df_chg )
            ##########################################
            ### exhi，将百分比数值控制百分比,2位小数点
            ### 前缀 "exhi_"表示
            df_chg[ temp_period] = df_chg[ temp_period].round(decimals=2)

        ####################################################################################
        ### 获取给定日期区间的涨跌幅  
        if "date_begin" in obj_m.keys() :
            ### 假设code_list 一次不超过100个
            code_list = list( df_chg["code"] )
            
            ##########################################
            ###
            for temp_i in range( num_100 ) : 
                print("Working on codes: ",temp_i*para_num_code ,(temp_i+1)*para_num_code  )
                sub_index = df_chg.index[ temp_i*para_num_code :(temp_i+1)*para_num_code ]
                code_list = list( df_chg.loc[ sub_index , "code"] )
                
                ### "startDate=20201231;endDate=20211231"
                str_date = "startDate=" + str( date_begin ) +";endDate=" + str( date_end )
                obj_w = w.wss( code_list , "pct_chg_per",str_date )

                print("Debug=", code_list  )
                print( obj_w.Data[0] )
                ### 判断是否报错
                if obj_w.ErrorCode == 0 :
                    ### 合并数据
                    if temp_i == 0 :
                        code_list_all = obj_w.Codes
                        data_list = obj_w.Data[0]
                    else :
                        code_list_all = code_list_all + obj_w.Codes
                        data_list = data_list + obj_w.Data[0]
                ### 
                time.sleep(0.3)
            
            ### 都下载后存入df 
            df_temp = pd.DataFrame( data_list ) 
            df_temp.index=code_list_all  
            ### index=codes, columns=0 
            ### notes:df_chg.index 是数字0，1，2； 代码在"code"
            df_chg["temp"] = df_chg.index
            df_chg.index = df_chg["code"]
            df_chg.loc[ : ,"period"] = df_temp.loc[code_list_all,0 ]   
            ### 恢复原始的index
            df_chg.index = df_chg["temp"] 
            df_chg = df_chg.drop(["temp"] ,axis=1 )
            print("df_chg \n",  df_chg )
            ##########################################
            ### exhi，将百分比数值控制百分比,2位小数点
            ### 前缀 "exhi_"表示 
            df_chg["exhi_period"] = df_chg["period"].round(decimals=2)


        #################################################### 
        ### 计算wind-api用量,并保存到excel
        count_col = len( dict_period[ "col_list"] )
        count_code =   len( df_chg.index )  
        self.api_count["wss"] = count_col * count_code 
        result = self.save_api_count()

        ####################################################################################
        ### output 
        obj_m["dict_period"] = dict_period
        obj_m["df_chg"] = df_chg

        return obj_m

    def get_wss_fund_1date(self, obj_f) :
        ### 给定1个基金代码及日期，获取多个不同基金指标

        ##########################################
        ### 
        fund_code = obj_f["fund_code"]
        date= obj_f["date"]
        col_list = obj_f["col_list"] 
        str_col = ""
        for temp_col in col_list :
            str_col = str_col + temp_col +","
        str_col = str_col[:-1]
        ##########################################
        ###         
        from WindPy import w
        w.start()
        obj_w = w.wss( fund_code , str_col , "tradeDate="+date )

        ### obj_w .ErrorCode=0  .Codes=[110011.OF]
        # .Fields=[FUND_FUNDMANAGEROFTRADEDATE]   .Times=[20220902 17:31:20]   .Data=[[张坤]]
        print( "obj_w.Data \n", obj_w.Data )
        ### 判断是否报错
        if obj_w.ErrorCode == 0 :
            ### 合并数据
            obj_f["list_data"] = obj_w.Data[0]
        else :
            print( "Error  \n", obj_w )

        return obj_f


    ######################################################################################################
    ### WSD
    def get_wsd_period(self, obj_m) :
        ###########################################################################
        ### 给代码和收盘价等指标，获取区间内每个交易日的指标数据,并合并保存到xlsx文件
        ### 假设 df_chg 至少包括 columns:"code",
        temp_code = obj_m["code"]  
        date_begin = obj_m["date_begin"] 
        date_end = obj_m["date_end"]  

        ##########################################
        ### 指标从list转str || 先不用
        # indicator_list = obj_m["indicator_list"] 
        
        ####################################################################################
        ### 
        from WindPy import w
        w.start()

        ### 计算交易日数量
        ### 输入日期是int格式
        date_begin_adj =  dt.datetime.strptime( str(date_begin) , "%Y%m%d")
        date_end_adj =  dt.datetime.strptime( str(date_end) , "%Y%m%d")
        obj_date= w.tdayscount( date_begin_adj ,date_end_adj, "")
        count_date = obj_date.Data[0][0]          
        
        ####################################################################################
        ### 可能地代码类型：885001.WI、 000300.SH、159915.SZ、000333.SZ、519979.OF
        # 收盘价：w.wsd("885001.WI", "close", "2022-01-01", "2022-02-13", "Period=W;PriceAdj=F")
        # 涨跌幅：w.wsd("885001.WI", "pct_chg", "2022-01-01", "2022-02-13", "Period=W;PriceAdj=F")
        ### notes:涨跌幅pct_chg、close和定点复权"close2",适用于股票和指数"000300.SH,000333.SZ,159915.SZ； 不适用于 基金 519979.OF
        # w.wsd("000300.SH,000333.SZ,159915.SZ", "close2", "2021-12-31", "2022-02-13", "adjDate=20220214;Period=W;PriceAdj=F")
        ###########################################
        ### 1，基金：
        if temp_code[-3:] == ".OF" :
            temp_col = "NAV_adj"
            obj_w = w.wsd( temp_code , temp_col, date_begin, date_end, "PriceAdj=F" )
            if obj_w.ErrorCode == 0 :
                ### notes: 这里index不能用 obj_w.Fields
                df2 = pd.DataFrame(obj_w.Data, index= ["NAV_adj"] ,columns=obj_w.Times )
                df2 = df2.T 
                df2["date"] = df2.index 
            else :
                print("Debug:temp_code ,",  obj_w )
        else :
            ### 涨跌幅pct_chg、close和定点复权"close2" 都可以;但是 885001.WI 不能用 "close2"
            # temp_col = "close"
            # obj_w = w.wsd( temp_code , temp_col, date_begin, date_end, "PriceAdj=F" )
            # if obj_w.ErrorCode == 0 : 
            #     ### notes: 这里index不能用 obj_w.Fields
            #     df2 = pd.DataFrame(obj_w.Data, index= [temp_col] ,columns=obj_w.Times )
            #     df2 = df2.T
                
            # else :
            #     print("Debug:temp_code ,",  obj_w )

            print("temp_code ,date_begin, date_end \n ", temp_code , date_begin, date_end )
            ### w.wsd( "000300.SH" ,"pct_chg","20220525", "20220615", "PriceAdj=F" )
            temp_col = "pct_chg"
            obj_w = w.wsd( temp_code ,temp_col, str(date_begin), str(date_end), "PriceAdj=F" )

            if obj_w.ErrorCode == 0 : 
                ### notes: 这里index不能用 obj_w.Fields
                df2 = pd.DataFrame(obj_w.Data, index= [temp_col] ,columns=obj_w.Times )
                df2 = df2.T  
                df2["date"] = df2.index
                ### notes:wind下载的涨跌幅是 1.20 % 这样的百分比数据，需要转换
                df2["pct_chg"] =df2["pct_chg"]/100

            else :
                print("Debug:temp_code ,",  obj_w ) 
        
        #################################################### 
        ### 计算wind-api用量,并保存到excel
        if obj_w.ErrorCode == 0 : 
            count_col = 1 
            self.api_count["wsd"] = count_col * count_date 
            result = self.save_api_count() 

        ##########################################
        ### 判断是否已有该指数、股票、债券的历史日收益率文件，并获取截至最新的日期。
        if obj_w.ErrorCode == 0 : 
            file_name= "wsd_" + temp_code + ".xlsx"
            if os.path.exists( self.path_wsd + file_name  ):
                ### 读取文件,判断每个指标数值对应的最新日期
                df_old = pd.read_excel( self.path_wsd + file_name  )
                df_old.index = df_old["date"]
                ### 删除重复项,根据"date"列去重；因为出现2个一样日期列，导致报错
                df_old = df_old.drop_duplicates(["date"],keep="last" )
                
                ### 判断列名中是否有 temp_col
                df_all = df2.append( df_old )
            else :
                ### 没有文件，只用下载到的数据保存
                df_all = df2
            
        ####################################################
        ### 合并后，需要计算全历史 pct_chg
        ### 删除日期重复项
        df_all = df_all.drop_duplicates( subset=["date"])
        ### 升序排列
        if obj_w.ErrorCode == 0 : 
            df_all = df_all.sort_values( by="date" )

            ####################################################
            ### 合并后计算百分比变化率 pct_change() || notes:第一个值会是NaN，最好先合并收盘价再算
            if not temp_col == "pct_chg" :
                ### 通常是 "NAV_adj"                 
                df_all["pct_chg"] = df_all[ temp_col ].pct_change() 
                
            
            ### 填补空值NaN，method：ffill 用后边值填前边; bfill ，用前边值填后边
            df_all[ "pct_chg" ] = df_all[ "pct_chg" ].fillna( 0.0 )

            ### 根据"pct_chg" 计算 unit ||
            df_all["unit"] = df_all[ "pct_chg" ]+1
            df_all["unit"] = df_all[ "unit" ].cumprod()
            
            ####################################################
            ### save to output
            ### 删除日期重复项
            df_all = df_all.drop_duplicates( subset=["date"])

            df_all.to_excel( self.path_wsd + file_name ,index=False  )
            ### 全部历史百分比变动率
            obj_m["df_pct_chg"] = df_all 
            
            ### 给定区间的百分比变动率 
            obj_m["df_period"] = df_all.loc[df2.index, : ].sort_values( by="date" )

        return obj_m

    def get_wss_fund_perf(self,obj_f):
        ###########################################################################
        ### 给定基金代码、区间、获取基金和基金经理绩效指标
        ### notes：wss一次提取多个指标时，所有指标对应的时间区间必须一样
        ### INPUT
        temp_fund_code = obj_f[ "fund_code"] 
        ### 偏股混合型基金指数 885001.WI ; benchmark_code = obj_f[ "benchmark_code"] 
        ### 获取区间开始和结束日期
        date_begin = obj_f[ "date_begin"] 
        date_end = obj_f[ "date_end"]  
        #################################################### 
        # w.wss("720001.OF", "NAV_adj_return,return","startDate=20220114;endDate=20220214;annualized=0;tradeDate=20220213;fundType=1") 
        ### 今年以来回报排名periodreturnranking ytd，近1月回报排名periodreturnranking_1m ;近3月回报排名periodreturnranking_3m ;近6月回报排名periodreturnranking_6m
        # w.wss("720001.OF", "periodreturnranking_ytd,periodreturnranking_1m,periodreturnranking_3m,periodreturnranking_6m","tradeDate=20220213;fundType=1")
        ### 最大回撤risk_maxdownside 基金经理（现任）fund_fundmanager；任职日期fund_manager_startdate ; 任职基金总规模fund_manager totalnetasset ;
        # 现任基金最佳回报fund manager bestperformance；任期最大回撤|fund manager maxdrawdown ; 
        # w.wss("720001.OF", "risk_maxdownside,fund_fundmanager,fund_manager_startdate,fund_manager_totalnetasset,fund_manager_bestperformance,fund_manager_maxdrawdown,fund_managerindex_return","startDate=20211214;endDate=20220214;order=1;unit=1;topN=1;index=1")
        # 720001.0F  7.058352金样才,2014/11/19,6978687519.42,41064466462,-54.064% ,1.542803
        # 基金经理指数区间回报（算术平均）fund_managerindex_return：1.542803 表示 720001.OF 从 20211214-to-20220214 收益率 1.54% 
        count_col = 0 
        ### 
        from WindPy import w
        w.start()
        #################################################### 
        ### 区间类涨跌幅（没有排名）|| 这2个指标的收益率数据是一样的：区间复权单位净值增长率NAV_adj_return ;区间回报return ;
        # 区间\复权单位净值增长率NAV_adj_return；区间\最大回撤
        col_list_perf = ["NAV_adj_return","risk_maxdownside"]
        col_list_str_perf = "NAV_adj_return,risk_maxdownside"
        count_col = count_col + 2 
        # "720001.OF", "NAV_adj_return,return","startDate=20220114;endDate=20220214;annualized=0;tradeDate=20220213;fundType=1"
                
        #################################################### 
        ### 非区间类涨跌幅及排名 
        # return_ytd,今年以来回报； return_1m，近1个月回报
        col_list_perf = col_list_perf +  ["return_ytd","return_1m","return_3m","return_6m"]
        col_list_str_perf = col_list_str_perf + "," + "return_ytd,return_1m,return_3m,return_6m"
        # 今年以来回报排名,periodreturnranking_ytd; 近1月回报排名periodreturnranking_1m ;近3月回报排名periodreturnranking_3m ;近6月回报排名periodreturnranking_6m
        col_list_perf = col_list_perf + ["periodreturnranking_ytd","periodreturnranking_1m","periodreturnranking_3m","periodreturnranking_6m"]
        col_list_str_perf = col_list_str_perf +"," + "periodreturnranking_ytd,periodreturnranking_1m,periodreturnranking_3m,periodreturnranking_6m"
        count_col = count_col + 8

        para_str = "startDate=" + date_begin + ";endDate=" + date_end +";annualized=0;tradeDate=" + date_end + ";fundType=1"
        print("Debug=", temp_fund_code, col_list_str_perf, para_str )
        obj_w = w.wss(temp_fund_code, col_list_str_perf, para_str )           
        ### notes:返回的指标都是大写！ 
        # NAV_ADJ_RETURN,RISK_MAXDOWNSIDE,RETURN_YTD,RETURN_1M,RETURN_3M,RETURN_6M,PERIODRETURNRANKING_YTD,PERIODRETURNRANKING_1M,PERIODRETURNRANKING_3M,PERIODRETURNRANKING_6M]
        ### 
        print("obj_w \n", obj_w)
        if obj_w.ErrorCode == 0 :
            ### notes: 这里index不能用 obj_w.Fields
            df1=pd.DataFrame(obj_w.Data, index= col_list_perf )

            #################################################### 
            ### 基金简称；基金经理（现任）fund_fundmanager；任职日期fund_manager_startdate ;几何平均年化回报率,fund_manager_geometricannualizedyield 
            # 任职基金总规模fund_manager_totalnetasset ;；任期最大回撤|fund manager maxdrawdown ; #这个不要：基金经理指数区间回报（算术平均）fund_managerindex_return
            # notes：有了任职日期和几何年化回报，就可以计算出任职以来的累计回报率。 
            col_list_perf_2 = ["name_official","fund_fundmanager","fund_manager_startdate","fund_manager_geometricannualizedyield"]
            col_list_perf_2 = col_list_perf_2 +  ["fund_manager_totalnetasset","fund_manager_maxdrawdown"]
            col_list_str_perf_2 = "name_official,fund_fundmanager,fund_manager_startdate,fund_manager_geometricannualizedyield"
            col_list_str_perf_2 = col_list_str_perf_2 + "," +"fund_manager_totalnetasset,fund_manager_maxdrawdown"
            count_col = count_col + 6
            para_str = "order=1;returnType=1" 
            ### 
            print("Debug=", temp_fund_code, col_list_str_perf_2 , para_str )
            obj_w = w.wss(temp_fund_code, col_list_str_perf_2 , para_str )  
            print("obj_w \n", obj_w)
            if obj_w.ErrorCode == 0 :
                ### notes: 这里index不能用 obj_w.Fields
                df2=pd.DataFrame(obj_w.Data, index= col_list_perf_2)
                #################################################### 
                ### 部分数据转换： 日期 
                # notes :"fund_manager_startdate" 返回的原始日期类型是list, [datetime.datetime(2014, 11, 19, 0, 0)]
                # 转化成df后的类型是Timestamp，数值如  dt.datetime.strftime
                import datetime as dt 
                df2.loc[ "fund_manager_startdate",0] = dt.datetime.strftime(  df2.loc[ "fund_manager_startdate",0] , "%Y%m%d")
                ### 亿元转换
                df2.loc[ "fund_manager_totalnetasset",0] = df2.loc[ "fund_manager_totalnetasset",0]/100000000
                
                ### notes: index 是指标，不能ignore
                df2= df2.append(df1 , ignore_index=False)

                ### transpose
                obj_f["df_perf"] = df2.T
            else :
                ### transpose
                obj_f["df_perf"] = df1.T
        
        #################################################### 
        ### 简历,fund_manager_resume
        if obj_f[ "if_jjjl_resume"] in [1,"1" ] :
            para_str = "order=1;returnType=1" 
            count_col = count_col + 5
            obj_w = w.wss(temp_fund_code, "fund_manager_resume", para_str )  
            obj_f["fund_manager_resume"] = obj_w.Data[0][0]
        
        ### notes:返回的指标都是大写！ 
        # NAV_ADJ_RETURN,RISK_MAXDOWNSIDE,RETURN_YTD,RETURN_1M,RETURN_3M,RETURN_6M,PERIODRETURNRANKING_YTD,PERIODRETURNRANKING_1M,PERIODRETURNRANKING_3M,PERIODRETURNRANKING_6M]
        # NAME_OFFICIAL,FUND_FUNDMANAGER,FUND_MANAGER_STARTDATE,FUND_MANAGER_GEOMETRICANNUALIZEDYIELD,FUND_MANAGER_TOTALNETASSET,FUND_MANAGER_MAXDRAWDOWN

        #################################################### 
        

        #################################################### 
        ### 计算wind-api用量,并保存到excel
        # count_col  
        count_port = 1 
        self.api_count["wss"] =  count_col * count_port 
        result = self.save_api_count() 

        return obj_f

    
    def get_wsd_fund_unit(self,obj_f):
        ###########################################################################
        ###  给定基金代码、区间、获取基金净值
        ### INPUT
        temp_fund_code = obj_f[ "fund_code"] 
        ### 偏股混合型基金指数 885001.WI
        # benchmark_code = obj_f[ "benchmark_code"] 
        ### 获取区间开始和结束日期
        date_begin = obj_f[ "date_begin"] 
        date_end = obj_f[ "date_end"]  
        #################################################### 
        ### 日收益率：w.wsd("720001.OF", "NAV_adj", "2022-01-01", "2022-02-13", "PriceAdj=F")
        ### 周收益率：w.wsd("720001.OF", "NAV_adj", "2022-01-01", "2022-02-13", "Period=W;PriceAdj=F")
        count_col = 0 
        ### 除了给定交易日，其他都按照week 周度频率提取数据
        if obj_f["unit_type"] == "day" :
            # "PriceAdj=F" 
            para_end = "Period=D;PriceAdj=F"
        # elif obj_f["unit_type"] == "week" :
        else : 
            ### "unit_type" =="week"
            para_end = "Period=W;PriceAdj=F"
        from WindPy import w
        w.start()
        #################################################### 
        ### 提取的指标，默认只有 "NAV_adj"
        if "col_list_wind_api" in obj_f.keys() :
            col_list_wind_api = obj_f["col_list_wind_api"]
            ### col_list_wind_api 应该类似于 "NAV_adj,return_1m"
            ### notes:若指标不只是NAV_adj，有可能需要设置其他参数，否则会报错。如return_1m 指标需要设置参数 annualized=0 ,用于判断是否换算年化收益率
            obj_w = w.wsd( temp_fund_code , col_list_wind_api, date_begin, date_end, para_end )
        else :
            col_list_wind_api = "NAV_adj"
            obj_w = w.wsd( temp_fund_code , "NAV_adj", date_begin, date_end, para_end )

        #################################################### 
        ### Debug判断返回数据是否出错
        if obj_w.ErrorCode == 0 :
            ### notes: 这里index不能用 obj_w.Fields
            df1 = pd.DataFrame(obj_w.Data, index= ["NAV_adj"] ,columns=obj_w.Times )
            df1 =df1.T
            df1["unit_fund"] = df1["NAV_adj"]/ df1["NAV_adj"].values[0]
            df_unit= df1
            ##########################################
            ### 计算涨跌幅 | 需要df里时间序列数据升序排列
            df_unit["pct_chg_fund"] = df_unit["unit_fund"].pct_change(1)
            df_unit["pct_chg"] = df_unit["pct_chg_fund"]

            #################################################### 
            ### 计算wind-api用量,并保存到excel
            # count_col = 1 
            count_col = len( col_list_wind_api.split(",") ) 
            ### 计算交易日数量
            obj_date= w.tdayscount( date_begin, date_end, "")
            count_date = obj_date.Data[0][0] 
            self.api_count["wsd"] =  count_col * count_date 
            result = self.save_api_count()  
                
        else : 
            print("Error,wind_api: check input var: wsd()", temp_fund_code , "NAV_adj", date_begin, date_end, para_end )
            print( obj_w )
        
        ##############################################################################
        ### 下载基准指数
        print("keys=" , obj_f.keys()  )
        if "benchmark_code" in obj_f.keys() or "bench_code" in obj_f.keys() :
            ### 获取基准地区间净值or收益率
            ### 偏股混合型基金指数 885001.WI
            if "benchmark_code" in obj_f.keys()  :
                benchmark_code = obj_f[ "benchmark_code"] 
            else :
                benchmark_code = obj_f[ "bench_code"] 
            
            #################################################### 
            ### 可能地代码类型：885001.WI、 000300.SH、159915.SZ、000333.SZ、519979.OF
            # 收盘价：w.wsd("885001.WI", "close", "2022-01-01", "2022-02-13", "Period=W;PriceAdj=F")
            # 涨跌幅：w.wsd("885001.WI", "pct_chg", "2022-01-01", "2022-02-13", "Period=W;PriceAdj=F")
            ### notes:涨跌幅pct_chg、close和定点复权"close2",适用于股票和指数"000300.SH,000333.SZ,159915.SZ； 不适用于 基金 519979.OF
            # w.wsd("000300.SH,000333.SZ,159915.SZ", "close2", "2021-12-31", "2022-02-13", "adjDate=20220214;Period=W;PriceAdj=F")
            if benchmark_code[-3:] == ".OF" :
                obj_w = w.wsd( benchmark_code , "NAV_adj", date_begin, date_end, para_end )
                if obj_w.ErrorCode == 0 :
                    ### notes: 这里index不能用 obj_w.Fields
                    df2 = pd.DataFrame(obj_w.Data, index= ["NAV_adj"] ,columns=obj_w.Times )
                    df2 = df2.T
                    df2["unit_bench"] = df2["NAV_adj"]/ df2["NAV_adj"].values[0]
                    df_unit= df_unit.append( df2.loc[:, "NAV_adj" ] )
            else :
                ### 涨跌幅pct_chg、close和定点复权"close2" 都可以;但是 885001.WI 不能用 "close2"
                temp_col = "close"
                obj_w = w.wsd( benchmark_code , temp_col, date_begin, date_end, para_end )
                if obj_w.ErrorCode == 0 : 
                    ### notes: 这里index不能用 obj_w.Fields
                    df2 = pd.DataFrame(obj_w.Data, index= [temp_col] ,columns=obj_w.Times )
                    df2 = df2.T
                    df2["unit_bench"] = df2[ temp_col ]/ df2[ temp_col].values[0]
                    df_unit["unit_bench"] = df2["unit_bench"]

            ##########################################
            ### 计算涨跌幅 | 需要df里时间序列数据升序排列
            df_unit["pct_chg_bench"] = df_unit["unit_bench"].pct_change(1) 

            

            print("Debug== df-unit \n", df_unit )
            #################################################### 
            ### 计算wind-api用量,并保存到excel
            count_col = 1 
            self.api_count["wps"] = count_col * count_date 
            result = self.save_api_count()


        #################################################### 
        ### output: 
        df_unit["date"] = df_unit.index
        ### 判断返回数据是否出错 if obj_w.ErrorCode == 0
        obj_f["ErrorCode"] = obj_w.ErrorCode

        obj_f["df_unit"] = df_unit

        return obj_f
    
    
    def get_index_indi_data (self,obj_index):
        ###########################################################################
        ###  给定基金基准指数代码、日期，获取月度收益率 
        ### INPUT
        date_start = obj_index["date_start"] 
        date_end = obj_index["date_end"]  
        
        ### 导入配置文件中的指数列表 
        temp_file = "fund_indi_manage.xlsx"
        path_file = self.obj_config["dict"]["path_data_pms"]
        df_benchmark = pd.read_excel( path_file + temp_file ,sheet_name="benchmark")
        df_benchmark = df_benchmark[ df_benchmark["if_use"] ==1 ] 
        list_index_code = list( df_benchmark["code"] )
        
        from WindPy import w
        w.start() 
        ###  
        # w.wss("CBA02501.CS,000832.CSI", "pct_chg_per","startDate=20230228;endDate=20230331")
        str_para = "startDate="+ str(date_start) +";endDate="+ str(date_end ) 
        obj1 = w.wss(list_index_code,"pct_chg_per", str_para )    
        time.sleep(0.3)

        ### 判断是否报错
        if obj1.ErrorCode == 0 :
            ### 合并数据
            code_list_all = obj1.Codes
            data_list = obj1.Data[0]  
            ### 这时index是指数代码，需要转置
            obj_index["df_index"] = pd.DataFrame( obj1.Data[0] ,index=obj1.Codes ,columns=[ str(date_end)] )  
            obj_index["df_index"] =obj_index["df_index"].T

        else :
            # 报错的情况
            print("Error:obj1 \n ", obj1 )
            print("code_list" ,list_index_code )  
            print("Debug=================== \n", obj1 )


        #################################################### 
        ### 计算wind-api用量,并保存到excel 
        count_col = len( list_index_code ) 
        ### 计算交易日数量 obj_date= w.tdayscount( date_begin, date_end, "") || count_date = obj_date.Data[0][0] 
        self.api_count["wss"] =  count_col 
        result = self.save_api_count()   

        return obj_index 


    ######################################################################################################
    ### tdays
    def get_tdays(self,obj_dt  ):
        ### 获取日期
        
        from WindPy import w
        w.start()
        # obj_w = w.tdays("2019-07-17", "2019-08-14", "")
        
        obj_w = w.tdays( obj_dt["date_begin"], obj_dt["date_end"], "")
        
        '''obj_w
        .ErrorCode=0
        .Codes=[]
        .Fields=[]
        .Times=[20220905,20220906,20220907,20220908,20220909,20220913,20220914,20220915,20220916,20220919,...]
        .Data=[[2022-09-05 00:00:00,2022-09-06 00:00:00,2022-09-07 00:00:00,2022-09-08 00:00:00,2022-09-09 00:00:00,2022-09-13 00:00:00,2022-09-14 00:00:00,2022-09-15 00:00:00,2022-09-16 00:00:00,2022-09-19 00:00:00,...]]

        '''
        print("Debug tdays: \n", obj_w )

        ### notes: 数据是倒置的，所以日期要先放在columns
        df_dt = pd.DataFrame(obj_w.Data, index= ["date"] ,columns=obj_w.Times )
        df_dt=df_dt.T

        ### notes: column=date 格式是datetime, 还需要保存成 str 和 int 格式
        # int是外部excel文件导入df时的默认格式
        import datetime as dt  
        df_dt["date_str"] = df_dt["date"].apply( lambda x :dt.datetime.strftime(x,"%Y%m%d" ))
        df_dt["date_int"] = df_dt["date_str"].apply( lambda x : int(x) )

        obj_dt["df_dt"] = df_dt



        
        return obj_dt