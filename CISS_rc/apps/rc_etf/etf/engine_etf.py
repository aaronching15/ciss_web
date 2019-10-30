# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function: realize engine(loop) function for ETF management 
功能：实现ETF-pcf 模拟组合的循环测试和验证 

last update 1 | since 190708

Menu :
    1,
    2,


notes:
    1,

derived from test_etf.py
===============================================
'''
#############################################################################

import sys
import pandas as pd 
path_ciss_rc= "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\"
### reference the absolute path 
sys.path.append( path_ciss_rc )


class ETF_manage():
    #############################################################################

    def __init__(self):
        self.name = "ETF management module"

    def menu4etf(self) :
        #############################################################################
        ### print function menu for this module in structural tree
        #
        print(" ")
        print(" ")

        print("gen_port")
        print("gen_port: generate portfolio using pcf or index constitutes ")

        print("update_port ")
        print("update_port: ")

        print("get_pcf_file")
        print("get_pcf_file Import pcf file from sse website and save in csv format,转码PCF文件")

        return result 

    def gen_port(self,port_name,config_port,df_head,df_stocks):
        #############################################################################
        ### generate portfolio using pcf or index constitutes
        # 2种新建组合方式：1，从pcf文件新建；2，从指数成分新建
        # type_gen_etf in { "pcf","const"  }
        '''todo list 
        1, 新建组合相关文件
            1.1,head file object and path
            1.2,
        '''
        type_gen_etf =config_port["type_gen_etf"] # "pcf"
        #############################################################################
        ### import necessarily module 

        ### Import data_IO modules
        from db.data_io import data_wind

        ### Import CISS modules
        path_base= "D:\\CISS_db\\" 
        # strategy and relative functions 
        from db.func_stra import stra_allocation
        # alogrithm and optimizer module 
        from db.algo_opt import optimizer
        # singals that connect strategy and simulation 
        from db.signals import signals
        # trade management
        from db.trades import manage_trades
        # generating,operating modules for portfolio import,updatean output
        from db.ports import gen_portfolios,manage_portfolios
        # abm模型的参数 config_apps_abm
        from config.config_apps_abm import config_apps_abm
        from config.config_IO import config_IO
        ### Add suffix for code from pcf file 
        from db.basics import symbol_admin
        symbol_admin0 = symbol_admin()

        # import time module 
        from db.times import times
        times0 = times('CN','SSE')
        method4time='stock_index_csi'


        portfolio_gen = gen_portfolios(config_port ,port_name )
        
        ### Setting benchmark for portfolio head
        portfolio_gen.port_head["bench_name"] = config_port["bench_name"]
        portfolio_gen.port_head["bench_code"] = config_port["bench_code"] 

        port_id = portfolio_gen.port_id
        port_head = portfolio_gen.port_head
        port_df = portfolio_gen.port_df

        # now no benchmark

        print("Portfolio has been generated. ")
        print( portfolio_gen.port_head['portfolio_name'] )

        ##############################################################################
        ### Portfolio configurations
        ##############################################################################
        # date_start= temp_date,  date_end=temp_date2
        date_start = config_port["date_start"].replace("-","") # 20140531" 
        date_end = config_port["date_end"].replace("-","")  # "20141130"
        # port_name=  name_id
        init_cash = config_port["init_cash"]

        
        config_apps = config_apps_abm(init_cash,date_start,date_end,port_name ) 

        ##############################################################################
        ### generate portfolio_suites object with  AS,Asum,trades,signal
        ''' 

        INPUT: config_apps,temp_df_growth,sp_name0,port_name
        ALGO: gen_port_suites 
        OUTPUT: stockpool_0,account_0,trades_0, signals_0 contents
        '''

        if config_port["portfolio_type"] == "etf" :
            sp_name0=  config_port["bench_name"] 

        ### Setting initial table of stockpoo 
        # "temp_df_growth" 实质上是 table of stockpool,具体表格内容是没有要求的，建议将当期持仓股票代码列表加入即可
        # df_sp == temp_df_growth

        ### Add suffix for code from pcf file 
        ### SZ for ["00","30","15"]; SH: ["60","68","51"]
        ### notes: ["0","3","1"]  might not work if we have bond codes in portfolios
        df_stocks["code_wind"] = symbol_admin0.raw2wind_list( list( df_stocks.code)  )

        df_sp = df_stocks.loc[:,['code_wind',"code",'name'] ] 

        portfolio_suites = portfolio_gen.gen_port_suites(port_head,config_apps,df_sp,sp_name0,port_name)
        print('portfolio_suites has been generated.')
        print("trade=========================================")

        ##############################################################################
        ### use portfolio ID to load portfolio which just be generated  
        port_id = portfolio_gen.port_head["portfolio_id"]   # id_port_1541729640_rc001_401010
        port_name=  portfolio_gen.port_head["portfolio_name"] #'port_rc001' 

        ##############################################################################
        ### load configuration of portfolio 
        config= config_IO('').load_config_IO_port(port_id,path_base,port_name) 
        config_port = config
        ### save abm-model analytical data to aaps file directory of portfolio 
        import os
        if not os.path.isdir( config_port['path_apps'] ) :
            os.makedirs( config_port['path_apps'] )  

        path_base = config['path_base']

        ##############################################################################
        ### get portfolio object 
        portfolio_manage = manage_portfolios(path_base,config,port_name,port_id )

        ### load portfolio information
        # 导入port数据，确定需要计算的时间周期; port_head记录，导入AS,Asum,sp,trade，signal,signal_nextday等数据
        # (port_head,port_df,config_IO_0 )= portfolio_manage.load_portfolio(port_id,path_base,port_name )

        portfolio_manage.port_head["date_LastUpdate"] =date_start
        port_head =portfolio_manage.port_head
        port_df  =portfolio_manage.port_df
        config_IO_0 = portfolio_manage.config_IO_0       
        print("port_head")
        print( port_head ) 

        ##############################################################################
        '''The strategy process just began.
        the strategy is simple:
        1, for current date, judge if change of symbol universe 
        2,if yes, run ideal weights allocation and generate ana,signal,tradeplan 
        '''
        ##############################################################################
        ### get stockpool 
        stockpool_df = portfolio_suites.stockpool.sp_df
        print("Info of stockpool:")
        print( stockpool_df.info() )
        print( stockpool_df.head() )

        ##############################################################################
        ### Quotation data(historical,feed,...) for stockpool
        ### Method 1:get data by downloading from Wind-API 
        '''
        INPUT:symbol_list ,date_start,data_end,config_IO_0
        ALGO: data_wind
        OUTPUT: quotation data files
        '''  
        # 下载stockpool里所有股票day,or week数据，stockpool_df['code']
        # for temp_code in stockpool_df['code'] :
        #     symbols = temp_code #  '600036.SH' 
        #     # multi-codes with multi-indicators is not supported 
        #     wd1 = data_wind('' ,'' ).data_wind_wsd(symbols,date_start,date_end,'day')
        #     print('symbols ',symbols )
        #     print(wd1.wind_head )
        #     # output wind object to json and csv file 
        #     file_json = wd1.wind_head['id']  +'.json'
        #     with open( config_IO_0['path_data']+ file_json ,'w') as f:
        #         json.dump( wd1.wind_head  ,f) 
        #     file_csv =  wd1.wind_head['id'] +'.csv'
        #     wd1.wind_df.to_csv(config_IO_0['path_data']+file_csv ) 

        ### Method 2: Load quote data from existing quotation directory 
        path0 = 'D:\\db_wind\\'
        path0 = 'D:\\data_Input_Wind\\'
        data_wind_0 = data_wind('' ,path0 )
        quote_type='CN_day'

        ############################################################################## 
        ### Run strategy for allocation weights 
        # Straetgy could be a roughly estimation of how many stock to trade
        # 策略是粗线条的，只说我们要买入多少比例的股票
        stra_weight_list = stra_allocation('').stock_weights_etf(df_head,df_stocks)
        print('weight_list of strategy:')
        print(stra_weight_list)

        ### Build value and growth portfolio, and a mixed portfolio that can dynamically changed over time  
        # 建立 value 组合，growth组合，混合组合{weight_port=[0.5,0.5]}
        # 混合组合随着时间变动，根据业绩调整权重
        stra_estimates_group = {}
        stra_estimates_group['key_1'] = stra_weight_list

        ### Strategy optimizer 
        optimizer_weight_list = optimizer('').optimizer_weight(stra_estimates_group )
        ## 3 methods:
        ## 1, w_allo, only value(current choice )
        ## 2, w_allo, only growth
        ## 3, w_allo, only half value and half growth

        ##############################################################################
        ### Signal generator
        ## get signals by strategy estimations 
        # 交易信号是精细化的，对应了目标的数量，金额，持仓百分比等要素。
        
        portfolio_suites = signals('sig_stra_weight').update_signals_stock_weight(optimizer_weight_list,portfolio_suites)
        signals_list = portfolio_suites.signals.signals_df 

        # signals_list.to_csv("D:\\signals_list.csv")
        
        ##############################################################################
        ### Trade management 
        ### Generate trade plan 
        ## when and which amrket to trade, price or volumne zone for setting trade plan 
        # load trade head file 
        manager_trades = manage_trades('')
        # sty_v_g,sty_v_g is used to judge value , growth or other styles. sty_v_g='value'
        if_rebalance =0 # 0 for the generate period and 1 for the update period 
        ind_level = "0" # 0 is default value for ind_level.,should be str type
        sty_v_g='value' # "value" as default 
        portfolio_suites = manager_trades.manage_tradeplan(if_rebalance,ind_level,sty_v_g,portfolio_suites, signals_list, config_IO_0 ,date_start,date_end,quote_type,data_wind_0 )
        print('trade_plan')
        print( portfolio_suites.trades.tradeplan.info() )

        #### get trade details 
        portfolio_suites= manager_trades.manage_tradebook(portfolio_suites, config_IO_0 ,date_start,date_end,quote_type,data_wind_0 )
        print( 'trades.tradebook' )
        print( portfolio_suites.trades.tradebook.info() )

        ##############################################################################
        ### Portfolio management 
        ### update trades in portfolio_suites, done in previous manage_tradeplan or tradebook

        ### update accounts using trade result
        ## we only update trades that have not been used by accounts
        from db.accounts import manage_accounts

        ###  get trading days using account_sum and date_start,date_end 
        date_list = portfolio_suites.account.account_sum.index
        # print('date_list')
        # print(date_list[date_list<date_end ] )
        #  2014-06-03 to 2014-11-28
        date_list_units = date_list[date_list<date_end ]

        trades_0 = portfolio_suites.trades
        tradebook = trades_0.tradebook

        ### get all trading dates from tradebook
        tradebook['datetime'] = pd.to_datetime(tradebook['date'], format='%Y-%m-%d' ) 
        tradebook =tradebook.sort_values('datetime')
        date_list_trades = list( tradebook['datetime'].drop_duplicates() )

        for temp_date in date_list_units  :
            if_trade =0
            ### Update AS and Asum for tradebook
            if temp_date in date_list_trades :
                # date with trading 
                portfolio_suites = manage_accounts('').update_accounts_with_trades(temp_date,portfolio_suites,config_IO_0,date_start,date_end,quote_type,data_wind_0)
                if_trade =1 

            ### ！！ 这里已经是cash= Nan ，说明是跟新组合时的问题
            df1= portfolio_suites.account.account_sum.loc[temp_date, :] 
            

            ### Update closing price for all holding stocks, whether date with no trading or not, 
            portfolio_suites = manage_accounts('').update_accounts_with_quotes(temp_date,portfolio_suites,config_IO_0,date_start,date_end,quote_type,data_wind_0,if_trade )
            ## we should update statistics results of portfolio_head file before output 
            # Debug Check in case that "cash" is NaN 
            # print( df1 ) 
            # print( portfolio_suites.account.account_sum.loc[temp_date, :] )
            # print( portfolio_suites.account.account_sum.loc[temp_date, 'cash'] )
            # print( portfolio_suites.account.account_sum.loc[temp_date, 'cash'] == np.nan )
            # asdasd = input("Check errors2......") 

            ## for every trading day, Out portfolio_suites to files
            # temp_date  2014-06-03 00:00:00 type is time stamp
            temp_date = dt.datetime.strftime(temp_date,"%Y%m%d")
            print( "temp_date ",type(temp_date),temp_date )

            portfolio_manage.port_head["date_LastUpdate"] = temp_date
            portfolio_suites = portfolio_manage.output_port_suites(temp_date,portfolio_suites,config_IO_0,port_head,port_df)
            




















        return  portfolio_manage,portfolio_suites 
















        return result 

    def update_port(self,) :
        #############################################################################
        ###



        return 


    #############################################################################
    ### Get Data
    #############################################################################
    def get_pcf_file(self,date,name_etf,path_etf) :
        #############################################################################
        ### Import pcf file from sse website and save in csv format
        ### 转码PCF文件

        import json
        import pandas as pd 
        import numpy as np 
        import math

        ### INPUT samples 
        # path_etf = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\apps\\black_litterman\\etf\\"
        # name_etf = "510300"
        # date= "0704"

        with open(path_etf+name_etf+ date+ ".ETF",'r') as f: 
            str= f.read()

        # First split with "\n"
        str_split = str.split("\n")
        # First 14 items 
        # ['[ETF]', 'Fundid1=510301', 'CreationRedemptionUnit=900000', 'MaxCashRatio=0.50000', 'Publish=1']
        # ['CreationRedemption=1', 'Recordnum=300', 'EstimateCashComponent=-20946.00', 'TradingDay=20190704', 'PreTradingDay=20190703']
        # ['CashComponent=-21412.00', 'NAVperCU=3532219.00', 'NAV=3.9247', 'TAGTAG',

        pcf_head = str_split[:14]
        ''' ['[ETF]', 'Fundid1=510301', 'CreationRedemptionUnit=900000', 'MaxCashRatio=0.50000', 'Publish=1', 'CreationRedemption=1'
        , 'Recordnum=300', 'EstimateCashComponent=-20946.00', 'TradingDay=20190704', 'PreTradingDay=20190703', 'CashComponent=-2
        1412.00', 'NAVperCU=3532219.00', 'NAV=3.9247', 'TAGTAG']
        '''
        list_head = [] 
        j=0 
        for temp_str in pcf_head :
            if '=' in temp_str :
                temp_s = temp_str.split("=")
                print( temp_s )
                if j == 0 :
                    list_head = [temp_s]
                else :
                    list_head = list_head + [temp_s]
            j = j+1

        df_head = pd.DataFrame( list_head )
        print(  df_head.info() )

        df_head.columns = ["key","value"]
        # axis=1 means columns
        print(df_head)

        path_out1 = "D:\\df_head_"+ name_etf+ date  +".csv"
        df_head.to_csv(path_out1,encoding="gbk")

        ####################################################################################

        pcf_const = str_split[14:]
        # ['000001', '平安银行', '    2400', '3', '0.10000', '   33624.000', '']
        # stock1 = pcf_const[0].split("|")
        df_stocks = pd.DataFrame( )
        list_0 = [] 

        i=0
        for temp_str in pcf_const :
            # print("temp_str \n", temp_str )

            ### Notes:正常 len(pcf_const[?]) == 44, len(pcf_const[-1])=9{ENDENDEND},len(pcf_const[-2])=0
            if len(temp_str) > 10 :
                temp_s = temp_str.split("|")
                if i == 0 :
                    list_0 =  [temp_s ]
                else :    
                    list_0 = list_0 + [temp_s ]
                
            i=i+1

        # for temp_str in pcf_const :
        #     print("temp_str \n", temp_str )
            
        #     temp_s = temp_str.split("|")
        #     if i == 0 :
        #         df_stocks = pd.DataFrame(temp_s).T
        #     else :
        #         df_stocks = df_stocks.append( pd.DataFrame(temp_s).T )
        #     i=i+1

        df_stocks = pd.DataFrame( list_0 )
        print(  df_stocks.info() )

        df_stocks.columns = ["code","name","num","mark","premium_pct","amount","useless"]
        # axis=1 means columns
        df_stocks = df_stocks.drop("useless",axis=1)

        path_out2 = "D:\\df_stocks_"+ name_etf+ date  +".csv"
        df_stocks.to_csv(path_out2,encoding="gbk")

        '''
        Fundid1 510301 一级市场基金代码
        CreationRedemptionUnit  900000 最小申购、赎回单位(单位:份)
        MaxCashRatio    0.5 现金替代比例上限
        Publish 1 是否需要公布IOPV,是
        CreationRedemption  1 申购赎回的允许情况，申购和赎回皆允许
        Recordnum   300 ？成分股数量
        EstimateCashComponent   -20946 最小申购、赎回单位的预估现金部分(单位:元)
        TradingDay  20190704 最新交易日
        PreTradingDay   20190703 上一交易日
        CashComponent   -21412 现金差额(单位:元)
        NAVperCU    3532219 最小申购、赎回单位净值(单位：元)
        NAV 3.9247 基金份额净值(单位:元)

        '''
        print("File has been saved to")
        print( path_out1 )
        print( path_out2 )

        return df_head,df_stocks






































