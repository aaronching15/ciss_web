# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
Generate portfolio using symbol list as input 
        last 181102 | since 181102     
功能：
### Data of portfolio suites I/O 
1,导出组合数据，输出保存至文件
ports.py\\gen_port_suites

2,导入文件数据，用组合管理引擎分析，评估，更新组合状况。
    1，组合管理对象：ports.py\\class admin_portfolios():
    2，组合管理引擎：bin\\engine_portfolio.py

last update 181103 | since  181102
Menu :
THREE COMPONENTS:
1,class A:
    {INPUT,ALGO,OUTPUT  }
1,function a:
    {INPUT,ALGO,OUTPUT  }
    
分析：
0，main function
    0.1, profit and loss analysis/monitoring
    0.2, performance attributes
    0.3, periodic statistics
    0.4, asset allocation ? | data can be merged into grouped view in accounts,funds or groups
    0.5, risks 
    0.6, rebalance 组合调整
    0.7， 

1，输入:
    1.1,证券池子,如StockPool：
    
2，配置文件 | config\config_port.py

3，股票池和组合当前是 1：N，下一步考虑 N：1，N:N的情况
    例如：如果同时有｛A股，港股，美股｝多个市场情况下，是把所有个股放1个stockpool里，
    还是每个市场单独一个stockpool ?

4,output file 
    1,head json file of portfolio
    2,portfolio dataframe of portfolio 
    3,stockpool dataframe of portfolio

Notes: 
refernce: rC_Portfolio_17Q1.py 
===============================================
'''
import pandas as pd 
import json
import sys
sys.path.append("..") 
###################################################
class portfolios():
    def __init__(self, config={},port_name=''):
        import gen_portfolios
        # generate portfolio object
        self.portfolio = gen_portfolios(config,port_name)





###################################################
class gen_portfolios():
    def __init__(self,config={},port_name=''):
        portfolio_head = self.gen_port_head(config,port_name)
        self.port_name = portfolio_head["portfolio_name"] 
        self.port_id = portfolio_head["portfolio_id"] 
        self.port_head = portfolio_head
        self.port_df = pd.DataFrame()
        # self.sp_df =sp_df
       
         

    def gen_port_head(self, config={},port_name='' ) :
        '''
        Generate portfolio head 
        refernce: rC_Portfolio_17Q1.py 
        previous object : log of portfolio 
        '''
        
        portfolio_head ={}
        ## get stockpool id using  time stamp
        import sys
        sys.path.append("..")
        from db.basics import time_admin
        time_admin1 = time_admin()
        time_stamp = time_admin1.get_time_stamp()

        # if config == {} :
        ## Basic info
        # initial date of generate portfolio 
        portfolio_head["InitialDate"] = "" # previous 
        portfolio_head["Index_Name"] = ""
        if port_name =='':
            portfolio_head["portfolio_name"] = str(time_stamp )
            portfolio_head["portfolio_id"] =  "id_time_" + str(time_stamp )
            portfolio_head["portfolio_id_time"] = str(time_stamp )
        else :
            portfolio_head["portfolio_name"] = port_name
            portfolio_head["portfolio_id"] =  "id_time_" + str(time_stamp)+"_name_"+port_name
            portfolio_head["portfolio_id_time"] = str(time_stamp )

        portfolio_head["MaxN"] = ""
        portfolio_head["Leverage"] = ""
        portfolio_head["date_Start"] = ""
        portfolio_head["date_LastUpdate"] = ""
        portfolio_head["path_SP"] = ""   # previous name = path_Symbol
        portfolio_head["w_equity_max"] = 0.0
        portfolio_head["w_equity_min"] = 0.0
        portfolio_head["w_bond_max"] = 0.0
        portfolio_head["w_bond_min"] = 0.0
        portfolio_head["w_cash_min"] = 0.0 # min level of cash as weight in portfolio
        portfolio_head["info"] = ""
        ## Values 
        portfolio_head["Total_Cost"] = 0.0
        portfolio_head["Cash"] = 0.0
        portfolio_head["Stock"] = 0.0
        portfolio_head["Total"] = 0.0
        portfolio_head["Unit"] = 0.0
        portfolio_head["MDD"] = 0.0
        ## profit,loss, returns,risks and other statistics 
        portfolio_head["PnL"] = 0.0
        portfolio_head["PnL_pct"] = 0.0
        portfolio_head["r_annual"] = 0.0
        portfolio_head["PnL_total"] = 0.0
        portfolio_head["PnL_Pct"] = 0.0
        portfolio_head["total_ProfitReal"] = 0.0
        portfolio_head["total_Profit_R"] = 0.0
        # statistics: max,min,mean,median
        portfolio_head["W_max"] = 0.0
        portfolio_head["W_max_code"] = 0.0
        portfolio_head["profit_max"] = 0.0
        portfolio_head["profit_max_code"] = ""
        portfolio_head["loss_max"] = 0.0
        portfolio_head["loss_max_code"] = ""
        portfolio_head["PnL_Pct_max"] = 0.0
        portfolio_head["PnL_Pct_max_code"] = ""
        portfolio_head["PnL_Pct_min"] = 0.0
        portfolio_head["PnL_Pct_min_code"] = ""
        portfolio_head["num_Trade_Profit"] = 0.0
        portfolio_head["ave_Trade_Profit"] = 0.0
        portfolio_head["total_Loss_R"] = 0.0
        portfolio_head["num_Trade_Loss"] = 0.0
        portfolio_head["total_Fees"] = 0.0
        portfolio_head["W_Ideal_max"] = 0.0
        portfolio_head["W_Ideal_max_code"] = ""
        portfolio_head["ave_Trade_Loss"] = 0.0
        # unit, cash,stocks
        portfolio_head["Unit-5D"] = {}
        portfolio_head["Cash-5D"] = {}
        portfolio_head["Stock-5D"]= {}

        return portfolio_head

    def gen_port_stat(self, sp_df,config={},port_name='' ) :
        '''output_port_suites(
        Generate portfolio using symbol list as input 
        last 181102 | since 181102     
        
        '''
        port_head = self.gen_port_head({},'') 
        import pandas as pd 
        class port():
            def __init__(self, port_name,port_head,sp_df):
                self.port_name = port_head["portfolio_name"]
                self.port_df = pd.DataFrame()
                self.portfolio_head = port_head
                self.sp_df = sp_df
        portfolio = port(port_name,port_head,sp_df)

        return portfolio


    def gen_port_suites(self,port_head,config_apps,temp_df_growth,sp_name0,port_name) :
        ### using stockpool as input to generate portfolio 
        # 应该是不包括apps对象信息的
        # Ana:portfolio中，cash deposit/withdraw 是被动接受。
        # Ana:account 中，由于要算净值，出入金应该放account里，
        # funds是主动管理cash I/O的主体，涉及安排资金流转的功能
        ### 根据带权重的symbol list，用gen_portfolio 模块，建立初始组合，采用不复权价格。
        
        ###
        id_time_stamp = port_head["portfolio_id_time"]  


        ###########################################################
        ### Generate and output SP, Port., Accounts, Trades

        from db.stockpools import gen_stockpools
        # config= {}
        # sp_name0= 'growth_' + str(int_ind3)  
        config ={}
        stockpool_0 = gen_stockpools(id_time_stamp,temp_df_growth,config,sp_name0)
        print("type of sp_df",  type( stockpool_0 )  ) 

        ### Initialize supportting configuration 
        from config.config_IO import config_IO
        ## Import config module 
        config_IO_0 = config_IO('config_name').gen_config_IO_port('',port_name)
        import json
        file_json = stockpool_0.sp_head["id_sp"] +'.json'
        with open( config_IO_0['path_stockpools']+ file_json ,'w') as f:
            json.dump( stockpool_0.sp_head ,f) 
        file_csv = stockpool_0.sp_head["id_sp"] +'.csv'
        stockpool_0.sp_df.to_csv(config_IO_0['path_stockpools']+file_csv)

        print("Stockpool has been generated ")
        print( stockpool_0.sp_head )
        print("==============================================")

        ### save to portfolio
        print("Portfolio has been generated ")
        print( self.port_head )
        print("==============================================")

        # config_IO_0['path_accounts'] == 'D:\\CISS_db\\rc001\\ports\\' 
        ###########################################################
        ### IO| Output stockpool into output file
        file_json = self.port_head["portfolio_id"] +'.json'
        with open( config_IO_0['path_ports']+ file_json ,'w') as f:
            json.dump( self.port_head ,f) 
        file_csv =  self.port_head["portfolio_id"] +'.csv'
        self.port_df.to_csv(config_IO_0['path_ports']+file_csv )

        ### generate account 
        port_df_0 = self.port_df
        from db.accounts import gen_accounts
        
        init_cash= config_apps.config['init_cash']

        # todo dates? comes from where ?
        init_date=config_apps.config['date_start'].replace("-",'') # "2014-05-31"
        account_name= config_apps.config["account_name"] # 'rc001'

        account_0 = gen_accounts(id_time_stamp,port_df_0,init_date,config,init_cash,account_name)
        print("Accounts have been generated ")
        print( account_0.account_sum.info() )
        print( account_0.account_stock.info() )
        # print( account_0.account_bond.info() )
        print("==============================================")
        ## IO| Output account into file
        file_json = account_0.account_head["account_id"] +'.json'
        with open( config_IO_0['path_accounts']+ file_json ,'w') as f:
            json.dump( account_0.account_head ,f) 
        file_csv_as = account_0.account_head["account_id"]+ '_AS' +'.csv'
        file_csv_ab = account_0.account_head["account_id"]+ '_AB' +'.csv'
        file_csv_asum = account_0.account_head["account_id"]+ '_Asum' +'.csv'
        account_0.account_sum.to_csv(config_IO_0['path_accounts']+file_csv_asum )
        account_0.account_stock.to_csv(config_IO_0['path_accounts']+file_csv_as )
        account_0.account_bond.to_csv(config_IO_0['path_accounts']+file_csv_ab )

        ###########################################################
        ### generate trade book 
        from db.trades import gen_trades 
        trades_0 = gen_trades(id_time_stamp,config_apps.config["trade_name"])  # 'rc001'
        trades_id = trades_0.tradebook_head['trades_id']
        print('Name of trading object :', trades_id )
        ## IO| Output trade book into file
        # trade head
        file_json = trades_0.tradebook_head["trades_id"] +'.json'
        with open( config_IO_0['path_trades']+ file_json ,'w') as f:
            json.dump( trades_0.tradebook_head ,f) 
        # trade book 
        file_csv_tb = trades_id+ '_TB' +'.csv'
        trades_0.tradebook.to_csv(config_IO_0['path_trades']+file_csv_tb )
        # trade statistics 
        file_csv_tb_stat = trades_id+ '_TB_stat' +'.csv'
        trades_0.tradebook_stats.to_csv(config_IO_0['path_trades']+file_csv_tb_stat )
        # trade plan
        file_csv_tp = trades_id+ '_TP' +'.csv'
        trades_0.tradeplan.to_csv(config_IO_0['path_trades']+file_csv_tp )
        # trade analyzing data/report 
        file_csv_ta = trades_id+ '_TA' +'.csv'
        trades_0.trade_ana.to_csv(config_IO_0['path_trades']+file_csv_ta )

        ### generate signal
        from db.signals import signals 
        signals_0 = signals(id_time_stamp,config_apps.config["signal_name"])  # 'rc001'
        ## IO| Output signals into output file
        file_json = signals_0.signals_head["signals_id"] +'.json'
        with open( config_IO_0['path_signals']+ file_json ,'w') as f:
            json.dump( signals_0.signals_head ,f) 
        file_csv =  signals_0.signals_head["signals_id"] +'.csv'
        signals_0.signals_df.to_csv(config_IO_0['path_signals']+file_csv )

        class portfolio_suites():
            def __init__(self,stockpool_0,account_0,trades_0, signals_0 ):
                self.stockpool = stockpool_0
                self.account = account_0
                self.trades = trades_0
                self.signals = signals_0
        portfolio_suites = portfolio_suites(stockpool_0,account_0,trades_0, signals_0)

        return portfolio_suites


###################################################
class manage_portfolios():
    ### admin: load/dump,update portfolio using given periods 

    ## import portfolio information 
    # dir: D:\CISS_db\rc001\ports
    def __init__(self,path_base, config={},port_name='',port_id=''):
        self.port_name = port_name
        # port_id can help us to locate portfolio head file 
        self.port_id = port_id
        (port_head,port_df,config_IO_0 ) = self.load_portfolio(port_id,path_base,port_name)
        self.port_head = port_head
        self.port_df = port_df
        self.config_IO_0 = config_IO_0

    def load_portfolio(self,port_id='',path_base= '',port_name='') :
        '''
        notes:current engine_portfilio does not use this module | 190109 

        load portfolio information 
        # similar with gen_port_suites(config_apps,temp_df_growth,sp_name0)
        Input： Dates, Path_TradeSys , start_Date, path_Input,
        
        Output： Account_Sum ,Account_Stocks, StockPool ,Trading_Book, temp_Sig_Ana
        reference: def Import_Portfolio_Data_Live(self, Path_TradeSys , start_Date):
        logic: there should be only one head json file in directory of "..\\ports\\",if not

        '''

        ### load configuration  
        ### "from ..config" 表示从上一级文件夹内的config文件夹读取模块 | 190109
        ### Old method to import from adjacent directory
        # sys.path.append("..") 
        # from ..config.config_IO import config_IO
        # New method to include absolute path | since 190412
        sys.path.append("C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\")
        from config.config_IO import config_IO
                
        ## Import config module 
        # 主要是组合目录和子目录信息
        config_IO_0 = config_IO('config_name').load_config_IO_port(port_id,path_base,port_name)
        ## load portfolio head and df 

        ### Get id_time_1544021284_name_port_rc181205_market_value_999.json.
        # Version concise | port_id = 1544021284
        # file_json = "id_time_"+port_id_time+"_name_"+ port_name +'.json' 
        # file_csv =  "id_time_"+port_id+"_name_"+ port_name  +'.csv'
        # Version complete 
        # port_id = id_time_1544021284_name_port_rc181205_market_value_999
        # but port_id_time  = 1544021284
        file_json = port_id +'.json' 
        file_csv =  port_id + '.csv'

        # TypeError: the JSON object must be str, bytes or bytearray, not 'TextIOWrapper'
        # with open( config_IO_0['path_ports']+ file_json ,'w') as f:
        #     port_head = json.load( f) 
        
        # Qs pd.read_json() will import empty port_head
        # port_head = pd.read_json( config_IO_0['path_ports']+ file_json ) 
        with open(config_IO_0['path_ports']+ file_json, 'r') as f: 
            port_head = json.loads(f.read())
        # type of port_head is dict  

        ### Get id_time_1544021284_name_port_rc181205_market_value_999.json
        
        port_df = pd.read_csv(config_IO_0['path_ports']+file_csv )

        return port_head,port_df,config_IO_0 

    def load_portfolio_suites(self,date_LastUpdate ,config_IO_0,port_head,port_df,port_name,sp_name0) :
        # load portfolio suites using 
        # last 181115 
        ###########################################################
        ### IO| Input stockpool into output file

        #######################################################################        
        ### IO stockpool   
        id_time_stamp = port_head["portfolio_id_time"]
        str_date = date_LastUpdate.replace("-","")
        id_sp =  "id_sp_"+id_time_stamp+"_"+sp_name0
        print("str_date ", str_date  )
        print("id_sp " , id_sp )

        class stockpool:
            def __init__(self,id_time_stamp,config_IO_0,id_sp, str_date,sp_name0) :
                # load sp_head frpm outside file 
                # sp_name0=  str(int_ind3) 
                
                # Qs :TypeError: the JSON object must be str, bytes or bytearray, not 'TextIOWrapper'
                # Ana:error json file with date is empty for lates version
                # Ans: open(~,'w' )中 "w"表示打开文件写入，但是 这里我们需要的是read，所以应该用'r'
                file_json_date = id_sp +'.json'
                # file_json_date = id_sp +'_'+str_date  +'.json'
                print( config_IO_0['path_stockpools']+ file_json_date )
                with open( config_IO_0['path_stockpools']+ file_json_date ,'r') as f:
                    # sp_head = json.loads( f )  will bring error 
                    sp_head = json.loads( f.read() ) 
                self.sp_head = sp_head
                # print("sp_head 666")
                # print( sp_head )

                file_csv_date = id_sp +'_'+str_date +'.csv' 
                sp_df = pd.read_csv( config_IO_0['path_stockpools']+file_csv_date)
                self.sp_df = sp_df
         
        # print( file_csv_date )
        # asd # 后缀加 最新日期！！！~ 
        stockpool_0 = stockpool(id_time_stamp,config_IO_0,id_sp,str_date,sp_name0)
        print("Stockpool has been import ")
        print( stockpool_0.sp_head )

        #######################################################################        
        ### IO stockpool account 
        account_name= port_name
        class accounts():
            def __init__(self,id_time_stamp,config_IO_0 ,str_date):
                id_account = "id_account_" + id_time_stamp+"_"+ account_name
                file_json_date = id_account +'_'+str_date  +'.json'
                with open( config_IO_0['path_accounts']+ file_json_date ,'r') as f:
                    account_head = json.loads( f.read() ) 
                self.account_head = account_head

                file_csv_as   = id_account + '_AS'  +'_'+str_date +'.csv'
                file_csv_ab   = id_account + '_AB'  +'_'+str_date +'.csv'
                file_csv_asum = id_account + '_Asum'+'_'+str_date +'.csv'
                account_sum   = pd.read_csv(config_IO_0['path_accounts']+file_csv_asum )
                print( account_sum.head() )
                if "Unnamed: 0" in account_sum.columns :
                    account_sum.index = account_sum["Unnamed: 0"]
                    account_sum= account_sum.drop(["Unnamed: 0"],axis =1) 
                
                account_stock = pd.read_csv(config_IO_0['path_accounts']+file_csv_as )
                # assign original index to account stock 
                if "Unnamed: 0" in account_stock.columns :
                    account_stock.index = account_stock["Unnamed: 0"]
                    account_stock= account_stock.drop(["Unnamed: 0"],axis =1) 
                account_bond  = pd.read_csv(config_IO_0['path_accounts']+file_csv_ab )
                if "Unnamed: 0" in account_bond.columns :                    
                    account_bond.index = account_bond["Unnamed: 0"]
                    account_bond= account_bond.drop(["Unnamed: 0"],axis =1) 

                account_sum.index= account_sum['date']
                self.account_sum  = account_sum
                # index should be date but 0,1,2 if imported 
                self.account_sum.index  = account_sum['date']
                self.account_stock= account_stock
                self.account_bond = account_bond


        account_0 = accounts(id_time_stamp,config_IO_0 ,str_date)

        #######################################################################        
        ### IO trade book
        trades_name = port_name 
        class trades():
            def __init__(self,trades_name, str_date): 
                # trades_id = id_time_stamp +"_"+ trades_name
                # trade head
                file_json_date = "trades_id_"+ id_time_stamp +"_"+ trades_name+"_"+ str_date +'.json'
                with open( config_IO_0['path_trades']+ file_json_date ,'r') as f:
                    tradebook_head = json.loads( f.read() ) 
                self.tradebook_head = tradebook_head

                # trade book 
                file_csv_tb = "trades_id_"+ id_time_stamp +"_"+trades_name+ '_TB'+"_"+ str_date +'.csv'
                tradebook = pd.read_csv(config_IO_0['path_trades']+file_csv_tb )
                if "Unnamed: 0" in tradebook.columns :
                    tradebook.index = tradebook["Unnamed: 0"]
                    tradebook= tradebook.drop(["Unnamed: 0"],axis =1) 

                self.tradebook = tradebook 
                # trade statistics 
                file_csv_tb_stat = "trades_id_"+ id_time_stamp +"_"+trades_name+ '_TB_stat'+"_"+ str_date +'.csv'
                tradebook_stats = pd.read_csv(config_IO_0['path_trades']+file_csv_tb_stat )
                if "Unnamed: 0" in tradebook_stats.columns :
                    tradebook_stats.index = tradebook_stats["Unnamed: 0"]
                    tradebook_stats= tradebook_stats.drop(["Unnamed: 0"],axis =1)
                self.tradebook_stats = tradebook_stats

                # trade plan
                file_csv_tp = "trades_id_"+ id_time_stamp +"_"+trades_name+ '_TP'+"_"+ str_date +'.csv'
                tradeplan = pd.read_csv(config_IO_0['path_trades']+file_csv_tp )
                if "Unnamed: 0" in tradeplan.columns :
                    tradeplan.index = tradeplan["Unnamed: 0"]
                    tradeplan= tradeplan.drop(["Unnamed: 0"],axis =1) 

                self.tradeplan = tradeplan 
                # trade analyzing data/report 
                file_csv_ta = "trades_id_"+ id_time_stamp +"_"+trades_name+ '_TA'+"_"+ str_date +'.csv'
                trade_ana = pd.read_csv(config_IO_0['path_trades']+file_csv_ta )
                if "Unnamed: 0" in trade_ana.columns :
                    trade_ana.index = trade_ana["Unnamed: 0"]
                    trade_ana= trade_ana.drop(["Unnamed: 0"],axis =1) 
                self.trade_ana = trade_ana

        trades_0 = trades(trades_name, str_date)

        #######################################################################        
        ### IO generate signal
        signals_name = port_name
        # ????要把 id_time_stamp, 放进signals.py
        class signals():
            def __init__(self,id_time_stamp,signals_name,str_date,config_IO_0) :
                # signal head 
                file_json_date = "id_signals_" + id_time_stamp +"_"+signals_name +"_"+ str_date +'.json'
                with open( config_IO_0['path_signals']+ file_json_date ,'r') as f:
                    self.signals_head = json.loads( f.read() ) 
                
                # signal df 
                file_csv_date ="id_signals_" + id_time_stamp +"_"+signals_name +"_"+ str_date +'.csv'
                signals_df = pd.read_csv(config_IO_0['path_signals']+file_csv_date )
                self.signals_df = signals_df

        signals_0 = signals(id_time_stamp,signals_name,str_date,config_IO_0)

        class portfolio_suites():
            def __init__(self,stockpool_0,account_0,trades_0, signals_0 ):
                self.stockpool = stockpool_0
                self.account = account_0
                self.trades = trades_0
                self.signals = signals_0
        portfolio_suites = portfolio_suites(stockpool_0,account_0,trades_0, signals_0)


        return portfolio_suites

    def output_port_suites(self,temp_date,portfolio_suites,config_IO_0,port_head,port_df ) :
        # sp_name0 = str(int_ind3)
        # save contents of portfolio to output csv file
        # derived from def gen_port_suites(self)
              
        stockpool_0 = portfolio_suites.stockpool
        account_0 = portfolio_suites.account 
        trades_0 = portfolio_suites.trades
        signals_0 = portfolio_suites.signals  

        # datetime to string 
        # import datetime as dt 
        # str_date = dt.datetime.strftime(temp_date,"%Y%m%d")
        str_date = temp_date.replace("-","") # 20140603
        print('str_date',str_date)
    
        #######################################################################        
        ### save stockpool 
        file_json = stockpool_0.sp_head["id_sp"] +'.json'
        with open( config_IO_0['path_stockpools']+ file_json ,'w') as f:
            json.dump( stockpool_0.sp_head ,f) 
        file_json_date = stockpool_0.sp_head["id_sp"]+'_'+str_date  +'.json'
        with open( config_IO_0['path_stockpools']+ file_json_date ,'w') as f:
            json.dump( stockpool_0.sp_head ,f) 
        
        file_csv = stockpool_0.sp_head["id_sp"] +'.csv'
        stockpool_0.sp_df.to_csv(config_IO_0['path_stockpools']+file_csv)
        file_csv_date = stockpool_0.sp_head["id_sp"]+'_'+str_date +'.csv'
        stockpool_0.sp_df.to_csv(config_IO_0['path_stockpools']+file_csv_date)
        # print( file_csv_date )
        # asd # 后缀加 最新日期！！！~ 
        print("Stockpool has been updated ")
        print( stockpool_0.sp_head )

        #######################################################################
        ### save portfolio
        ## todo at least update date of portfolio head 
        port_head["date_LastUpdate"] = str_date

        file_json = port_head["portfolio_id"] +'.json'
        with open( config_IO_0['path_ports']+ file_json ,'w') as f:
            json.dump( port_head ,f) 
        file_json_date = port_head["portfolio_id"]+'_'+str_date  +'.json'
        with open( config_IO_0['path_ports']+ file_json_date ,'w') as f:
            json.dump( port_head ,f) 

        file_csv =  port_head["portfolio_id"] +'.csv'
        port_df.to_csv(config_IO_0['path_ports']+file_csv )
        file_csv_date =  port_head["portfolio_id"]+'_'+str_date  +'.csv'
        port_df.to_csv(config_IO_0['path_ports']+file_csv_date )

        print("Portfolio has been updated ")

        #######################################################################
        ### save account into file     
        file_json = account_0.account_head["account_id"] +'.json'
        with open( config_IO_0['path_accounts']+ file_json ,'w') as f:
            json.dump( account_0.account_head ,f) 
        file_json_date = account_0.account_head["account_id"] +'_'+str_date +'.json'
        with open( config_IO_0['path_accounts']+ file_json_date ,'w') as f:
            json.dump( account_0.account_head ,f) 

        file_csv_as = account_0.account_head["account_id"]+ '_AS' +'.csv'
        file_csv_ab = account_0.account_head["account_id"]+ '_AB' +'.csv'
        file_csv_asum = account_0.account_head["account_id"]+ '_Asum' +'.csv'
        account_0.account_sum.to_csv(config_IO_0['path_accounts']+file_csv_asum )
        account_0.account_stock.to_csv(config_IO_0['path_accounts']+file_csv_as )
        account_0.account_bond.to_csv(config_IO_0['path_accounts']+file_csv_ab )

        file_csv_as_date = account_0.account_head["account_id"]    + '_AS' +'_'+str_date +'.csv'
        file_csv_ab_date = account_0.account_head["account_id"]    + '_AB' +'_'+str_date +'.csv'
        file_csv_asum_date = account_0.account_head["account_id"]+ '_Asum' +'_'+str_date +'.csv'
        account_0.account_sum.to_csv(config_IO_0['path_accounts']+file_csv_asum_date )
        account_0.account_stock.to_csv(config_IO_0['path_accounts']+file_csv_as_date )
        account_0.account_bond.to_csv(config_IO_0['path_accounts']+file_csv_ab_date )
        print("Accouts has been updated ")

        #######################################################################
        ### save trades into file
        # trade head
        print( "trades_0.tradebook_head " )
        print( trades_0.tradebook_head  ) 
        file_json = trades_0.tradebook_head["trades_id"] +'.json'
        with open( config_IO_0['path_trades']+ file_json ,'w') as f:
            json.dump( trades_0.tradebook_head ,f) 

        file_json_date = trades_0.tradebook_head["trades_id"] +'_'+str_date +'.json'
        with open( config_IO_0['path_trades']+ file_json_date ,'w') as f:
            json.dump( trades_0.tradebook_head ,f) 

        # trade book 
        trades_id = trades_0.tradebook_head['trades_id']
        file_csv_tb = trades_id+ '_TB' +'.csv'
        trades_0.tradebook.to_csv(config_IO_0['path_trades']+file_csv_tb )

        file_csv_tb_date = trades_id+ '_TB' +'_'+str_date +'.csv'
        trades_0.tradebook.to_csv(config_IO_0['path_trades']+file_csv_tb_date )
        # trade statistics 
        file_csv_tb_stat = trades_id+ '_TB_stat' +'.csv'
        trades_0.tradebook_stats.to_csv(config_IO_0['path_trades']+file_csv_tb_stat )

        file_csv_tb_stat_date = trades_id+ '_TB_stat'+'_'+str_date  +'.csv'
        trades_0.tradebook_stats.to_csv(config_IO_0['path_trades']+file_csv_tb_stat_date )
        # trade plan
        file_csv_tp = trades_id+ '_TP' +'.csv'
        trades_0.tradeplan.to_csv(config_IO_0['path_trades']+file_csv_tp )

        file_csv_tp_date = trades_id+ '_TP' +'_'+str_date +'.csv'
        trades_0.tradeplan.to_csv(config_IO_0['path_trades']+file_csv_tp_date )
        # trade analyzing data/report 
        file_csv_ta = trades_id+ '_TA' +'.csv'
        trades_0.trade_ana.to_csv(config_IO_0['path_trades']+file_csv_ta )

        file_csv_ta_date = trades_id+ '_TA'  +'_'+str_date+'.csv'
        trades_0.trade_ana.to_csv(config_IO_0['path_trades']+file_csv_ta_date )

        print("Trades has been updated ")

        # Output signals into output file
        file_json = signals_0.signals_head["signals_id"] +'.json'
        with open( config_IO_0['path_signals']+ file_json ,'w') as f:
            json.dump( signals_0.signals_head ,f) 

        file_json_date = signals_0.signals_head["signals_id"]+'_'+str_date +'.json'
        with open( config_IO_0['path_signals']+ file_json_date ,'w') as f:
            json.dump( signals_0.signals_head ,f) 

        file_csv =  signals_0.signals_head["signals_id"] +'.csv'
        signals_0.signals_df.to_csv(config_IO_0['path_signals']+file_csv )

        file_csv_date =  signals_0.signals_head["signals_id"]+'_'+str_date  +'.csv'
        signals_0.signals_df.to_csv(config_IO_0['path_signals']+file_csv_date ) 

        return portfolio_suites 













