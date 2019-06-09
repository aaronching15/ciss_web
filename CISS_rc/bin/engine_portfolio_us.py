# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function: realize engine(loop) function for portfolio simulation 
功能：实现模拟组合的循环测试和验证
last update | since 181226

Menu :
port组合分析应该包括 实盘监控，盈亏分析汇总，阶段性汇报统计，仓位变动，持仓股票分析等数据。
相关记录写在port_head中：
    steps：|参考：rC_Portfolio_17Q1.py
    1，根据输入的时间周期，空间要求，导入组合信息，更新日数据模块

    2，根据port_admin 模块，导入
 
    1，导入port相关数据，确定需要计算的时间周期。
    2，port_head记录，导入AS,Asum,sp,trade，signal,signal_nextday等数据    
    3，导入(交易)参数? line 3419    
    4,日初更新AS，Asum，...
    5,导入真实数据，Tran_Live2Standard，得到交易模块 ||
    6，获取指数，宏观择时模块 line 3485，
    7，line3524，获取价格和回报率数据，更新Account_Sum, Account_Stocks, StockPool
    8，更新持仓股在stockpool或者port_df里的数据变量。
    9,非持仓股的分析，信号和交易    
    9，预判明日数据ana,signals
    10，port_head 中更新log文件

notes:
    1,美股和港股的行情在基于A股交易日时，很多是空的数据，需要小心 
    
derived from rC_Portfolio_17Q1.py
===============================================
'''
import sys
import pandas as pd 
sys.path.append("..")

class Engine_ports_us():
    #####################################################
    def __init__(self ):
        self.name = "engine of portfolio"


    def test_abm_1port_Nperiods_ind_x(self,df_w_allo,ind_level,sty_v_g,date_periods,int_ind_x,port_name,init_cash ) :
        '''   
        last 181115 | since 181115 
        derived from  def test_abm_1port_1period
        '''
        #######################################################################
        import datetime as dt
        if_period_1st = 0 
        for temp_i in range( len(date_periods.periods_start) ) :
            date_reference_change= date_periods.periods_reference_change[temp_i]
            date_start = date_periods.periods_start[temp_i]
            date_end =  date_periods.periods_end[temp_i]
            # datetime to string 
            date_reference_change=  dt.datetime.strftime( date_periods.periods_reference_change[temp_i] ,"%Y-%m-%d" )
            date_start = dt.datetime.strftime( date_periods.periods_start[temp_i],"%Y-%m-%d" )
            date_end =  dt.datetime.strftime( date_periods.periods_end[temp_i],"%Y-%m-%d" )
            print("date_reference_change ",date_reference_change)
            print("date_start ",date_start )
            print("date_end ",date_end   )  
            # suit only for one period
            if_period_1st = temp_i # 0 means the first period
            print("if_period_1st ",if_period_1st)

            if if_period_1st == 0 :
                # The first period that we need to generate a new port folio
                # we need port_name to genarate portfolio head 
                (portfolio_manage,portfolio_suites) = self.gen_abm_1port_1period_ind_x(df_w_allo,ind_level,sty_v_g,port_name,int_ind_x,date_start,date_end,date_reference_change,init_cash ) 
            else :
                # port_name = portfolio_manage.portfolio_head["portfolio_name"] || port_id = portfolio_manage.portfolio_head["portfolio_id"] 
                (portfolio_manage,portfolio_suites) = self.update_abm_1port_1period_ind_x(df_w_allo,ind_level,sty_v_g,portfolio_manage,int_ind_x,date_start,date_end,date_reference_change,init_cash) 

        return portfolio_manage,portfolio_suites

    def gen_abm_1port_1period_ind_x(self,df_w_allo,ind_level,sty_v_g,port_name,int_ind_x,date_start,date_end,date_reference_change,init_cash) :
        '''
        For the first time,genarate and calculate the whole portfolio suites for ABM model.
        ind_level='1'
        Warning : 
        1,only accept period within which no stockpool/fundamental change.
        2,Cannot be used to calculate some portfolio suites that already exists.

        INPUT: 
            date_start,date_end: start and end date for the period
            date_reference_change:the date used to get fundamental(csi index adjusting date)
        ALGO:  
        OUTPUT: 

        run ABM model for 1 portfolio(i.e. int_ind3='401010') and 1 period
        last 181115 | since 181115 
        derived from test_abm.py,test_abm_1port_Nperiods,test_abm_1port_1period,
            def test_abm_1port_1period
        ''' 
        ##################################################################
        # if_rebalance means we are rebalance portfolio periodically
        if_rebalance = 0 
        ##################################################################
        ### Import requirement outside modules
        sys.path.append("..") 
        import json
        import datetime as dt 

        ### Import personal model engine 
        from bin.abm_engine import Abm_model
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

        # import time module 
        from db.times import times
        times0 = times('CN','SSE')
        method4time='stock_index_csi'
        ##################################################################
        ### Initialize common configurations and variables
 

        if not ind_level == '0' :
            # Working on specific industry level: ind_level == '1' "2" "3"
            path0 = "D:\\CISS_db\\abm_weights\\"
            file_name = "weights_"+date_reference_change.replace("-","") +"_"+str(ind_level) +".csv"
            temp_list = pd.read_csv(path0 + file_name)
            temp_list = temp_list.drop(["Unnamed: 0"],axis=1)
            # int to str
            col_name_ind = 'ind'+ ind_level + '_code' # 'ind1_code'
            temp_list[col_name_ind]=temp_list[col_name_ind].astype(str)
            ##################################################################
            print("temp_list has been loaded.")
            # print( temp_list.info())

            print("Working on industry :", int_ind_x )
            
            
            ### Get allocation weights for industry level 3 :
            ### Value allocation in industry level 3 
            # Notes:  temp_list["ind1_code"] is int in method1 and string in method2
            if not int_ind_x == "999" :
                temp_df_value = temp_list[ temp_list[col_name_ind] ==  int_ind_x  ]
            else :
                temp_df_value = temp_list
            # temp_df_value["ind1_pct_profit_q4_es"].sum() # 1 
            print('VALUE:weight allocation for industry: temp_df_value')
            print( "temp_df_value" )
            print( temp_df_value.info() )
            print( temp_df_value.head() )
            print(temp_df_value["w_allo_value_ind"+ind_level].sum() )
            print(temp_df_value["w_allo_value_ind"+ind_level] ) 

            ### Growth allocation in industry level 3 
            temp_df_growth =temp_df_value
            # temp_df_growth["ind3_pct_profit_q4_es"].sum() # 1 
            print('GROWTH:weight allocation for industry: ')
            print(temp_df_growth["w_allo_growth_ind"+ind_level].sum() )
            print(temp_df_growth["w_allo_growth_ind"+ind_level] ) 
            # equivalent to "para_value" , whcih using 1 for anchor stock, still need to be devided by sum of columns

            ### save result to temp file directory  
            # 若 apps和abm两级文件夹都要新建，则不用mkdir，用makedirs
            temp_path = "D:\\CISS_db\\temp\\"
            import os
            if not os.path.isdir( temp_path) :
                os.makedirs(temp_path)  
            # temp_df_growth.to_csv(temp_path + "temp_df.csv"  )
            temp_df_value.to_csv(temp_path + "temp_df_value_"+ str(int_ind_x)+"_"+ date_reference_change +".csv"  )
            temp_df_growth.to_csv(temp_path + "temp_df_growth_"+ str(int_ind_x)+"_"+ date_reference_change +".csv"  )
        else :
            ##############################################################################
            ### working on whole symbol universe here when ind_level == '0'
            # using data from df_w_allo and date_start 
            df_w_allo2 =df_w_allo.T 
            df_w_allo2["code"] =  df_w_allo2.index
            # df_w_allo2["code"]=df_w_allo2["code"].apply(lambda x: x.strftime("%Y-%m-%d"))
            import datetime as dt 
            date_start2 = dt.datetime.strptime(date_start,"%Y-%m-%d")
            temp_df_0 = df_w_allo2.loc[:,[date_start2, "code"] ] 
            
            temp_df_value = temp_df_0 
            temp_df_value["w_allo_value_ind1"] =   temp_df_0[date_start2] 
            # temp_df_growth["w_allo_growth_ind1"] = temp_df_growth["profit_q4_es_dif"]/df_profit_dif_posi["profit_q4_es_dif"].sum()
            ### df_w_allo already get rid of negative values and make sum of w_allo_ to 1 
            # print("temp_df_value")
            # print( temp_df_value )

        ##############################################################################
        ### Portfolio simulation using CISS standarded modules.
        #  example ,confiuration file and portfolio data 
        ##############################################################################
        ### Initialize portfolio example  
        '''
        INPUT: config_port ,port_name
        ALGO: 
        OUTPUT: portfolio_0 
        '''
        ## temp_date 后第一个交易日T开始建仓，初始资金{1,5,10,50,100,500}亿元
        # port_name=  name_id
        # name_id='sys_rc001' if it is a system with multiple portfolios 
        # name_id='tree_rc001' if it is a tree structure with multiple systems 
        config_port={} 
        portfolio_gen = gen_portfolios(config_port ,port_name )
        port_id = portfolio_gen.port_id
        port_head = portfolio_gen.port_head
        port_df = portfolio_gen.port_df

        print("Portfolio has been generated. ")
        print( portfolio_gen.port_head['portfolio_name'] )

        ##############################################################################
        ### Portfolio configurations
        # date_start= temp_date,  date_end=temp_date2
        date_start = date_start.replace("-","") # 20140531" 
        date_end = date_end.replace("-","")  # "20141130"
        # port_name=  name_id
        config_apps = config_apps_abm(init_cash,date_start,date_end,port_name ) 

        
        ### generate portfolio_suites object with  AS,Asum,trades,signal
        '''
        INPUT: config_apps,temp_df_growth,sp_name0,port_name
        ALGO: gen_port_suites 
        OUTPUT: stockpool_0,account_0,trades_0, signals_0 contents
        '''
        sp_name0=  str(int_ind_x)  
        portfolio_suites = portfolio_gen.gen_port_suites(port_head,config_apps,temp_df_value,sp_name0,port_name)
        print('portfolio_suites has been generated.')
        print("trade=========================================")

        ### use portfolio ID to load portfolio which just be generated  
        port_id = portfolio_gen.port_head["portfolio_id"]   # id_port_1541729640_rc001_401010
        port_name=  portfolio_gen.port_head["portfolio_name"] #'port_rc001' 

        ### load configuration of portfolio 
        config= config_IO('').load_config_IO_port(port_id,path_base,port_name) 
        config_port = config
        ### save abm-model analytical data to aaps file directory of portfolio 
        import os
        if not os.path.isdir( config_port['path_apps'] ) :
            os.makedirs( config_port['path_apps'] )  
        # temp_df_growth.to_csv(temp_path + "temp_df.csv"  )
        temp_df_value.to_csv(config_port['path_apps']  + "temp_df_value_"+ str(int_ind_x)+"_"+ date_reference_change +".csv"  )
        # temp_df_value.to_csv(config_port['path_apps']  + "temp_df_growth_"+ str(int_ind_x)+"_"+ date_reference_change +".csv"  )

        path_base = config['path_base']
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

        ### Method 2: Load quote data from existing quotation directory 
        path0 = 'D:\\db_wind\\quotes_us\\'
        data_wind_0 = data_wind('' ,path0 )
        quote_type='US_day'

        ############################################################################## 
        ### Run strategy for allocation weights 
        # Straetgy could be a roughly estimation of how many stock to trade
        # 策略是粗线条的，只说我们要买入多少比例的股票
        stra_weight_list = stra_allocation('').stock_weights(ind_level,sty_v_g,stockpool_df)
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

        ##############################################################################
        ### Trade management 
        ### Generate trade plan 
        ## when and which amrket to trade, price or volumne zone for setting trade plan 
        # load trade head file 
        manager_trades = manage_trades('')
        # sty_v_g,sty_v_g is used to judge value , growth or other styles. sty_v_g='value'
        if_rebalance =0 # 0 for the generate period and 1 for the update period 
        portfolio_suites = manager_trades.manage_tradeplan(if_rebalance,ind_level,sty_v_g,portfolio_suites, signals_list, config_IO_0 ,date_start,date_end,quote_type,data_wind_0 )
        print('trade_plan')
        print( portfolio_suites.trades.tradeplan )

        #### get trade details 
        portfolio_suites= manager_trades.manage_tradebook(portfolio_suites, config_IO_0 ,date_start,date_end,quote_type,data_wind_0 )
        print( 'trades.tradebook' )
        print( portfolio_suites.trades.tradebook )

        ##############################################################################
        ### Portfolio management 
        ### update trades in portfolio_suites, done in previous manage_tradeplan or tradebook

        ### update accounts using trade result
        ## we only update trades that have not been used by accounts 
        from db.accounts import manage_accounts

        ###  get trading days using account_sum and date_start,date_end 
        date_list = portfolio_suites.account.account_sum.index
        # date_list <class 'pandas.core.indexes.datetimes.DatetimeIndex'>
        # '2017-06-12', '2017-06-13',
        print('date_list', type( date_list ))
        # print( date_list )
        # pd.DataFrame(index=date_list).to_csv("D:\\date_list.csv")

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
            print("temp_date  ", type(temp_date) , temp_date  )

            if_trade =0
            if temp_date in date_list_trades :
                # date with trading 
                print("date with trading ")
                portfolio_suites = manage_accounts('').update_accounts_with_trades(temp_date,portfolio_suites,config_IO_0,date_start,date_end,quote_type,data_wind_0)
                if_trade =1 
            
            # update closing price for all holding stocks, whether date with no trading or not, 
            portfolio_suites = manage_accounts('').update_accounts_with_quotes(temp_date,portfolio_suites,config_IO_0,date_start,date_end,quote_type,data_wind_0,if_trade )
            ## we should update statistics results of portfolio_head file before output 
            # todo todo 

            ## for every trading day, Out portfolio_suites to files
            # temp_date  2014-06-03 00:00:00 type is time stamp
            # transfer datetime to string 
            temp_date2 = dt.datetime.strftime(temp_date,"%Y%m%d")
            print( "temp_date ",type(temp_date2),temp_date2 )
            portfolio_manage.port_head["date_LastUpdate"] = temp_date2
            portfolio_suites = portfolio_manage.output_port_suites(temp_date2,portfolio_suites,config_IO_0,port_head,port_df)
            
        return  portfolio_manage,portfolio_suites 


    def update_abm_1port_1period_ind_x(self,df_w_allo,ind_level,sty_v_g,portfolio_manage,int_ind_x,date_start,date_end,date_reference_change,init_cash) :
        '''
        ind_level="1"
        For some existed portfolio,
        Genarate and calculate the whole portfolio suites for ABM model.

        Warning : 
        1,only accept period within which no stockpool/fundamental change.
        2,Cannot be used to calculate some portfolio suites have not been generated.

        INPUT: 
            date_start,date_end: start and end date for the period
            date_reference_change:the date used to get fundamental(csi index adjusting date)
        ALGO:  
        OUTPUT: 

        run ABM model for 1 portfolio(i.e. int_ind3='401010') and 1 period
        
        update portfolio suites 
        last 181115 | since 181115 
        derived from gen_abm_1port_1period
        '''
        ##################################################################
        # if_rebalance means we are rebalance portfolio periodically
        if_rebalance = 1 
        ### ### get portfolio object using input:portfolio_manage
        port_name = portfolio_manage.port_head["portfolio_name"] 
        port_id = portfolio_manage.port_head["portfolio_id"] 

        ### Import requirement outside modules
        sys.path.append("..") 
        import json

        ### Import personal model engine 
        # from bin.abm_engine import Abm_model
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

        # import time module 
        from db.times import times
        times0 = times('CN','SSE')
        method4time='stock_index_csi'

        ##############################################################################
        ### Application ABM=model
        ##############################################################################
        ### Initialize ABM model engine 
        ### step 1 获取原始研究数据,data_in 经过研究员初步梳理后的结构化信息 
        # 7个indicator从属于 核心因子:[行业，流动性，动量，主动收益，价值，成长，
        # 资本结构，财务优势，经营能力，人力优势，信息优势] 
        # Merge additional data into one pd： create/import database file,update data 
        # with imported new info, save to database file.

        # # abm_model = Abm_model()
        # # dataG = abm_model.load_symbol_universe('','' )
        # dataG = self.load_symbol_universe('','' )
        # ### Get historical fundamental financial and capital data
        # [df_tb_fi_fi, df_tb_fi_cap] =self.get_histData_finance_capital()

        ##################################################################
        ### Calculate analyzing indicators for given index conponents and time periods
        '''
        INPUT:dataG
        ALGO:
        OUTPUT: temp_list
        '''

        ##################################################################     
        ### Method 1 get symbol list for 1 index 
        # # temp_list = abm_model.get_symbol_list( dataG, '000300.SH','2014-05-31')
        # # temp_list = abm_model.get_symbol_list( dataG, '000300.SH',temp_date)
        # # get symbol list for all indexes

        # ### Get symbol list for all indexes 
        # # originally, we simply set start date equals to adjusting date of index,

        # temp_list = abm_model.get_all_list( dataG, date_reference_change )

        # print('==========================')
        # print('temp_list, length ', len(temp_list['code']) )
        # print(temp_list.info() ) 

        # ### Calculate financial estimates for current period
        # # (temp_list,temp_date,df_tb_fi_fi)
        # [temp_list,cols_new_es] = abm_model.calc_financial_estimates(temp_list,date_reference_change,df_tb_fi_fi)

        # ### calculate weights of asset allocation for industry hierachy from level1 to level3 
        # [temp_list,cols_new_w_allo] = abm_model.calc_weight_allo_ind_hierachy(temp_list)

        # ### calculate for anchor stocks in value and growth perspective
        # [temp_list,cols_new_anchor] = abm_model.calc_anchor_stocks(temp_list,ind_level)

        # ### calculate shadow enterprise value from anchor stocks in value and growth perspectives
        # [temp_list,cols_new_shadow] = abm_model.calc_shadow_ev_from_anchor(temp_list, ind_level) 

        # ##################################################################
        # ### Method 2  get symbol list for 1 index 
        # path0 = "D:\\CISS_db\\abm_weights\\"
        # file_name = "weights_"+date_reference_change.replace("-","") +"_"+str(ind_level) +".csv"
        # temp_list = pd.read_csv(path0 + file_name)
        # temp_list = temp_list.drop(["Unnamed: 0"],axis=1)
        path0 = "D:\\CISS_db\\abm_weights\\"
        if ind_level == "0" :
            ind_level = "1" 

        file_name = "weights_"+date_reference_change.replace("-","") +"_"+str(ind_level) +".csv"
        temp_list = pd.read_csv(path0 + file_name)
        temp_list = temp_list.drop(["Unnamed: 0"],axis=1)
        # int to str
        col_name_ind = 'ind'+ ind_level + '_code' # 'ind1_code'
        temp_list[col_name_ind]=temp_list[col_name_ind].astype(str)
        ##################################################################
        print("temp_list has been loaded.")
        # print( temp_list.info())


        ##################################################################
        ### get annalytical dateframe for specific industry     
        '''
        INPUT: temp_list,int_ind3
        ALGO:
        OUTPUT: temp_df_growth,temp_df_value
        '''
        print("Working on industry 3 :", int_ind_x )
        col_name_ind = 'ind'+ ind_level + '_code' # 'ind1_code'

        ### Get allocation weights for industry level 3 :
        ### Value allocation in industry level 3 
        if not int_ind_x == "999" :
            temp_df_value = temp_list[ temp_list[col_name_ind ] == int_ind_x  ]
        else :
            ##############################################################################
            ### working on whole symbol universe here when ind_level == '0'
            # using data from df_w_allo and date_start 
            df_w_allo2 =df_w_allo.T
            df_w_allo2["code"] = df_w_allo2.index

            import datetime as dt 
            date_start2 = dt.datetime.strptime(date_start,"%Y-%m-%d")
            temp_df_0 = df_w_allo2.loc[:,[date_start2, "code"] ] 
            
            temp_df_value = temp_df_0 
            temp_df_value["w_allo_value_ind1"] =   temp_df_0[date_start2]

            # temp_df_0 = df_w_allo2.loc[:,[date_start, "code"] ] 
            # # print( "temp_df_0" )
            # # print( temp_df_0)
            # temp_df_value = temp_df_0 
            # temp_df_value["w_allo_value_ind1"] =   temp_df_0[date_start] 


        # temp_df_value["ind3_pct_profit_q4_es"].sum() # 1 
        print('VALUE:weight allocation for industry: temp_df_value')
        print(temp_df_value["w_allo_value_ind"+ind_level].sum() )
        print(temp_df_value["w_allo_value_ind"+ind_level] ) 

        ### Growth allocation in industry level 3 
        
        # equivalent to "para_value" , whcih using 1 for anchor stock, still need to be devided by sum of columns

        ### save result to temp file directory  
        # 若 apps和abm两级文件夹都要新建，则不用mkdir，用makedirs
        temp_path = "D:\\CISS_db\\temp\\"
        import os
        if not os.path.isdir( temp_path) :
            os.makedirs(temp_path)  
        # temp_df_growth.to_csv(temp_path + "temp_df.csv"  )
        temp_df_value.to_csv(temp_path + "temp_df_value_"+ str(int_ind_x)+"_"+ date_reference_change +".csv"  )
        # temp_df_growth.to_csv(temp_path + "temp_df_growth_"+ str(int_ind_x)+"_"+ date_reference_change +".csv"  )

        ##############################################################################
        ### Portfolio simulation using CISS standarded modules.
        #  example ,confiuration file and portfolio data 
        ##############################################################################
        ##############################################################################
        ### Load portfolio example  
        '''
        INPUT: config_port ,port_name 
        ALGO: 
        OUTPUT: portfolio_gen 
        ''' 
        print("Portfolio to be loaded. ")
         
        ##############################################################################
        ### Portfolio configurations
        # date_start= temp_date,  date_end=temp_date2
        date_start = date_start.replace("-","") # 20140531" 
        date_end = date_end.replace("-","")  # "20141130"
        # port_name=  name_id
        config_apps = config_apps_abm(init_cash,date_start,date_end,port_name )

        ### generate portfolio_suites object with  AS,Asum,trades,signal
        '''
        INPUT: config_apps,temp_df_growth,sp_name0,port_name
        ALGO: gen_port_suites
        OUTPUT: stockpool_0,account_0,trades_0, signals_0 contents
        '''
        sp_name0=  str(int_ind_x)  

        ##############################################################################
        ### Load latest portfolio suites 
        # 若temp_date=20141130为周末，非交易日，则需要选定一个外部文件里最接近的交易日,如 20141127
        date_LastUpdate = portfolio_manage.port_head["date_LastUpdate"]  
        print("date_LastUpdate ",date_LastUpdate)

        port_head =portfolio_manage.port_head
        port_id = portfolio_manage.port_head["portfolio_id"]   # id_port_1541729640_rc001_401010
        port_name=  portfolio_manage.port_head["portfolio_name"] #'port_rc001'         
        port_df  =portfolio_manage.port_df
        config_IO_0 = portfolio_manage.config_IO_0  

        portfolio_suites = portfolio_manage.load_portfolio_suites(date_LastUpdate,config_IO_0,port_head,port_df,port_name,sp_name0)
        # if temp_date == "20141201" :
        
        id_time_stamp = portfolio_manage.port_head["portfolio_id_time"]
        sp_head = portfolio_suites.stockpool.sp_head
        
        ##############################################################################
        ### Crutial: assign new stockpool df to portfolio suites
        sp_df =  temp_df_value # a new one 
        portfolio_suites.stockpool.sp_df = sp_df

        ##############################################################################
        ### load configuration of portfolio 
        config= config_IO('').load_config_IO_port(port_id,path_base,port_name) 
        config_port = config
        ### save abm-model analytical data to aaps file directory of portfolio 
     

        ############################################################################
        ### From here, it is the same to contents in gen_abm_1port_1period
        ############################################################################
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
        path0 = 'D:\\db_wind\\quotes_us\\'
        data_wind_0 = data_wind('' ,path0 )
        quote_type='US_day'

        ############################################################################## 
        ### Run strategy for allocation weights 
        # Straetgy could be a roughly estimation of how many stock to trade
        # 策略是粗线条的，只说我们要买入多少比例的股票
        stra_weight_list = stra_allocation('').stock_weights(ind_level,sty_v_g, stockpool_df)
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
        print('signals_list')
        print(signals_list )
        ##############################################################################
        ### Trade management 
        ### Generate trade plan 
        ## when and which amrket to trade, price or volumne zone for setting trade plan 
        # load trade head file 
        manager_trades = manage_trades('')
        if_rebalance = 1 # 0 for the generate period and 1 for the update period 
        portfolio_suites = manager_trades.manage_tradeplan(if_rebalance,ind_level,sty_v_g,portfolio_suites, signals_list, config_IO_0 ,date_start,date_end,quote_type,data_wind_0 )

        # print('trade_plan')
        # print( portfolio_suites.trades.tradeplan.tail() )        
        #### get trade details 
        portfolio_suites= manager_trades.manage_tradebook(portfolio_suites,config_IO_0 ,date_start,date_end,quote_type,data_wind_0 )
        # print( 'trades.tradebook' )
        # print( portfolio_suites.trades.tradebook.tail() )

        ##############################################################################
        ### Portfolio management 
        ### update trades in portfolio_suites, already done

        ### update accounts using trade result
        ## we only update trades that have not been used by accounts
        from db.accounts import manage_accounts
        import datetime as dt
        ###  get trading days using account_sum and date_start,date_end 
        
        asum_list = portfolio_suites.account.account_sum
        asum_list['datetime'] = pd.to_datetime( asum_list['date'] )
        # print('asum_list',type( asum_list['datetime'] ) ) 
        
        # print(asum_list[asum_list<date_end ] )
        #  2014-06-03 to 2014-11-28
        # print("Debug: ",date_start,date_end) 
        asum_list_units = asum_list[ asum_list['datetime'] <=  dt.datetime.strptime(date_end,'%Y%m%d')  ]
        asum_list_units = asum_list_units[ asum_list_units['datetime'] >=  dt.datetime.strptime(date_start,'%Y%m%d')  ]
        asum_list_dates = list( asum_list_units['date'].drop_duplicates()  )
        # asum_list_dates <class 'pandas._libs.tslib.Timestamp'>
        # print('asum_list_dates',type(asum_list_dates[0]) )
        # print(  dt.datetime.strftime(asum_list_dates[0],"%Y-%m-%d") ) 

        trades_0 = portfolio_suites.trades
        tradebook = portfolio_suites.trades.tradebook

        ### get all trading dates from tradebook
        tradebook['datetime'] = pd.to_datetime(tradebook['date'], format='%Y-%m-%d' ) 
        
        date_start2 = dt.datetime.strptime(date_start, "%Y%m%d" )
        # print("dates ",date_start, date_start2)

        tradebook2 = tradebook[ tradebook['datetime'] >= date_start2  ] 
        # date_list_trades = list( tradebook2['date'].drop_duplicates() ) # format is 2014-12-01

        date_list_trades = list( pd.to_datetime(tradebook2['date']).drop_duplicates() ) # format is 2014-12-01
        # sample:['2014-12-01', '2014-12-02', '2014-12-03', '2014-12-04', '2014-12-05']
        # print("date_list_trades")
        # print(date_list_trades)
        
        # asum_list_dates used to be date_list_units
        for temp_date0 in asum_list_dates  :
            temp_date = dt.datetime.strftime( temp_date0 ,"%Y-%m-%d") # 2014-12-01
            print("We are updating trading date ", temp_date)
            if_trade =0
            if temp_date in date_list_trades :
                # date with trading 
                portfolio_suites = manage_accounts('').update_accounts_with_trades(temp_date,portfolio_suites,config_IO_0,date_start,date_end,quote_type,data_wind_0)
                if_trade =1 
            
            # update closing price for all holding stocks, whether date with no trading or not, 
            portfolio_suites = manage_accounts('').update_accounts_with_quotes(temp_date,portfolio_suites,config_IO_0,date_start,date_end,quote_type,data_wind_0,if_trade )
            ## we should update statistics results of portfolio_head file before output 
            # todo todo 

            ## for every trading day, Out portfolio_suites to files
            # transfer datetime to string 

            portfolio_suites = portfolio_manage.output_port_suites(temp_date,portfolio_suites,config_IO_0,port_head,port_df)
            # print("Dbuging=====================")
            # print( portfolio_suites.account.account_sum.head() )
            
            # if temp_date == "2012-12-01" :
            print("Debuging=====================")
            account1 = portfolio_suites.account
            # print( account1.account_sum[account1.account_sum['unit']>0 ] )
            # print("Debuging=====================")
            print("portfolio_suites.trades.tradebook" )
            print( portfolio_suites.trades.tradebook.tail() )
            


        return  portfolio_manage,portfolio_suites 













































