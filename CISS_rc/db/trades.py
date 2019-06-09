# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：
last update 181104 | since  181104
Menu :
THREE COMPONENTS:
1,class A:
    {INPUT,ALGO,OUTPUT  }
1,function a:
    {INPUT,ALGO,OUTPUT  }

分析：
1，交易管理管理
    1.1，分账户交易历史明细数据
    1.2，分账户交易统计
    1.3，交易汇总统计
    1.4，交易计划{若多策略对应流动性过多，则团队内部可以评估策略汇总情况下对市场的流动性冲击}
    1.5，交易计算

2，配置文件 | config\config_trades.py
 
4,output file 
    1,head json file of trades
    2,  dataframe of  
    3,  dataframe of 

Notes: 
refernce: rC_Portfolio_17Q1.py 
===============================================
'''
import sys
sys.path.append("..")
import pandas as pd
###################################################
class trades():
    def __init__(self,trades_name=''):
        self.trades_name = trades_name

###################################################
class gen_trades():
    def __init__(self,id_time_stamp,trades_name=''):

        
        self.tradebook_head  = self.gen_tradebook_head(id_time_stamp,trades_name)
        self.tradebook  = self.gen_tradebook()
        self.tradebook_stats = self.gen_tradebook_stats()
        self.tradeplan  = self.gen_tradeplan() 
        self.trade_ana  = self.gen_trade_analyze() 

    def gen_tradebook_head(self,id_time_stamp,trades_name) :
        # generate head of trade infomation
        tradebook_head = {}

        if trades_name =='':
            ## get stockpool id using  time stamp 
            sys.path.append("..")
            # from db.basics import time_admin
            # time_admin1 = time_admin()
            # time_stamp = time_admin1.get_time_stamp()

            tradebook_head['trades_name'] = "trades_name_"+  id_time_stamp
            tradebook_head['trades_id'] = "trades_id_"+  id_time_stamp
            tradebook_head['trades_id_time'] = id_time_stamp 
        else :
            tradebook_head['trades_name'] = "trades_name_"+  trades_name
            tradebook_head['trades_id'] = "trades_id_" + id_time_stamp +"_"+ trades_name
            tradebook_head['trades_id_time'] = id_time_stamp 

        # other useful information 
        # tradebook_head['']


        return tradebook_head

    def gen_tradebook(self) :
        # order records of a trading book | Buy Sell
        # ave_price means the average price only in last trade action
        # 1 order might have several or many detaled entrusts
        columns_trade = ['market','currency', 'date', 'symbol', 'BSH', 'ave_price', 'number', 'ave_cost', 'fees', 'profit_real', 'open', 'close']
        tradebook = pd.DataFrame(  columns=columns_trade)


        return tradebook
    def gen_tradebook_stats(self) :
        # statistics of a trading book
        # key 对于债券，期权期货，衍生品等需要分大类；对于股票分行业，区域。风格
        # 账户类型！！！
        columns_trade_stats = ['num_profit']
        tradebook_stats = pd.DataFrame(  columns=columns_trade_stats)

        return tradebook_stats

    def gen_tradeplan(self) :
        # trading plan
        # 'duration' : times that trade entrust can be executed 
        columns_tradeplan = ['duration']
        tradeplan = pd.DataFrame(  columns=columns_tradeplan)

        return tradeplan
    def gen_trade_analyze(self) :
        # trading calculation with technical methods 
        columns_analyze = ['num_ave','price_ave']
        trade_ana = pd.DataFrame(  columns=columns_analyze)


        return trade_ana

    def gen_tradebook_detail(self) :
        # detalied entrust records of a trading book | Buy Sell
        # 1 order might have several or many detaled entrusts
        columns_trade = ['market','currency', 'date', 'symbol', 'BSH', 'ave_price', 'number', 'ave_cost', 'fees', 'profit_real']
        tradebook_detail = pd.DataFrame(  columns=columns_trade)


        return tradebook_detail


class manage_trades():
    '''
    Manage and update trade objects 
    1, build trade plan according to signals
    2, exucute trade orders with trade plan and market quotes
    3, 
    '''
    def __init__(self,trades_name=''):
        # load trade objects 
        # load trades_id_port_rc001 
        self.trades_name='' 
    
    def load_trades(self,trade_id='',path_base= '',trades_name='') :
        '''
        using portfolio_suites to load trades info ,
        or can use this module to load trades specifically.
        todo

        ''' 

        return 1


    def manage_tradeplan(self,if_rebalance,ind_level,sty_v_g,portfolio_suites,signals_df,config_IO_0,date_start,date_end,quote_type,data_wind):
        '''
        there are 4 cases: 
        1, buy with no previous positions
        2, buy with some positions
        3, sell for some positions 
        4, sell for all positions 
        last update 181123
        
        var:
        if_rebalance means we need to sell all previous positions if code is not in latest target weight from signals
        '''

        # sty_v_g is used to judge value , growth or other styles. sty_v_g='value'
        # making trade plan using signals 
        #################################################################
        ### modify ind_level if we want to calculate the whole market 
        if ind_level == "0"  :
            ind_level2 = "1"
        else :
            ind_level2 = ind_level

        ###
        trades_1 = portfolio_suites.trades
        account_1 =  portfolio_suites.account
        # notes: date_start might not be a trading date !!!
        import datetime as dt
        import numpy as np
        ################################################################################
        ### trading para
        fees = 0.0025
        # number of shares per lot for all Ashares but not for all HK stocks
        num_per_lot =100 
        ################################################################################
        ### Cash budget | make non negative cash balance for current trade period
        # 导入账户现金余额，计算交易计划对应的现金头寸
        # 
        dt_start = dt.datetime.strptime(date_start,"%Y%m%d")
        dt_start_str = dt.datetime.strftime(dt_start,"%Y-%m-%d")
        print("head")
        print( account_1.account_sum.head() )
        temp_asum= account_1.account_sum[ account_1.account_sum['date'] < dt_start_str ]
        ################################################################################
        ### trade stats part {cash, number of trade, ...}  todo
        ### setting minimum cash level of account 
        if len( temp_asum.index ) <1 :
            # firt index of A_sum is 2014-06-03, but date_start might be 2014-05-31
            temp_index2 = account_1.account_sum[ account_1.account_sum['date'] >= dt_start_str ].index[0]
            cash_balance = account_1.account_sum.loc[temp_index2, "cash"]    
            cash_balance_min = account_1.account_sum.loc[temp_index2, "total"] *0.01    
        else :
            cash_balance = temp_asum.loc[temp_asum.index[0], "cash"]
            cash_balance_min = temp_asum.loc[temp_asum.index[0],  "total"] *0.01   
        ### define cash_in and cash_out for calculating all buy or sell actions 
        index_list_sell = []
        cash_in = 0.0
        index_list_buy = []
        cash_out= 0.0
        print("Date",dt_start_str,"cash_balance ", cash_balance)
        
        ################################################################################
        ### Sell all positions that is not in latest signal list  
        for as_index in account_1.account_stock.index :
            temp_code = account_1.account_stock.loc[as_index,'code']
            print("Working on code ", temp_code , date_start )
            # check if code is in signal list 
            df_sig = signals_df[  signals_df['code'] == temp_code]
            # print(df_sig )
            if len(df_sig.index ) <1 :
                ### we want to sell all of  this positions 
                weight_dif = account_1.account_stock.loc[as_index,'w_real']*-1
                ### load quote date
                (code_head,code_df)=data_wind.load_quotes(config_IO_0,temp_code,date_start,date_end,quote_type)
                ################################################################################
                ### Filter: we want only trading day that has volume > 0 
                code_df = code_df[ code_df["volume"]>0 ]


                dt_0 = dt.datetime.strptime(date_start,"%Y%m%d")
                code_df['datetime'] = pd.to_datetime( code_df['date'],format="%Y-%m-%d",errors="ignore")
                ################################################################################
                ### code_df2 is time series of stock quotes that satisfy volume and date conditions 
                code_df2=code_df[ code_df['datetime']>=dt_0 ]

                date_index_1 = code_df2.index[0]
                date_index_5 = code_df2.index[4]
                # print( date_index_1,date_index_5 )
                quote_df = code_df2.loc[date_index_1:date_index_5 ,: ]
                # print( "quote_df ", quote_df   )

                # extend tradeplan, we assume 
                # print( trades_1.tradeplan)
                tp_index = trades_1.tradeplan.index.max() +1
                trades_1.tradeplan.loc[tp_index ,'code'] = temp_code
                
                trades_1.tradebook_head['date_last_trade'] =  code_df2.loc[date_index_5 ,'date' ]

                trades_1.tradeplan.loc[tp_index,'date_plan'] = dt_0 
                trades_1.tradeplan.loc[tp_index,'date_trade_1st'] = code_df2.loc[date_index_1,'datetime']
                # 根据资金量确定交易金额和数量等。
                # get initial capital or latest cash| value
                # Assume 5 days to buy enough positions,and no change since then
                # half open half close; later we want to get average of 1min frequency
                trades_1.tradeplan.loc[tp_index,'method'] = "open_close" 
                trades_1.tradeplan.loc[tp_index,'period'] = "5D"
                trades_1.tradeplan.loc[tp_index,'quote_index_start'] = date_index_1
                trades_1.tradeplan.loc[tp_index,'quote_index_end'] = date_index_5
                trades_1.tradeplan.loc[tp_index, 'signal_pure'] = -1
                trades_1.tradeplan.loc[tp_index, 'weight_dif'] = weight_dif
                
                ### In case not type of datetime, transfer string to datetime.
                trades_1.tradeplan.loc[tp_index, 'total_amount'] = account_1.account_stock.loc[as_index,'market_value']
                trades_1.tradeplan.loc[tp_index, 'num'] = account_1.account_stock.loc[as_index,'num']
                trades_1.tradeplan.loc[tp_index, 'ave_price'] =trades_1.tradeplan.loc[tp_index, 'total_amount'] / account_1.account_stock.loc[as_index,'num']
                # print("account information ... ")
                # print( account_1.account_stock.loc[as_index,:]  )
                cash_balance =cash_balance + trades_1.tradeplan.loc[tp_index, 'total_amount'] 
                index_list_sell = index_list_sell + [ tp_index  ]
                cash_in =cash_in+  trades_1.tradeplan.loc[tp_index, 'total_amount'] *(1-fees)
                print("Date",dt_start_str,"cash_balance ", cash_balance)
        ################################################################################
        ### Buy or Sell all positions that is in latest signal list 
        signals_df['signal_pure']=1
        for temp_i in signals_df.index : 
            if signals_df.loc[temp_i,'signal_pure'] == 1 : 
                ### buy or open long position  trade
                temp_code = signals_df.loc[temp_i,'code']
                print( 'Working on code ', temp_code, date_start )
                name_column = "w_allo_"+sty_v_g + "_ind" +ind_level2
                weight_target = signals_df.loc[temp_i,name_column ]#'w_allo_value_ind3']
                # type of temp_asum.index is timestamp,so we need to change date_start2 into timestamp
                import datetime as dt 
                ### get target weights from signals, and real weights in current positions
                df_as = account_1.account_stock[account_1.account_stock['code'] == temp_code]

                ### Get amount to trade
                if len( df_as.index ) < 1 :
                    ################################################################################
                    ### no such stock positions 
                    ### code_head is temporarily no use in this module .
                    weight_dif = weight_target - 0
                    
                elif len( df_as.index ) >= 1 :
                    ################################################################################
                    ### with such stock positions, sell if current weight larger than target weights
                    ###若现有权重大于目标权重，则需要卖出
                    # print("df_as")
                    # print( df_as)
                    weight_real = df_as.loc[ df_as.index[0],'w_real']
                    weight_dif = weight_target - weight_real

                account_1.account_sum['date'] = pd.to_datetime( account_1.account_sum['date'] ,format="%Y-%m-%d",errors="ignore") 
                ################################################################################
                ### load quote date
                (code_head,code_df)=data_wind.load_quotes(config_IO_0,temp_code,date_start,date_end,quote_type)
                
                ################################################################################
                ### Filter: we want only trading day that has volume > 0 
                code_df = code_df[ code_df["volume"]>0 ]
                # print("==========================")
                
                # case like 000024.SZ might not in ind1= "60",for delisting, we need to try... except and pass
                if len(code_df.index) > 1 :
                    ################################################################################
                    ### code_df2 is time series of stock quotes that satisfy volume and date conditions 
                    dt_0 = dt.datetime.strptime(date_start,"%Y%m%d")
                    code_df['datetime'] = pd.to_datetime( code_df['date'] ,format="%Y-%m-%d",errors="ignore")
                    code_df2=code_df[ code_df['datetime']>=dt_0 ]

                    date_index_1 = code_df2.index[0]
                    date_index_5 = code_df2.index[4]
                    date_start2 = code_df2.loc[date_index_1 ,'date' ]
                    # date_start2 = date_start2[:4]+'-' +date_start2[4:6]+'-'+date_start2[6:] 

                    ################################################################################
                    ### Account summary at Day 1 
                    temp_asum= account_1.account_sum[ account_1.account_sum['date'] == date_start2 ]
                    # print('temp_asum of account_sum' , date_start2 )
                    # print(  temp_asum  )
                    ### Account summary at previous Day 1 
                    temp_asum_pre = account_1.account_sum[ account_1.account_sum['date'] < date_start2 ]
                    
                    # date_start2 = dt.datetime.strptime(date_start2, "%Y-%m-%d" )
                    # the total value of account sum is NaN here so we need to use values in previous date
                    if len( temp_asum_pre.index ) < 1:
                        # the first trading day, use  the first index 
                        temp_index_pre = temp_asum.index[0]
                    else :
                        temp_index_pre = account_1.account_sum[ account_1.account_sum['date'] < date_start2 ].index[-1]
                    # # print("temp_index_pre ",temp_index_pre)
                    # print("Account sum in last trading day:")
                    # print( account_1.account_sum.loc[temp_index_pre,:] )

                    quote_df = code_df2.loc[date_index_1:date_index_5 ,: ]
                    # print( "quote_df ", quote_df   )
                    #  open high  low close  volume  amt turn   date=2014-11-24

                    ################################################################################
                    ### Head of tradeplan
                    ### 交易的要素：交易目标，交易限价，市场成交价，市场挂盘价 || 目标量，市场成交量，挂盘量 
                    ### tp_index is the beginning index of trade plan 
                    if len( trades_1.tradeplan.index ) <1 :
                        trades_1.tradeplan.loc[0,'code'] = temp_code
                        tp_index= 0
                        trades_1.tradebook_head['date_first_trade'] =  code_df2.loc[date_index_1,'date' ]
                        trades_1.tradebook_head['date_last_trade'] =  code_df2.loc[date_index_5 ,'date' ]
                    else :
                        # extend tradeplan  
                        # print( trades_1.tradeplan)
                        tp_index = trades_1.tradeplan.index.max() +1
                        trades_1.tradeplan.loc[tp_index ,'code'] = temp_code
                        
                        trades_1.tradebook_head['date_last_trade'] =  code_df2.loc[date_index_5 ,'date' ]

                    ### Content of tradeplan
                    trades_1.tradeplan.loc[tp_index,'date_plan'] = dt_0 
                    trades_1.tradeplan.loc[tp_index,'date_trade_1st'] = code_df2.loc[date_index_1,'datetime']
                    # 根据资金量确定交易金额和数量等。
                    # get initial capital or latest cash| value
                    # Assume 5 days to buy enough positions,and no change since then
                    # half open half close; later we want to get average of 1min frequency
                    trades_1.tradeplan.loc[tp_index,'method'] = "open_close" 
                    trades_1.tradeplan.loc[tp_index,'period'] = "5D"
                    trades_1.tradeplan.loc[tp_index,'quote_index_start'] = date_index_1
                    trades_1.tradeplan.loc[tp_index,'quote_index_end'] = date_index_5
                    if weight_dif >= 0 :
                        trades_1.tradeplan.loc[tp_index, 'signal_pure'] = 1
                    elif weight_dif <0 :
                        trades_1.tradeplan.loc[tp_index, 'signal_pure'] = -1
                    trades_1.tradeplan.loc[tp_index, 'weight_dif'] = weight_dif
                    # portfolio_suites.account.account_sum['total','cash']
                    date_start2 = code_df2.loc[date_index_1 ,'date' ]
                    # date_start2 = date_start2[:4]+'-' +date_start2[4:6]+'-'+date_start2[6:] 
                    # print('date_start2 ',date_start2  )
                    # print( type( account_1.account_sum['date'] ) )
                    ### In case not type of datetime, transfer string to datetime.
                    trades_1.tradeplan.loc[tp_index, 'total_amount'] = account_1.account_sum.loc[temp_index_pre,'total'] * float( abs(weight_dif))
                    if weight_dif >= 0 :
                        cash_balance = cash_balance - trades_1.tradeplan.loc[tp_index, 'total_amount'] 
                        index_list_buy = index_list_buy + [ tp_index  ]
                        cash_out = cash_out + trades_1.tradeplan.loc[tp_index, 'total_amount'] *(1+fees)
                    elif weight_dif <0 :
                        cash_balance =cash_balance + trades_1.tradeplan.loc[tp_index, 'total_amount'] 
                        index_list_sell = index_list_sell + [ tp_index  ]
                        cash_in = cash_in+ trades_1.tradeplan.loc[tp_index, 'total_amount'] *(1-fees)
                    print("Date",dt_start_str,"cash_balance ", cash_balance)

                    ### get number
                    ave_price = 0.5*( quote_df['open'].mean() + quote_df['close'].mean() )
                    # Nan plus float = NaN
                    if np.isnan(ave_price) :
                        ave_price = quote_df['close'].mean()
                    
                    trades_1.tradeplan.loc[tp_index, 'num'] = round(trades_1.tradeplan.loc[tp_index, 'total_amount']/num_per_lot/ave_price,0)*num_per_lot
                    trades_1.tradeplan.loc[tp_index, 'ave_price'] = ave_price
                    trades_1.tradeplan.loc[tp_index, 'total_amount'] =ave_price* trades_1.tradeplan.loc[tp_index, 'num']
                    # print("ave price ", ave_price)
                    # print("total " ,account_1.account_sum.loc[tp_index_pre,'total']  )
                    # print("weight ", float(weight_dif) )
                    # print("amount ",  trades_1.tradeplan.loc[tp_index, 'total_amount'])
                    # print("num  ", round(trades_1.tradeplan.loc[tp_index, 'total_amount']/num_per_lot/ave_price,0)*num_per_lot)

        ################################################################################
        ### Check Cash balance and modified trade plan 
        cash_balance_now =cash_balance +cash_in - cash_out 
        if cash_balance_now < cash_balance_min :
            # we need to adjust all buy trade,starting from diminshing all buy trade
            # Logic: cash1 = cash0 + cash_in - cashout 
            cash_out_max =  cash_balance - cash_balance_min +cash_in
            amount_sell_pct = cash_out_max /cash_out # maybe 88% 
            for tp_index in index_list_sell : 
                trades_1.tradeplan.loc[tp_index, 'num'] =  round( trades_1.tradeplan.loc[tp_index, 'num']*amount_sell_pct/num_per_lot,0)*num_per_lot
                trades_1.tradeplan.loc[tp_index, 'total_amount']= trades_1.tradeplan.loc[tp_index, 'ave_price']*trades_1.tradeplan.loc[tp_index, 'num'] 

        ################################################################################
        ### Drop tradeplan with num < 1
        # 190414 发现TP里20070604对csi800位股票池计算交易计划时，3e的市场组合里有21个股票的num,total_amout是空值。估计大概率是因为小于100股。
        print( "trades_1.tradeplan" )
        print( trades_1.tradeplan )
        trades_1.tradeplan =trades_1.tradeplan[ trades_1.tradeplan['num']>= 1 ]
        trades_1.tradeplan =trades_1.tradeplan[ trades_1.tradeplan['total_amount']>= 100 ]

        ################################################################################
        ### We want to sell first to get cash and buy then

        trades_1.tradeplan= trades_1.tradeplan.sort_values(by='signal_pure')
        ################################################################################
        ###  'weight_dif' is the original adjusting target and "total_amount" is realizable amount according to the 5D trade plan 

        # print("Amount pct of tradeplan list ")
        # print( trades_1.tradeplan['total_amount'].sum()/account_1.account_sum.loc[temp_index_pre,'total'])

        portfolio_suites.trades = trades_1
        return portfolio_suites


    def manage_tradebook(self,portfolio_suites, config_IO_0,date_start,date_end,quote_type,data_wind):
        ### making trade execution records
        # last update 190414 | since 181123 
        trades = portfolio_suites.trades
        account =  portfolio_suites.account
        import datetime as dt 
        # 181123这个是对所有交易日的交易做回顾，不对，应该先定位当前的交易日
        dt_0 = dt.datetime.strptime(date_start,"%Y%m%d")
        tradeplan2 = trades.tradeplan[ trades.tradeplan['date_plan'] == dt_0 ]
        for temp_i in  tradeplan2.index : 
            ########################################################################
            ### first load quotation data 
            temp_code = trades.tradeplan.loc[temp_i,'code']
            (code_head,code_df)=data_wind.load_quotes(config_IO_0,temp_code,date_start,date_end,quote_type)
            code_df = code_df[ code_df["volume"]>0 ]

            code_df['datetime'] = pd.to_datetime( code_df['date'],format="%Y-%m-%d",errors="ignore" )
            code_df2=code_df[ code_df['datetime']>=dt_0 ]

            date_index_1 = code_df2.index[0]
            date_index_5 = code_df2.index[4]

            # print( date_index_1,date_index_5 )
            quote_df = code_df2.loc[date_index_1:date_index_5 ,: ]
            

            if trades.tradeplan.loc[temp_i,'method'] == "open_close" :
                ########################################################################
                ### Notes: sometimes there is no open price for HK and us stock
                # ['market','currency', 'date', 'symbol', 'BSH', 'ave_price', 'number', 'ave_cost', 'fees', 'profit_real']

                quote_df['ave_cost'] = 0.5*( quote_df['open'] + quote_df['close'])
                ### 判断df一列是否有一个Nan
                if quote_df['open'].isnull().any() :
                    quote_df['ave_cost'] =  quote_df['close']
                quote_df['ave_price'] = quote_df['ave_cost']
                ################################################################################
                ### extend tradebook if needed 
                if len( trades.tradebook.index ) >=1 :
                    
                    temp_index = trades.tradebook.index.max() +1
                    temp_index_end = trades.tradebook.index.max() +1+ len( quote_df.index )
                    # print(trades.tradeplan.index)
                    # print( temp_index,temp_index_end )
                else : 
                    temp_index= 0
                    temp_index_end =  len( quote_df.index )
                ################################################################################
                ## Add market and currency tags
                for temp_j in range(temp_index,temp_index_end): 
                    trades.tradebook.loc[temp_j ,'symbol']= temp_code  
                                 
                if temp_code[-2:] == 'SH' :
                    trades.tradebook.loc[temp_index:temp_index_end ,'market'] = 'SSE'
                    trades.tradebook.loc[temp_index:temp_index_end ,'currency'] = 'rmb'
                elif temp_code[-2:] == 'SZ' :
                    trades.tradebook.loc[temp_index:temp_index_end ,'market'] = 'SZSE'
                    trades.tradebook.loc[temp_index:temp_index_end ,'currency'] = 'rmb'
                elif temp_code[-2:] == 'HK' :
                    trades.tradebook.loc[temp_index:temp_index_end ,'market'] = 'CNHK'
                    trades.tradebook.loc[temp_index:temp_index_end ,'currency'] = 'hkd'
                    
            trades.tradebook.loc[temp_index:temp_index_end ,'date'] =  list( quote_df.loc[:,'date'] )
            trades.tradebook.loc[temp_index:temp_index_end ,'BSH'] = trades.tradeplan.loc[temp_i,'signal_pure']

            ########################################################################
            ### buy action 
            if trades.tradeplan.loc[temp_i,'signal_pure'] == 1 : 
                ### buy or open long position  trade
                # get trade for first date               
                # print(trades.tradebook)
                # print( quote_df.loc[:,'date'] )
                # print("=============================")
                # print( trades.tradebook.loc[temp_index:temp_index_end ,'date'] ) 

                ### todo 若非第一次买入，需要更新平均成本，非第一次卖出，需要更新最后一次卖出价格=ave_price
                ### Consider to update average cost or price when there are multiple buy/sell actions
                trades.tradebook.loc[temp_index:temp_index_end ,'ave_price'] = list( quote_df.loc[:,'ave_price']) 
                trades.tradebook.loc[temp_index:temp_index_end ,'ave_cost'] = list( quote_df.loc[:,'ave_cost']) 
                trades.tradebook.loc[temp_index:temp_index_end ,'profit_real'] = 0.0 

                ### note the total amout is devide by 5 here 
                ### quote_df.index means how many trading days we need
                amount_1day = round ( trades.tradeplan.loc[temp_i,'total_amount']/ len(quote_df.index)  )     
                num_per_lot =100                
                trades.tradebook.loc[temp_index:temp_index_end ,'number'] =max(round(amount_1day/quote_df['ave_cost'].mean()/num_per_lot)*num_per_lot,0)
                temp_amount_list =max(round(amount_1day/quote_df['ave_cost'].mean()/num_per_lot)*num_per_lot,0)*quote_df['ave_cost']
                
                trades.tradebook.loc[temp_index:temp_index_end ,'amount'] = list(temp_amount_list)
                trades.tradebook.loc[temp_index:temp_index_end ,'fees'] =trades.tradebook.loc[temp_index:temp_index_end ,'amount']  *0.0025
                # print("trades.tradeplan======================")
                # print(trades.tradeplan ) 
            elif trades.tradeplan.loc[temp_i,'signal_pure'] == -1 : 
                ### sell action 
                trades.tradebook.loc[temp_index:temp_index_end ,'ave_price'] = list( quote_df.loc[:,'ave_price']) 
                # get average cost per share from account stock
                code_as_index = account.account_stock[ account.account_stock['code']== temp_code ].index[0]
                temp_total_cost = account.account_stock.loc[code_as_index,'total_cost']
                temp_num = account.account_stock.loc[code_as_index,'num']
                temp_ave_cost = temp_total_cost/temp_num
                trades.tradebook.loc[temp_index:temp_index_end ,'ave_cost'] = temp_ave_cost
                
                ### get number to trade per day 
                num_1day = round ( trades.tradeplan.loc[temp_i,'num']/ len(quote_df.index) ,0 ) 
                ### rest num to sell at the end trading day
                num_1day_end = trades.tradeplan.loc[temp_i,'num'] - ( len(quote_df.index)-1)*num_1day  
                trades.tradebook.loc[temp_index:temp_index_end ,'number'] =num_1day
                trades.tradebook.loc[temp_index_end-1 ,'number'] =num_1day_end
                ### Amount 
                trades.tradebook.loc[temp_index:temp_index_end ,'amount'] = list( num_1day_end*quote_df['ave_cost'] )
                trades.tradebook.loc[temp_index_end-1 ,'amount'] =  num_1day_end* quote_df.loc[date_index_5,'ave_cost'] 
                
                trades.tradebook.loc[temp_index:temp_index_end ,'profit_real'] =list( trades.tradebook.loc[temp_index:temp_index_end ,'amount']*(1-0.0025)- trades.tradebook.loc[temp_index:temp_index_end ,'ave_cost']*trades.tradebook.loc[temp_index:temp_index_end ,'number'] )
                ### note the total amout is devide by 5 here 
                ### quote_df.index means how many trading days we need 

            ###############################################################################################
            trades.tradebook.loc[temp_index:temp_index_end ,'fees'] = list( trades.tradebook.loc[temp_index:temp_index_end ,'amount']  *0.0025 )

            ### open and close quotes are needed for account calculations
            trades.tradebook.loc[temp_index:temp_index_end ,'open'] =  list( quote_df.loc[:,'open'] )
            trades.tradebook.loc[temp_index:temp_index_end ,'close'] =  list( quote_df.loc[:,'close'] )

            # print("DEBUG================")
            # print( trades.tradebook.loc[temp_index:temp_index_end , :]  )
            # print( quote_df  )

        portfolio_suites.trades = trades
        portfolio_suites.account = account
        
        return portfolio_suites












