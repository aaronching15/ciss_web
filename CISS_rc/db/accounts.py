# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：
last update 181104 | since  181104
Menu :
分析：
1，账户管理
    1.1，账户持仓
    1.2，账户汇总

2，配置文件 | config\config_accounts.py

3，股票池和组合当前是 1：N，下一步考虑 N：1，N:N的情况
    例如：如果同时有｛A股，港股，美股｝多个市场情况下，是把所有个股放1个stockpool里，
    还是每个市场单独一个stockpool ?

4,output file 
    1,head json file of portfolio
    2,portfolio dataframe of portfolio 
    3,stockpool dataframe of portfolio
5, # generate account type 
    # 1, Open end/closed: cash withdraw/deposit 
    # 2, 社保或政府事业单位，公募基金，保险基金，券商集合，银行理财，私募，散户
    # notes:出资方的定义主要在 funds.py 模块，本处主要是根据出资方定义账户投资出入资金的行为

THREE COMPONENTS:
1,class A:
    {INPUT,ALGO,OUTPUT  }
1,function a:
    {INPUT,ALGO,OUTPUT  }



Notes: 
1,notes:account only provide raw data, statistics and summary should be procided by portfolio module 
2,refernce: rC_Portfolio_17Q1.py 
===============================================
'''
import numpy as np
import pandas as pd

###################################################
class accounts():
    def __init__(self,init_date="20140531",account_name=''):
        self.account_name = account_name
        # initial date is the beginning date of account 
        self.init_date = init_date





###################################################
class gen_accounts():
    def __init__(self,id_time_stamp,port_df,init_date, config={},init_cash=100000000.0,account_name=''):
         
        # generate Portfolio by using following modules:
        # 1,account_sum
        # 2,account_stock
        # 3,account_bond
        self.account_head = self.gen_account_head(id_time_stamp,port_df,init_date, config,init_cash,account_name)

        self.account_sum   = self.gen_account_sum(port_df,self.account_head ,init_date )
        self.account_stock = self.gen_account_stock(port_df,self.account_head  )
        self.account_bond  = self.gen_account_bond(port_df,self.account_head  )
        self.account_fund  = self.gen_account_fund(port_df,self.account_head  )
        # 现金工具，如交易所和银行间逆回购，银行账户存款等
        # self.account_cash | cash instuments:{SSE:GC001 ; IB-Mkt:R001 }

    def gen_account_sum (self, port_df,account_head,init_date='2014-05-31'  ) :
        ### account_sum  
        ### last update 190711

        len_init = 50 # initial maximum positions is 50 
        # market_value used to be 'Stock',but now we have more asset types
        # mdd:MDD; unit_i:index; mdd_i : MDD_I 
        columns_sum = ['cash','total_cost', 'market_value', 'total', 'unit', 'mdd', 'unit_i', 'mdd_i']  # 8 items
        ## create a date list using init_date as beginning, but notice that init_date might not be a trading day 
        # import trading date module and judge which weekday
        from db.times import times
        country='CN'
        exchange='SSE'
        times1 = times(country,exchange)
        init_date=init_date.replace('-','')
        if_start=0
        dates = times1.gen_dates_day(country,exchange,init_date,if_start) 
        #  np.zeros([len_init, len(columns_sum)])
        account_sum  = pd.DataFrame( columns=columns_sum, index=dates)
        # initialize the first day 
        print("dates \n", dates ,"init_date",init_date)
        ### if length of dates is 0, then assign initial date to start-date
        if len( dates.index ) < 1 :
            start_date = init_date
        else :
            start_date = dates.iloc[0]
        
        account_sum.loc[start_date,'cash'] = account_head["Cash"] # =init cash
        account_sum.loc[start_date,'total_cost'] = 0.0
        account_sum.loc[start_date,'market_value'] = 0.0
        account_sum.loc[start_date,'total'] = account_head["Cash"] 
        account_sum.loc[start_date,'unit'] = 1.000
        account_sum.loc[start_date,'mdd'] = 0.000
        account_sum.loc[start_date,'unit_i'] = 1.000
        account_sum.loc[start_date,'mdd_i'] = 0.000
        account_sum['date'] = account_sum.index

        return account_sum 

    def gen_account_stock (self, port_df,account_head  ) :
        # 2,account_stock
        # Columns_Stocks = ['Num', 'AveCost', 'LastPrice', 'TotalCost', 'MV', 'PnL', 'PnL_Pct', 'W_Real',
        #                   'W_Ideal','Date', 'code']  # 11 items
        columns_stocks = ['num', 'ave_cost', 'last_quote', 'total_cost', 'market_value', 'pnl', 'pnl_pct', 
            'w_real', 'w_optimal','date_update','date_in', 'code','currency','market']  # 11 items

        account_stock  = pd.DataFrame( columns=columns_stocks) 
 
        return account_stock 

    def gen_account_bond (self, port_df,account_head  ) :
        # 2,account_bond
  
        account_bond  = pd.DataFrame( ) 
        return account_bond 

    def gen_account_fund (self, port_df,account_head  ) :
        # 2,account_fund
        account_fund  = pd.DataFrame( ) 

        return account_fund 


    def gen_account_head(self,id_time_stamp,port_df,init_date="20140531", config={},init_cash=100000000.0,account_name='' ) :
        '''
        Generate account head 
        refernce: rC_Portfolio_17Q1.py 
        previous object : log of portfolio 
        notes:account only provide raw data, statistics and summary should be procided by portfolio module 
        '''
        # import pandas as pd 
        account_head ={}
        ## get stockpool id using  time stamp
        import sys
        sys.path.append("..")
        # from db.basics import time_admin 
        # time_admin1 = time_admin()
        # time_stamp = time_admin1.get_time_stamp()

        if config == {} :
            ## Basic info
            # initial date of generate account 
            account_head["InitialDate"] = init_date # previous 
            account_head["Index_Name"] = ""
            if account_name =='':
                account_head["account_name"] = "name_account_" + id_time_stamp
                account_head["account_id"] =  "id_account_" + id_time_stamp
                account_head["account_id_time"]= id_time_stamp
            else :
                account_head["account_name"] = "name_account_" +account_name
                account_head["account_id"] =  "id_account_" + id_time_stamp+"_"+account_name
                account_head["account_id_time"]= id_time_stamp
            # equity,bond,(hybrid),money market, alternative,QDII,RQFII,closed-end
            account_head["account_type"] = ""            

            account_head["MaxN"] = ""
            account_head["Leverage"] = ""
            account_head["date_Start"] = ""
            account_head["date_LastUpdate"] = ""
            account_head["path_SP"] = ""   # previous name = path_Symbol
            account_head["w_equity_max"] = 0.0
            account_head["w_equity_min"] = 0.0
            account_head["w_bond_max"] = 0.0
            account_head["w_bond_min"] = 0.0
            account_head["w_cash_min"] = 0.0 # min level of cash as weight in account
            account_head["info"] = ""
            ## Values 
            account_head["Total_Cost"] = 0.0
            account_head["Cash"] = init_cash
            account_head["Stock"] = 0.0
            account_head["Total"] = init_cash
            # account_head["Unit"] = 0.0
            # account_head["MDD"] = 0.0
            ## profit,loss, returns,risks and other statistics 
            account_head["PnL"] = 0.0
            account_head["PnL_pct"] = 0.0
            account_head["r_annual"] = 0.0
            account_head["PnL_total"] = 0.0
            account_head["PnL_Pct"] = 0.0
            account_head["total_ProfitReal"] = 0.0
            account_head["total_Profit_R"] = 0.0
            # # statistics: max,min,mean,median
            # account_head["W_max"] = 0.0
            # account_head["W_max_code"] = 0.0
            # account_head["profit_max"] = 0.0
            # account_head["profit_max_code"] = ""
            # account_head["loss_max"] = 0.0
            # account_head["loss_max_code"] = ""
            # account_head["PnL_Pct_max"] = 0.0
            # account_head["PnL_Pct_max_code"] = ""
            # account_head["PnL_Pct_min"] = 0.0
            # account_head["PnL_Pct_min_code"] = ""
            # account_head["num_Trade_Profit"] = 0.0
            # account_head["ave_Trade_Profit"] = 0.0
            # account_head["total_Loss_R"] = 0.0
            # account_head["num_Trade_Loss"] = 0.0
            # account_head["total_Fees"] = 0.0
            # account_head["W_Ideal_max"] = 0.0
            # account_head["W_Ideal_max_code"] = ""
            # account_head["ave_Trade_Loss"] = 0.0
            # unit, cash,stocks
            account_head["Unit-5D"] = {}
            account_head["Cash-5D"] = {}
            account_head["Stock-5D"]= {}
 
        return account_head



class manage_accounts():
    '''
    Manage and update trade objects 
    1, build trade plan according to signals
    2, exucute trade orders with trade plan and market quotes
    3, 
    '''
    def __init__(self,accounts_name=''):
        # load trade objects 
        # load accounts_id_port_rc001 
        self.accounts_name='' 
    
    def load_accounts(self,trade_id='',path_base= '',accounts_name='') :
        '''
        using portfolio_suites to load accounts info ,
        or can use this module to load accounts specifically.
        todo

        ''' 
        return 1

    def update_accounts_with_trades(self,temp_date,portfolio_suites,config_IO_0,date_start,date_end,quote_type,data_wind):
        # making trade plan using signals 
        trades_0 = portfolio_suites.trades
        tradebook = trades_0.tradebook

        accounts_0 =  portfolio_suites.account

        account_head  = accounts_0.account_head 
        account_sum   = accounts_0.account_sum
        account_stock = accounts_0.account_stock  
        account_bond  = accounts_0.account_bond  
        # account_fund  = accounts_0.account_fund  

        account_sum['datetime'] = pd.to_datetime( account_sum.index )
        account_sum  = account_sum.sort_values('datetime')

        # get all trading dates from tradebook
        # tradebook['datetime'] = pd.to_datetime(tradebook['date'], format='%Y-%m-%d' ) 
        # tradebook =tradebook.sort_values('datetime')
        # date_list = list( tradebook['date'].drop_duplicates() )
        # for temp_date in date_list: 
            
        # 2014-06-03
        ## initailize a_sum | format of date in index of a_sum is 2014-06-05
        df_as = account_sum[ account_sum['datetime']<= temp_date  ] 

        if len( df_as.index) < 2 :
            # we are at first account  day
            temp_index = temp_date
        else:
            pre_date = df_as.index[-2]
            # print('pre_date ',pre_date )
            account_sum.loc[temp_date, 'total_cost'] = account_sum.loc[pre_date, 'total_cost']
            account_sum.loc[temp_date, 'cash'] = account_sum.loc[pre_date, 'cash']
            account_sum.loc[temp_date, 'market_value'] = account_sum.loc[pre_date, 'market_value']
            account_sum.loc[temp_date, 'total'] = account_sum.loc[temp_date, 'cash'] + account_sum.loc[  temp_date, 'market_value']
            account_sum.loc[temp_date, 'unit'] = account_sum.loc[temp_date, 'total'] / account_sum[ 'total'].iloc[0]
            account_sum.loc[temp_date, 'mdd'] = account_sum.loc[pre_date, 'mdd']

        # temp_date = 2007-05-31 00:00:00 

        # print( account_sum.loc[temp_date, :] )
        # asdasd = input("Check errors1......") 

        ## update accounts using this trades  
        tradebook['datetime'] = pd.to_datetime(tradebook['date'])
        tb_1d = tradebook[ tradebook['datetime'] ==temp_date ] 



        for trade_index in tb_1d.index :
            ## step: 1,是否有持仓； 2，更新持仓，计算比例；3，调减现金(成交价)，增加股票(收盘价)
            temp_code = tb_1d.loc[trade_index, 'symbol']
            
            df_as = account_stock[ account_stock['code']== temp_code ]
     
            ### Assign value of BSH to bsh
            bsh= tradebook.loc[trade_index, 'BSH'] # 1 means buy and -1 means sell
            if len(df_as.index) <1 :
            # no holding record in account stocks                     
                ifholding = 0
                if len( account_stock.index ) <1 :
                    # no holding stocks in account stocks  
                    i_acc = 0
                    i_trade = trade_index  
                # some other stocks in account stocks
                else :
                    i_acc = account_stock.index.max() +1 
                    i_trade = trade_index 
                
                ### We can only buy if no holding 
                ## update account_stock,account sum
                if int(bsh) == 1 :
                    (account_stock,account_sum)= self.trade2account_stock(temp_date,i_acc,i_trade,account_stock,account_sum,tb_1d,bsh,ifholding )
            else :
            # some holding record in account stocks ,we still need to locate index in account stock for i_acc 
                ifholding = 1
                temp_df = account_stock[account_stock['code']==temp_code ]
                i_acc = temp_df.index.values[0]
                i_trade = trade_index 
                ### We can either buy or sell if no holding 
                ## update account_stock,account sum
                (account_stock,account_sum)= self.trade2account_stock(temp_date,i_acc,i_trade,account_stock,account_sum,tb_1d,bsh,ifholding )
            
            ### todo 未解之谜，会出现  bsh | ifholding  -1 0 for market value 20080604-000961.SZ 
            # and market growth for  600900.SH at 20090520 
        ### 
        # print("account_sum ")
        # print( account_sum.loc[temp_date, :] )
        # asdasd = input("Check errors......")  

        # update accounts in portfolio_suites
        portfolio_suites.account.account_sum = account_sum
        portfolio_suites.account.account_stock = account_stock

        return portfolio_suites

    def trade2account_stock(self,temp_date,i_acc,i_trade,account_stock,account_sum,tradebook,bsh=1,ifholding = 0 ):
        # project tradebook to account 
        # consider both case with previous holding and no holding 
        # bsh=1 means buy action 
        # ifholding = 0 means no holding before and 1 means we need to add holding 

        # columns_stocks = ['num', 'ave_cost', 'last_quote', 'total_cost', 'market_value',
        # 'pnl', 'pnl_pct', 'w_real', 'w_optimal','date_update','date_in', 'code','currency','market'] 
        import numpy as np 

        #################################################################################
        ### Initialize new entry for AS from trading book
        account_stock.loc[i_acc,'code'] = tradebook.loc[i_trade, 'symbol']
        account_stock.loc[i_acc,'date_update'] = temp_date
        account_stock.loc[i_acc,'date_in'] = temp_date 
        account_stock.loc[i_acc,'currency'] = tradebook.loc[i_trade, 'currency']
        account_stock.loc[i_acc,'market'] = tradebook.loc[i_trade, 'market']
        account_stock.loc[i_acc,'w_optimal'] = i_acc
        account_stock.loc[i_acc,'last_quote'] = tradebook.loc[i_trade, 'close']
        bsh = int(bsh)
        if bsh ==1 and ifholding == 0 :
            account_stock.loc[i_acc,'num'] = tradebook.loc[i_trade, 'number']           
            account_stock.loc[i_acc,'total_cost'] = tradebook.loc[i_trade, 'amount'] +tradebook.loc[i_trade, 'fees']
            # account sum
            account_sum.loc[temp_date, 'cash'] =account_sum.loc[temp_date, 'cash'] - tradebook.loc[i_trade, 'amount'] - tradebook.loc[i_trade, 'fees']
            account_sum.loc[temp_date, 'total_cost'] = account_sum.loc[temp_date, 'total_cost'] +tradebook.loc[i_trade, 'amount']
            # account_sum.loc[temp_date, 'market_value'] =account_sum.loc[temp_date, 'market_value'] +tradebook.loc[i_trade, 'number']*tradebook.loc[i_trade, 'close']          

        elif bsh ==1 and ifholding == 1 : 
            account_stock.loc[i_acc,'num'] = account_stock.loc[i_acc,'num']+tradebook.loc[i_trade, 'number']
            account_stock.loc[i_acc,'total_cost'] = account_stock.loc[i_acc,'total_cost']+ tradebook.loc[i_trade, 'amount'] -tradebook.loc[i_trade, 'fees']
            # account sum
            account_sum.loc[temp_date, 'cash'] =account_sum.loc[temp_date, 'cash'] - tradebook.loc[i_trade, 'amount'] -tradebook.loc[i_trade, 'fees']
            account_sum.loc[temp_date, 'total_cost'] = account_sum.loc[temp_date, 'total_cost'] +tradebook.loc[i_trade, 'amount']
            # account_sum.loc[temp_date, 'market_value'] =account_sum.loc[temp_date, 'market_value'] +tradebook.loc[i_trade, 'number']*tradebook.loc[i_trade, 'close']

        elif bsh == -1 and ifholding == 1 : 
            # notes, we might only sell a portion of stock 
            # account stock 
            account_stock.loc[i_acc,'num'] = account_stock.loc[i_acc,'num']- tradebook.loc[i_trade, 'number']
            account_stock.loc[i_acc,'total_cost'] = account_stock.loc[i_acc,'total_cost']- account_stock.loc[i_acc,'ave_cost']*tradebook.loc[i_trade, 'number']
            # account sum
            account_sum.loc[temp_date, 'cash'] =account_sum.loc[temp_date, 'cash'] + tradebook.loc[i_trade, 'amount'] -tradebook.loc[i_trade, 'fees']
            account_sum.loc[temp_date, 'total_cost'] = account_sum.loc[temp_date, 'total_cost'] - account_stock.loc[i_acc,'ave_cost']*tradebook.loc[i_trade, 'number']
        # else :
        #     #  bsh | ifholding  1.0 0
        #     if bsh == -1 and ifholding == 0 : 
        #         account_stock.to_csv("D:\\account_stock_190414.csv")
        #         df_as = account_stock[ account_stock['code']== temp_code ]
        #         df_as.to_csv("D:\\df_as_190414.csv")


            # print(" bsh | ifholding ", bsh , ifholding   )
            # print( type(bsh)   )
            # print( bsh ==1 and ifholding == 0  )
            # print( int(bsh) ==1 and ifholding == 0  )
            # print( tradebook.loc[i_trade, :] )
            # asd



        #################################################################################
        ### Rest columns in account stock
        account_stock.loc[i_acc,'ave_cost'] = account_stock.loc[i_acc,'total_cost'] /account_stock.loc[i_acc,'num'] 
        # Before 190412
        # account_stock.loc[i_acc,'market_value'] = tradebook.loc[i_trade, 'number']*tradebook.loc[i_trade, 'close']
        # Since 190412
        account_stock.loc[i_acc,'market_value'] = account_stock.loc[i_acc,'num']*tradebook.loc[i_trade, 'close']
        
        account_stock.loc[i_acc,'pnl'] = account_stock.loc[i_acc,'market_value']-account_stock.loc[i_acc,'total_cost']
        account_stock.loc[i_acc,'pnl_pct'] = account_stock.loc[i_acc,'pnl']/ account_stock.loc[i_acc,'total_cost'] 
        account_stock.loc[i_acc,'w_real'] = account_stock.loc[i_acc,'market_value']/account_sum.loc[temp_date, 'total']
        ### Delete single stock record if market value smaller than 1 or number smaller than 0.1
        account_stock = account_stock[ account_stock['market_value']>=1 ]
        account_stock = account_stock[ account_stock['num']>=0.1 ]

        #################################################################################
        ### account sum
        account_sum.loc[temp_date, 'market_value'] =  account_stock['market_value'].sum()
        account_sum.loc[temp_date, 'total'] = account_sum.loc[temp_date, 'cash'] + account_sum.loc[  temp_date, 'market_value']
        account_sum.loc[temp_date, 'unit'] = account_sum.loc[temp_date, 'total'] / account_sum[ 'total'].iloc[0]
        temp_mdd =  account_sum.loc[temp_date,'total' ] / max(account_sum.loc[:temp_date,'total' ] )-1

        ### Maximum drawdown; account_sum.index  is DatetimeIndex, which can be compared with string type of date 
        df_as = account_sum[ account_sum.index < temp_date ]
        if len( df_as.index ) > 1:
            # print('df_as',temp_date)
            # print( df_as )
            account_sum.loc[temp_date, 'mdd'] = min( df_as['mdd' ].min() , temp_mdd)  
        else :
            account_sum.loc[temp_date, 'mdd'] = 0


        return account_stock,account_sum
 
    def update_accounts_with_quotes(self,temp_date,portfolio_suites,config_IO_0,date_start,date_end,quote_type,data_wind,if_trade):
        # making trade plan using signals 

        accounts_0 =  portfolio_suites.account
        # account_head  = accounts_0.account_head 
        account_sum   = accounts_0.account_sum
        account_stock = accounts_0.account_stock  
        # account_bond  = accounts_0.account_bond  
        # account_fund  = accounts_0.account_fund  

        account_sum['datetime'] = pd.to_datetime( account_sum.index )
        account_sum  = account_sum.sort_values('datetime')

        ### Debug check in case 
        if account_sum.loc[temp_date, 'cash'] == np.nan :
            print("Cash before quote"  )
            print( "Check nan" )
            print( account_sum.loc[temp_date, 'cash']  )    
            asd

        ### Initialize Asum for no trading record case 
        if if_trade == 0 :
        # 2014-06-03
        ## initailize a_sum | format of date in index of a_sum is 2014-06-05
            df_as = account_sum[ account_sum['datetime']<= temp_date  ] 

            # 2017-06-07 00:00:00 || 20170607 
            
            if len( df_as.index) < 2 :
                # we are at first account  day
                temp_index = temp_date
            else:
                pre_date = df_as.index[-2]
                # print('pre_date ',pre_date )
                account_sum.loc[temp_date, 'total_cost'] = account_sum.loc[pre_date, 'total_cost']
                account_sum.loc[temp_date, 'cash'] = account_sum.loc[pre_date, 'cash']  
                account_sum.loc[temp_date, 'market_value'] = account_sum.loc[pre_date, 'market_value']
                account_sum.loc[temp_date, 'total'] = account_sum.loc[temp_date, 'cash'] + account_sum.loc[  temp_date, 'market_value']
                account_sum.loc[temp_date, 'unit'] = account_sum.loc[temp_date, 'total'] / account_sum[ 'total'].iloc[0]
                account_sum.loc[temp_date, 'mdd'] = account_sum.loc[pre_date, 'mdd']

        # if if_trade == 1 , then we already update account_sum in previous moudle

        import datetime as dt 
        for temp_index in account_stock.index :
            temp_code = account_stock.loc[temp_index,'code']
            
            ## load quote info using data_wind module  
            # print("temp_code ",temp_code)
            (code_head,code_df)=data_wind.load_quotes(config_IO_0,temp_code,date_start,date_end,quote_type)

            code_df['datetime'] = pd.to_datetime( code_df['date'] )
            code_df2=code_df[ code_df['datetime'] == temp_date ]
            # print('code_df2')
            # print(  code_df2 )
            if len( code_df2.index ) == 1 :
                # print("code_df2.index")
                # code_df2.index.values = [242]
                close = code_df2.loc[code_df2.index.values[0],'close']

                # print('close for code',temp_code,'is ',close )
                
                # account_stock 
                account_stock.loc[temp_index,'date_update'] = temp_date   
                account_stock.loc[temp_index,'last_quote'] = close  

                account_stock.loc[temp_index,'market_value'] = account_stock.loc[temp_index,'num'] * close
                account_stock.loc[temp_index,'pnl'] = account_stock.loc[temp_index,'market_value']-account_stock.loc[temp_index,'total_cost']
                account_stock.loc[temp_index,'pnl_pct'] = account_stock.loc[temp_index,'pnl']/ account_stock.loc[temp_index,'total_cost'] 

     
        account_stock['w_real'] = account_stock['market_value']/account_sum.loc[temp_date, 'total']
        # account sum
        account_sum.loc[temp_date, 'cash'] =account_sum.loc[temp_date, 'cash']*(1+0.025/365) 
        account_sum.loc[temp_date, 'market_value'] = account_stock['market_value'].sum()
        account_sum.loc[temp_date, 'total'] = account_sum.loc[temp_date, 'cash'] + account_sum.loc[  temp_date, 'market_value']
        account_sum.loc[temp_date, 'unit'] = account_sum.loc[temp_date, 'total'] / account_sum[ 'total'].iloc[0]
        temp_mdd =  account_sum.loc[temp_date,'total' ] / max(account_sum.loc[:temp_date,'total' ] )-1

        # account_sum.index  is DatetimeIndex, which can be compared with string type of date 
        df_as = account_sum[ account_sum.index < temp_date ]
        if len( df_as.index ) > 1:
            account_sum.loc[temp_date, 'mdd'] = min( df_as['mdd' ].min() , temp_mdd)  
        else :
            account_sum.loc[temp_date, 'mdd'] = 0
        
        ### Debug check in case 
        
        if account_sum.loc[temp_date, 'cash'] == np.nan :
            print("Cash after quote"  )
            print( "Check nan" )
            print( account_sum.loc[temp_date, 'cash']  )    
            asd
        # asdasd = input("Check errors-0414......") 

        # update accounts in portfolio_suites
        portfolio_suites.account.account_sum = account_sum
        portfolio_suites.account.account_stock = account_stock

        return portfolio_suites
