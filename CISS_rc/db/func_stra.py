# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
===============================================
Function:
Generate portfolio using symbol list as input 
        last 181109 | since 181109
功能： 
    若signals交易信号中资产配置比例数值有问题，需要进行调整

Menu :
THREE COMPONENTS:
1,class A:
    {INPUT,ALGO,OUTPUT  }
1,function a:
    {INPUT,ALGO,OUTPUT  }


分析：
0，策略算法开发流程：
    假设 --》 方法论，模型 --》 
    

1，输入:
   
2，配置文件 |  
 
3,output file 
    1,head json file of portfolio
    2,portfolio dataframe of portfolio 
    3,stockpool dataframe of portfolio

Notes: 
refernce: rC_Stra_MAX.py 
===============================================
'''
class functions():
    def __init__(self, func_name ):
        self.func_name = func_name

class strategies():
    def __init__(self, stra_name ):
        self.stra_name = stra_name

    # 策略流程应该是 输入信息管理，计算过程管理(可能涉及专属的策略模型)，输出信息。

##################################################################
class stra_prepare():
    def __init__(self, stra_name ):
        self.stra_name = stra_name
        # 

##################################################################

class stra_allocation():
    def __init__(self, stra_name ):
        self.stra_name = stra_name
        # generate allocation weights for portfolio assets 

    def stock_weights(self,ind_level,sty_v_g, sp_df) :
        # update:增加对市场组合的权重重新计算 | 190412
        # sty_v_g = 'value' or 'growth'
        # INPUT sp_df
        # OUTPUT: weigh_list
        # weigh_list.columns=['code','ind1_code','ind2_code','ind3_code',col_w_value,col_w_growth]
        
        # print(sp_df.loc[:,['code','ind1_code']].head()  )

        ####################################################################
        ### set column item that we want to filtering | col_name = 'w_allo_'+'growth'+'_ind3'
        
        if not ind_level == "0" :
            col_name = 'w_allo_'+sty_v_g+'_ind'+ind_level
            col_w_value = 'w_allo_value_ind'+ind_level
            col_w_growth = 'w_allo_growth_ind'+ind_level
            weight_list= sp_df.loc[:,['code','ind1_code','ind2_code','ind3_code',col_w_value,col_w_growth]]
            # we want to drop stock with portfolio weight smaller than 0.1%, which means no significant support
            # to portfolio return or risks.
            # when calc 601020, we found 600747 holds 0.1088% at 20140531,we think we want to excule this type of firm
            weight_list= weight_list[ weight_list[col_name ] >= 0.0011 ]       
        else :
            ####################################################################
            ### working on whole market 
            ind_level = "1"
            col_name = 'w_allo_'+sty_v_g+'_ind'+ind_level
            col_w_value = 'w_allo_value_ind'+ind_level
            col_w_growth = 'w_allo_growth_ind'+ind_level
            weight_list= sp_df.loc[:,['code','ind1_code','ind2_code','ind3_code',col_w_value,col_w_growth]]
            # we want to drop stock with portfolio weight smaller than 0.1%, which means no significant support
            # to portfolio return or risks.
            # when calc 601020, we found 600747 holds 0.1088% at 20140531,we think we want to excule this type of firm
            weight_list= weight_list[ weight_list[col_name ] >= 0.0005 ]  
            weight_list[col_name] = weight_list[col_name]/weight_list[col_name].sum()

            # Twice optimze :Drop small weights again 
            weight_list= weight_list[ weight_list[col_name ] >= 0.0005 ]  
            weight_list[col_name] = weight_list[col_name]/weight_list[col_name].sum()
            
            weight_list[col_w_value] = weight_list[col_w_value]/weight_list[col_w_value].sum()
            weight_list[col_w_growth] = weight_list[col_w_growth]/weight_list[col_w_growth].sum()

            # print( weight_list.info() )
            # print( weight_list.head() )
            # print( weight_list[col_w_value].sum() )
            # print( weight_list[col_w_growth].sum() )
            # asd

        ### 根据对利润的配置权重和估值，调整配置权重。 估值是个tricky的问题。
        # 读取最新股本，结合股价，计算出每个股票在t时间的P/E：如果w_allo 70:30, 估值调整后w_mv= 59.3:40.7
        # 如果从效率的角度，似乎应该全部选择每单位市值利润最大的股票 profit/(1rmb mv)。如果从均衡捕捉行业价值的角度，
        # 同时控制个股的风险，那么我们的方式就比较合适。

        ''' steps:
        1,get lastest number of shares, using close at reference date to get market value.p/e= MV/profit_q4_es
        2,get new weight of stock value using p/e and w_allo_value|growth
        
        
        '''
        ######################################################################
        ### 根据input参数，赋值给标准权重 
        # last | since 190712
        if sty_v_g == 'value' :
            weight_list[ "pct_port"] = weight_list[col_w_value]
        elif sty_v_g == 'growth':
            weight_list[ "pct_port"] = weight_list[col_w_growth] 


        return weight_list 


    def stock_weights_etf(self, df_head,df_stocks) :
        ### Function：生成etf组合权重
        ### INPUT :df_head{etf组合信息} ；df_stocks or sp_df{股票池权重}，
        ### OUTPUT:
        ### update:190709 | since 190709
        '''
        Example of df_stocks:
        code name num mark premium_pct amount
        0 1 平安银行 2400 3 0.1 33624
        todo items:
        1, if mark in{1=允许,3=深市退补},需要计算可成交价格，乘以股票数量后减去交易成本得到所费的现金。其中etf-sse的"amount"有值的是szse的
        2，if mark ==2=必须,要么选取停牌前20天均价，要么使用“amount"中的价格。

        '''
        ### get last quotation for given period and code list 
        ###TODO 在 db\\db_assets\\get_wind.py 里更新
        # 一次性获取所有的历史当日的股票收盘价，记住这里不要复权！
        '''
        >>> Wp.w.wss("600036.SH,601398.SH", "close,volume","tradeDate=20190704;priceAdj=U;cycle=D")
        .ErrorCode=0
        .Codes=[600036.SH,601398.SH]
        .Fields=[CLOSE,VOLUME]
        .Times=[20190709 16:48:28]
        .Data=[[36.08,5.68],[37917354.0,151690647.0]]
        '''
        import pandas as pd 
        
        ### notesL: df_stocks.code is in raw format : [1, 2, 63, 69, 100, 157, 166, 333]
        code_list = list( df_stocks.code_wind )
        items = ["close","volume"]

        # "20190708" or # "20190708.0"
        tradeDate =  df_head.loc["TradingDay","value"]  
        print( tradeDate )

        ### Method1 : Get wind quotation 
        # from db.db_assets.get_wind import wind_api
        # wind_api0 =wind_api()
        # wind0 = wind_api0.Get_wss(code_list,items,tradeDate)
        # df_wind = pd.DataFrame( wind0.Data, columns=code_list,index=items )
        # df_wind.to_csv("D:\\df_wind_1907.csv")

        ### Method2 :Import quotation from absolute path 
        df_wind = pd.read_csv("D:\\df_wind_190704.csv",index_col="Unnamed: 0")

        print("df_wind \n", df_wind.head)
        print( df_wind.info() )

        ####################################################################
        ### 根据mark计算权重、可执行价格。
        for temp_i in df_stocks.index :
            temp_mark = df_stocks.loc[temp_i, "mark"]
            temp_code_w = df_stocks.loc[temp_i, "code_wind"]
            # type(temp_code) is string 

            ### add quotation for trading stocks 
            #notes: type(temp_mark) is "str"
            
            if temp_mark in ["1","3"] :
                ### get last price from quotation
                df_stocks.loc[temp_i, "mv_es"] = df_stocks.loc[temp_i, "amount"]
                ### find quotes
                temp_close = df_wind.loc["close",temp_code_w]
                temp_vol = df_wind.loc["volume",temp_code_w]
                if temp_vol > 0 :
                    
                    # print( type( df_stocks.loc[temp_i, "num"]) )   # "str"
                    df_stocks.loc[temp_i, "mv_es"] =  temp_close * float( df_stocks.loc[temp_i, "num"] )


            elif temp_mark in ["2"] :
                df_stocks.loc[temp_i, "mv_es"] = df_stocks.loc[temp_i, "amount"]

        ####################################################################
        ### 
        weight_list = df_stocks
        para_cash_pct = 0.03 # 现金比例
        
        # type is str or char change to float
        weight_list[ "mv_es"] = pd.to_numeric( weight_list[ "mv_es"] )
        # print( weight_list[ "mv_es"].sum() )

        weight_list[ "pct_port"] = weight_list[ "mv_es"]/ (weight_list[ "mv_es"].sum()/para_cash_pct  )
        weight_list[ "amt"] =  weight_list[ "mv_es"]
        # df_stocks.to_csv("D:\\df_stocks_1907.csv",encoding="gbk")

        return weight_list




































