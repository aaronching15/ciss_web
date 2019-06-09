# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：
last update 181023 | since  160121
Menu :
THREE COMPONENTS:
1,class A:
    {INPUT,ALGO,OUTPUT  }
1,function a:
    {INPUT,ALGO,OUTPUT  }


class assets() : 
    初始化资产大类
class stocks() : 初始化权益\股票类别
class s() :

todo:
hierarchy of stocks 
    CN\\SSE\\A_sh,B_sh,科创板
    CN\\SZSE\\A_sz,B_sh,SME,ChiNext
    CN\\NEEQ\\base,innovation
    CN\\CNHK\stock,option,warrant
    US\\NYSE
    US\\NASDAQ

优先股等新金融工具如何建模？？
    preferred stocks



Notes:
1,行业类别除了计划在A股，港股，美股等不同市场股票类别下兼容分析，还计划在
股票和债券直接兼容分析，例如东方园林的股票和债券对应同一细分行业。

===============================================
'''

###################################################
class assets():
    def __init__(self,code=''):
        self.code = code 

###################################################
class stocks(assets): 
    # get from base class
    #
    def __init__(self):
        super(assets, self).__init__()


    # 
    def wind_api(self):
        # get,transform,analyzing and save data from Wind API 
        # derived from get_wind.py\class rC_DB
        def __init__(self):
            from db_assets.get_wind import wind_api
            self.wind_api = wind_api()

    def data_wash_funda(self):
        # derived from get_wind.py\class data_json_rc_head()
        import sys
        sys.path.append("..")
        from db.db_assets.stocks_funda_wash import funda_wash
        funda_wash = funda_wash()

        return funda_wash




###################################################
class stocks_cdr(stocks):
    def __init__(self):
        super(stocks, self).__init__()
        # CDR is a type of stock in Chinese market 
        # ref.:2018年11月2日，上海证券交易所正式发布以《上海证券交易所与
        # 伦敦证券交易所互联互通存托凭证上市交易暂行办法》为主的8项配套规则

        # exchange_status =0 means the cdr cannot be exchanged with its original 
        # stocks in other stock markets.
        self.exchange_status = 0
        self.owner_qualification = "market maker" # general participent or market maker.


###################################################
class stocks_us(stocks):
    # define stock in US markets
    def __init__(self):
        super(stocks, self).__init__()
        
        self.exchange_status = 0 





###################################################
class fixed_income(assets):
    # get from base class 
    def __init__(self):
        super(assets, self).__init__()



###################################################
class derivatives(assets):
    # get from base class 
    def __init__(self):
        super(assets, self).__init__()


class derivatives_FI(fixed_income) :
    # define bond futures, transferable bond,  ... 
    # 信用风险缓释合约(Credit Risk Mitigation Agreement,CRMA)
    # 信用风险缓释凭证（CreditRisk MitigationWarrant，CRMW
    # cds,swap,
    def __init__(self):
        super(fixed_income, self).__init__()    

class derivatives_stock(stocks) :
    # define index futures,stock option,warrant,
    def __init__(self):
        super(stocks, self).__init__()    

class derivatives_stock_warrant(derivatives_stock) :
    # define index futures,stock option,warrant,
    def __init__(self):
        super(derivatives_stock, self).__init__()    
        # 02318.HK 平安牛证，57831.HK, 溢价:0.78% 每手份数:5000
        # 成交价:0.067成交量:10万股行权价:71.20行权比例:100
        self.location = "CN"
        self.market = "CNHK"
        self.currency = "HKD"
        self.code = "57831.HK"
        self.code_anchor = "02318.HK"
        self.strike = 71.20
        self.callable_price = 71.8 
        self.balance = 700000  # remanning number of warrants in market 
        self.exercise_ratio = 0.01
        self.pct_callable_price = 0.0681
        self.type= 'bull'
        self.expiry_date = '20191014'

class derivatives_stock_option(derivatives_stock) :
    # define index futures,stock option,warrant,
    def __init__(self):
        super(derivatives_stock, self).__init__()    

class derivatives_commodities(assets) :
    # define index futures,stock option,warrant,
    def __init__(self):
        super(assets, self).__init__()    

class derivatives_forex(assets) :
    # define  
    def __init__(self):
        super(assets, self).__init__()    


###################################################
class funds(assets):
    # get from base class 
    def __init__(self):
        super(assets, self).__init__()









##TODOTODO#########################################
class data_operations():
    # deal with data operations:merge, join, concat ...
    def __init__(self):
        super(assets, self).__init__()

    # def data_merge_sources(self,asset,dates,items) :
    #     # status:todo 
    #     # merge financial items from multiple sources
    #     # case: get 2012-profit and 2013-2018 profit data from wind and dzh respectively 


    #     return 1
    #     