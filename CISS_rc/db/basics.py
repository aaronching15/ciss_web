# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：
last update 181023 | since  181023
Menu :
class basics() : 
    初始化各类基础数据
class sector() : 初始化（股票）板块
class industry() :

THREE COMPONENTS:
1,class A:
    {INPUT,ALGO,OUTPUT  }
1,function a:
    {INPUT,ALGO,OUTPUT  }


Notes:
1,行业类别除了计划在A股，港股，美股等不同市场股票类别下兼容分析，还计划在
股票和债券直接兼容分析，例如东方园林的股票和债券对应同一细分行业。

===============================================
'''
import pandas as pd 

###################################################
class basics():
    def __init__(self,country='',market=''):
        self.country = country
        self.market = market

###################################################
class industry(basics): 
    # get from base class
    def __init__(self):
        super(basics, self).__init__()


    def load_GICS(self,input='' ):
        # GICS method for deviding industries equals to Wind default method 
        super(load_wind_ind, self).__init__()

        return output
    def load_wind_ind(self,input=''):
        # load wind industry columns for given symbol lists
        # input: path of ind. file, 
        '''
        length of ind_raw Length: 2256 , 181023
           wind_code sec_name ind4_index_code ind3_index_code ind2_index_code  \
        0  000001.SZ     平安银行       882493.WI       882241.WI       882115.WI   
        1  000002.SZ      万科A       882509.WI       882247.WI       882118.WI    
          ind1_index_code  ind4_code  ind3_code  ind2_code  ind1_code  
        0       882007.WI   40101010     401010       4010         40  
        1       882011.WI   60102030     601020       6010         60   
        '''
        path0 = "C:\\zd_zxjtzq\\RC_trashes\\temp\\sys_stra_24h\\CISS_rc\\db\\db_assets\\"
        filename = "codelist_ind4.csv"
        ind_raw = pd.read_csv(path0+filename,encoding="GBK" )
        
        return ind_raw 

###################################################
class time_admin(basics) :
    # setting time zones, trading periods, ...null=
    # get from base class
    def __init__(self):
        super(basics, self).__init__()

    def get_yymmdd(self,str_in='',dt_in='',  format_in='%Y%m%d') :
        ''' 
        # datetime  object from string 
        dt0 = datetime.strptime('2015-6-1 18:19:59', '%Y-%m-%d %H:%M:%S')
        # string from datetime object
        str_time = dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')
        '''
        import datetime as dt 
        if not str_in== '' :
            # datetime  object from string 
            dt_time = datetime.strptime(str_in,format_in)
            str_time = str_in
        if not dt_in == '':
            # string from datetime object
            str_time = dt.datetime.strftime(dt_in, format_in)
            dt_time = dt_in
        elif dt_in == 'now':
            str_time = dt.datetime.strftime(dt.datetime.now(), format_in)
            dt_time = dt_in

        return str_time,dt_time
    def get_time_stamp(self) :
        import datetime as dt 
        time_int = round( dt.datetime.now().timestamp() )



        return time_int 

    def get_time_zone(self,tzone) :
        # setting time zone for asset in different market 





        return output

    def get_trading_periods(self,market='CN') :
        # return trading time for different market, using market to get time zone 
        # 交易时间的设置




        return output









