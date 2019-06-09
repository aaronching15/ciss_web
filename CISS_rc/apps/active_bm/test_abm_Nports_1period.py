# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Abstract
we want to first run simulated portfolio test for at least 2 periods

Function: derived from test_avtive_bm.py and abm_engine.py
last update 181114 | since 181031

Menu :



 
Derived from test_abm_1port_Nperiods.py
===============================================
'''

##############################################################################
'''
20140531后第一个交易日T开始建仓，初始资金{1,5,10,50,100,500}亿元 existing wind data
1,导出组合数据，输出保存至文件
ports.py\\gen_port_suites
2,导入文件数据，用组合管理引擎分析，评估，更新组合状况。
    1，组合管理对象：ports.py\\class admin_portfolios():
    2，组合管理引擎：bin\\engine_portfolio.py
3，参数，配置等：abm模型的参数 config_apps_abm

'''
##############################################################################







import sys
sys.path.append("..")
# from db.data_io import data_wind
# multi-codes with multi-indicators is not supported
import pandas as pd 
int_ind3 = "401010"
temp_date = "2014-05-31"
# temp_df_growth= pd.read_csv("D:\\CISS_db\\rc001\\apps\\abm\\temp_df.csv")

### todo 根据带权重的symbol list，用gen_portfolio 模块，建立初始组合，采用不复权价格。

from db.stockpools import gen_stockpools
config= {}
from config.config_IO import config_IO

config_IO_0 = config_IO('config_name').load_config_IO_port(port_id,path_base,port_name)

### get data-api to get local data or wind data
# rC_Portfolio_17Q1\ line 325
# data.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amt', 'pct_chg']
# 导入指数和个股的周数据 | 下载指数和个股的周数据

# date_start = temp_date.replace("-","")   # '20181010' temp_date = "2014-05-31" temp_date2 = "2014-11-30"
# date_end = temp_date2.replace("-","")
date_start = "2014-05-31" 
date_end ="2014-11-30"
from db.data_io import data_wind
db_name_win=''
path0='D:\\db_wind\\'

import pandas as pd 
# sp_df_0=pd.DataFrame(['600036.SH','000001.SZ'],columns=['code'])
temp_df_growth = pd.read_csv("D:\\CISS_db\\rc001\\apps\\abm\\temp_df.csv")
int_ind3 = "401010"
## 下载stockpool里所有股票day,or week数据，
for temp_code in temp_df_growth['code'] :
    symbols = temp_code #  '600036.SH' 
    # multi-codes with multi-indicators is not supported 
    wd1 = data_wind(db_name_win ,path0 ).data_wind_wsd(symbols,date_start,date_end,'day')
    print('head  ')
    print(wd1.wind_head )
 
    # print(wd1.wind_df )
    # output wind object to json and csv file 
    file_json = wd1.wind_head['id']  +'.json'
    with open( config_IO_0['path_data']+ file_json ,'w') as f:
        json.dump( wd1.wind_head  ,f) 
    file_csv =  wd1.wind_head['id'] +'.csv'
    wd1.wind_df.to_csv(config_IO_0['path_data']+file_csv )


###############################################################################
# 181113 pseudo code for whole empirical case 

### generate system: sys1 base only on anchor stocks, sys2 base on anchor portfolio
## for every trading day, check if the portfolios have trades, using adj_dates
    # adj_dates is list of dates that anchor stock of industry might be changed for 
    # financial estimation or stock.[201405 - 201805]
### notes:don't forget to update portfolio_head after operation for accounts 
for int_ind1 in list_ind1:
    ### generate portfolio_ind1 using anchor stock/port_ind2 
    
    for int_ind2 in list_ind2 :
        ### generate portfolio_ind2 using anchor stock/port_ind3 

        for int_ind3 in list_ind3 :
            ### generate portfolio_ind3 using anchor stock

            ## period1: buy positions 

            ## period2: sell(and then buy) positions 
                # sell(+buy) 20% for 5 days 


### After loop, 
# 1,collect all unit from portfolios,compared with {industry index,equal-weights,
    # market-cap weighted}
# 2,strategy management scale, analyze efficiency of sub modules
# 3,build active market portfolio{growth,value}, 
    # compared with CSI300,CSI500,max{csi300,csi500}.....
    # merged from ind1~ind3 industry portfolios,









































