# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
需求：
建立ETF组合日常管理的仿真脚本

last 19 || since 190705

Function:功能：
1, pcf_manage:读取申购赎回清单，保存成数据对象
2，


todo:

Notes:
##############################################
'''

##################################################################
### Initialization
import json
import pandas as pd 
import numpy as np 
import math

from etf.engine_etf import ETF_manage

etf_manage0 = ETF_manage()
path_etf = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\apps\\black_litterman\\etf\\"
name_etf = "510300"
date_init = "0704"
df_head,df_stocks = etf_manage0.get_pcf_file(date_init,name_etf,path_etf )
df_head.index = df_head.key
print("df_head ", df_head)
print( df_head.loc["TradingDay","value"] )
print("Head of df_stocks \n", df_stocks.head() )


####################################################################################
### Part 模拟组合初始化，使用给定日期的PCF
####################################################################################

### generate portfolio using pcf or index constitutes
# 2种新建组合方式：1，从pcf文件新建；2，从指数成分新建
# type_gen_etf in {  }
'''todo list 
1, 新建组合相关文件
    1.1,head file object and path
    1.2,
'''

port_name = "etf_csi300_01"
### Notes：和之前不一样的是现在新的参数设置可以放在config_port里。 
config_port = {}
config_port["bench_name"] = "CSI300"
config_port["bench_code"] = "000300.SH"
config_port["init_cash"] = 1000000000

config_port["date_init"]  = "20190704"
config_port["date_start"] = "20190704"
config_port["date_end"]  = "20190705"

config_port["type_gen_etf"] = "pcf"
config_port["portfolio_type"] = "etf"

portfolio_manage,portfolio_suites = etf_manage0.gen_port(port_name,config_port,df_head,df_stocks)





##################################################################
### Initialize common configurations and variables
from ..db.ports import gen_portfolios,manage_portfolios
### todo line 366 ;engine_portfolio.py 






##############################################################################
### Portfolio simulation using CISS standarded modules.
config_port ={}
















####################################################################################
### Part 
####################################################################################
'''todo list:
1, data collect:{指数成分，分红送配}

'''




































