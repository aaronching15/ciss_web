# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：
last update 181122 | since  181122
Menu :
1, 提前分行业把所有temp_list 算出来，避免下次再算。节省计算时间



Notes:  
===============================================
'''
import json
import pandas as pd 
import sys
sys.path.append("..") 

# from db.times import times
# time1 = times('CN','SSE')
# date_start = "20140531"
# date_end = "20150203"

# date_list = time1.get_port_rebalance_dates( date_start,date_end,'stock_index_csi'  )
# DatetimeIndex(['2014-05-31', '2014-11-30'], dtype='datetime64[ns]', freq=None)

#####################################################################
import time
start = time.clock()
import datetime as dt
### Import personal model engine 
from bin.abm_engine import Abm_model
from bin.engine_portfolio import Engine_ports
from db.times import times
times0 = times('CN','SSE')
method4time='stock_index_csi'

temp_date = "2014-05-31" # "2014-05-31"
temp_date2 = "2018-11-15" # "2014-11-30"
temp_date_now = times0.get_time_format('%Y%m%d','str')
# get date list with all pivot time point that we need to update stockpool,strategy and portfolio
# DatetimeIndex(['2014-05-31', '2014-11-30', '2015-05-31' ......
date_periods = times0.get_port_rebalance_dates(temp_date,temp_date2 ,method4time )
print("date_periods","   periods_reference_change ")
# [Timestamp('2014-05-31 00:00:00'), Timestamp('2014-11-30 00:00:00')]
print(date_periods.periods_reference_change )
print( date_periods.periods_start )
print( date_periods.periods_end )

# 601020， 351020  ,401010 | 251020-000333.SZ
# 302010 600519.SH  
path1= "C:\\zd_zxjtzq\\RC_trashes\\temp\\sys_stra_24h\\CISS_rc\\db\\db_assets\\"
file_name = "codelist_ind4.csv"

ind_level='1'

ind1_list_all = pd.read_csv(path1+file_name ,encoding="GBK")
ind1_list_all = ind1_list_all['ind'+ind_level+'_code'].drop_duplicates()
print( ind1_list_all)

abm_model = Abm_model()
engine_ports = Engine_ports()
# dataG = abm_model.load_symbol_universe('','' )
dataG = abm_model.load_symbol_universe('','' )
### Get historical fundamental financial and capital data
[df_tb_fi_fi, df_tb_fi_cap] =abm_model.get_histData_finance_capital()

for ind_level in ['2','3']: # "1"
	for temp_date in date_periods.periods_reference_change :
		# NOtes: calc_financial_estimates 中要求必须带 "-"
		temp_date = dt.datetime.strftime(temp_date,"%Y-%m-%d")
		# for int_ind_raw in ind1_list_all :  
		result = abm_model.calc_all_abm_weights( dataG, temp_date ,df_tb_fi_fi,ind_level ) 

		elapsed = (time.clock() - start)
		print("Time used:",elapsed)

elapsed = (time.clock() - start)
print("Total time used:",elapsed)