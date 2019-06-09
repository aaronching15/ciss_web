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

ind3_list_all = pd.read_csv(path1+file_name ,encoding="GBK")
ind3_list_all = ind3_list_all['ind3_code'].drop_duplicates()
print( ind3_list_all)
# int_ind3 = "302010" # "401010" # "601020" # Commertial banks 
				# sty_v_g = "growth" # "value" growth
init_cash = 300000000.0
##############################################################################
abm_model = Abm_model()
# dataG = abm_model.load_symbol_universe('','' )
dataG = abm_model.load_symbol_universe('','' )
### Get historical fundamental financial and capital data
[df_tb_fi_fi, df_tb_fi_cap] =abm_model.get_histData_finance_capital()

for int_ind3_raw in ind3_list_all : 
	int_ind3 = str(int_ind3_raw)
	if int_ind3 not in ["401020","302010","601020","302010"]:
		for sty_v_g in ["value","growth"]:
			start2 =  time.clock()
			try :
				name_id= "port_rc181119_" + sty_v_g + "_" + int_ind3
				 # generate name of portfolio
				port_name=  name_id
				# generate name of stockpool sp_name0= 'value_' + str(int_ind3) 
				# initial capital for portfolio simulation
				portfolio_manage,portfolio_suites= abm_model.test_abm_1port_Nperiods_ind3(sty_v_g,date_periods,int_ind3,port_name,init_cash,dataG,df_tb_fi_fi, df_tb_fi_cap,abm_model)
			except:
				pass
			elapsed = (time.clock() - start)
			print("Time used:",elapsed)

elapsed = (time.clock() - start)
print("Total time used:",elapsed)
# for temp_i in range( len(date_periods.periods_start) ) :
#	 date_reference_change= date_periods.periods_reference_change[temp_i]
#	 date_start = date_periods.periods_start[temp_i]
#	 date_end =  date_periods.periods_end[temp_i]
#	 # datetime to string 
#	 date_reference_change=  dt.datetime.strftime( date_periods.periods_reference_change[temp_i] ,"%Y-%m-%d" )
#	 date_start = dt.datetime.strftime( date_periods.periods_start[temp_i],"%Y-%m-%d" )
#	 date_end =  dt.datetime.strftime( date_periods.periods_end[temp_i],"%Y-%m-%d" )
	
#	 # suit only for one period
#	 if_period_1st = temp_i # 0 means the first period
#	 print("if_period_1st ",if_period_1st)











