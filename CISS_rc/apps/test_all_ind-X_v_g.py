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
### 设置地区时间和交易所时间，如中国地区上海交易所
times0 = times('CN','SSE')
### 设置模拟组合的交易日时间，如基于中证规模指数 
method4time='stock_index_csi'

### Setting periods | 
# Notes: new periods starts from 20070531 for 000300.SH and 000905.SH
temp_date = "2007-05-31" # "2014-05-31"
# temp_date = "2007-05-25" # "2014-05-31"
# temp_date2 = "2018-11-15" # "2014-11-30"
temp_date2 =  "2018-11-15"# "2014-11-30"


temp_date_now = times0.get_time_format('%Y%m%d','str')
# get date list with all pivot time point that we need to update stockpool,strategy and portfolio
# DatetimeIndex(['2014-05-31', '2014-11-30', '2015-05-31' ......
### 设置模拟组合的交易日时间，如基于中证规模指数 
date_periods = times0.get_port_rebalance_dates(temp_date,temp_date2 ,method4time )
print("date_periods","   periods_reference_change ")
# [Timestamp('2014-05-31 00:00:00'), Timestamp('2014-11-30 00:00:00')]
print(date_periods.periods_reference_change )
print( date_periods.periods_start )
print( date_periods.periods_end )

# 601020， 351020  ,401010 | 251020-000333.SZ
# 302010 600519.SH  
path1= "C:\\zd_zxjtzq\\RC_trashes\\temp\\CISS_web\\CISS_rc\\db\\db_assets\\"
file_name = "codelist_ind4.csv"

# ind_level='1'
# # indX_list_all = pd.read_csv(path1+file_name ,encoding="GBK")
# # indX_list_all = indX_list_all['ind'+ind_level+'_code'].drop_duplicates()
# indX_list = pd.read_csv( path1+"ind"+ ind_level+ "_list.csv",header=None )
# print("indX_list ")
# print( indX_list )
# len_indX = len( indX_list )
# block_indX = len_indX/4

#######################################################
# ### for ind X and ask for input 
# # 1：might not finished yet! todo
# # 2：25420s ,3: 13100sec|4:16896s,5:27545, 6:7380 
# ind_level = str( input('Type in X for ind level ') )
# print("There are number of blocks to calculate for indX:" )
# print("total length, max_N, rest_number")
# print(len_indX, len_indX//4, len_indX%4 )
# input_ind1_list = str( input('Type in list of ind1 lot:1~N ') )
# index_0 = (int(input_ind1_list )-1 )*4
# index_1 = (int(input_ind1_list ) )*4
# if index_1 > len_indX :
# 	indX_list_all = indX_list.loc[index_0: ,1]
# else :	
# 	indX_list_all = indX_list.loc[index_0:index_1 ,1]

#######################################################
# ### for ind1 only and ask for input 
##1:14100s;2,
# if input_ind1_list == "1" :
# 	indX_list_all = ["10","40","50"]  # 3 能源 
# elif input_ind1_list == "2" :
# 	indX_list_all =  ["60","45"] # 4, 60 房地产
# 	# "60"里，存在 000024.SZ 退市导致的报错。
# elif input_ind1_list == "3" :
# 	indX_list_all = ["20","15","25" ] # 1 ,20 工业
# elif input_ind1_list == "4" :
# 	indX_list_all =["55","35","30"] # 2 ,55,公用事业
# elif input_ind1_list == "5" : # test case 
# 	indX_list_all =  ["10"]  

##############################################################################
### parameter initialization

init_cash = 300000000.0
abm_model = Abm_model()
engine_ports = Engine_ports()
# dataG = abm_model.load_symbol_universe('','' )
dataG = abm_model.load_symbol_universe('','' )
### Get historical fundamental financial and capital data
[df_tb_fi_fi, df_tb_fi_cap] =abm_model.get_histData_finance_capital()

##############################################################################
### Method 3 Working on industry list
# # int_ind3 = "302010" # "401010" # "601020" # Commertial banks 
# # sty_v_g = "growth" # "value" growth

# ind_level='1' # '1' '1' '2' '3'
# # "10","15","20","25","30","35","45","40","50","55","60" 
# indX_list_all =  ["40","50" ]  # "15" "25"
# for int_ind_raw in indX_list_all :  
# 	int_ind_x = str(int_ind_raw) 
# 	# for sty_v_g in ["value"]:
# 	for sty_v_g in ["value","growth"]:
# 		start2 =  time.clock()
# 		# try :
# 		name_id= "port_rc1904_market_" + sty_v_g + "_" + int_ind_x
# 		 # generate name of portfolio
# 		port_name=  name_id
# 		print("We are working at ", port_name )

# 		# generate name of stockpool sp_name0= 'value_' + str(int_ind3) 
# 		# initial capital for portfolio simulation
# 		portfolio_manage,portfolio_suites= engine_ports.test_abm_1port_Nperiods_ind_x(ind_level,sty_v_g,date_periods,int_ind_x,port_name,init_cash,dataG,df_tb_fi_fi, df_tb_fi_cap,abm_model)
# 		# except: 
# 		# 	pass

# 		elapsed = (time.clock() - start)
# 		print("Time used:",elapsed)


#######################################################
### Method 1 Working on the whole market portfolio | 
# 注意：由于是把11个行业等权重分配比例，因此距离理论上的全部股票按净利润比例分配还有差距！！！
# todo：应该按照list里所有股票净利润分配权重，这样的话，估计银行股会占大头，但是似乎也更符合逻辑？
# ind_level='0' means th whole market  

# ind_level='0' # '1' '1' '2' '3'
# int_ind_x = "999" # means the whole market 
# # sty_v_g = "value" 
# sty_v_g = "growth"

# name_id= "port_rc1904_market_" + sty_v_g + "_" + int_ind_x
# port_name=  name_id
# print("We are working at ", port_name )

#######################################################
### Method 2 Working on single industry portfolio | 

ind_level='0' # '1'  '2' '3'
# ["10","15","20","25","30","35","45","40","50","55""60" 
int_ind_x = "999" # means the whole market 
sty_v_g = "value"  
# sty_v_g = "growth"

name_id= "port_rc190416_market_" + sty_v_g + "_" + int_ind_x
port_name=  name_id
print("We are working at ", port_name )

#######################################################
### Generate and update portfolio for given periods

portfolio_manage,portfolio_suites= engine_ports.test_abm_1port_Nperiods_ind_x(ind_level,sty_v_g,date_periods,int_ind_x,port_name,init_cash,dataG,df_tb_fi_fi, df_tb_fi_cap,abm_model)
                                                
### update existing portfolio for given periods



elapsed = (time.clock() - start)
print("Total time used:",elapsed)


