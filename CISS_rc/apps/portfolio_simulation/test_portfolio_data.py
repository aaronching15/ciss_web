# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
# 从给定文件夹获取基金定期披露信息，如前十大持仓等
last   | since 190917

MENU :
###  
###  
### 
 

'''
#################################################################################
### Initialization 
import sys
sys.path.append( "C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )

import pandas as pd 
import numpy as np 

# 6+6+4=16
columns = ["id" ]
columns = columns +["S_INFO_WINDCODE","F_PRT_ENDDATE","CRNCY_CODE","S_INFO_STOCKWINDCODE","F_PRT_STKVALUE","F_PRT_STKQUANTITY"]
columns = columns +["F_PRT_STKVALUETONAV","F_PRT_POSSTKVALUE","F_PRT_POSSTKQUANTITY","F_PRT_POSSTKTONAV","F_PRT_PASSTKEVALUE","F_PRT_PASSTKQUANTITY"]
columns = columns +["F_PRT_PASSTKTONAV","ANN_DATE","STOCK_PER","FLOAT_SHR_PER"]
columns = columns +["datetime","useless" ]
'''字段中文名 字段名 字段类型 
基金Wind代码 S_INFO_WINDCODE 
截止日期 F_PRT_ENDDATE
货币代码 CRNCY_CODE
持有股票Wind代码 S_INFO_STOCKWINDCODE
持有股票市值(元) F_PRT_STKVALUE
持有股票数量（股）F_PRT_STKQUANTITY

持有股票市值占基金净值比例(%) F_PRT_STKVALUETONAV
积极投资持有股票市值(元) F_PRT_POSSTKVALUE
积极投资持有股数（股）F_PRT_POSSTKQUANTITY
积极投资持有股票市值占净资产比例(%) F_PRT_POSSTKTONAV
指数投资持有股票市值(元) F_PRT_PASSTKEVALUE
指数投资持有股数（股）F_PRT_PASSTKQUANTITY

指数投资持有股票市值占净资产比例(%) F_PRT_PASSTKTONAV
公告日期 ANN_DATE
占股票市值比 STOCK_PER
占流通股本比例(%) FLOAT_SHR_PER
'''

# "S_INFO_WINDCODE", "F_PRT_ENDDATE","F_PRT_STKVALUE",  || "ANN_DATE",,"FLOAT_SHR_PER","datetime"
columns2 = ["S_INFO_STOCKWINDCODE","F_PRT_STKQUANTITY","F_PRT_STKVALUETONAV"]
columns2 = columns2 +[ "STOCK_PER"]

### Setting parameters 
list_years = [2005, 2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019 ]
list_mmdd= ["0331","0630","0930","1231" ]

file_path = "D:\\db_wind\\ChinaMutualFundStockPortfolio\\"
file_name = "WDS_F_PRT_ENDDATE_20060331_ALL_20190905.csv"

#################################################################################
### Given table name 
i= 0 

for temp_year in list_years :
	for temp_mmdd in list_mmdd :
		temp_date = str(temp_year) + temp_mmdd
		file_name = "WDS_F_PRT_ENDDATE_"+temp_date +"_ALL_20190905.csv"
		df_raw = pd.read_csv(file_path + file_name,index_col=0 )
		# print(df_raw.head(3) )

		#################################################################################
		### 给定基金代码  
		code_fund = "163402.SZ"
		# notes: 163402.OF 查不到，需要 163402.SZ
		df_raw2= df_raw[df_raw["1"]=="163402.SZ"]
		df_raw2.columns=columns
	 

		if len(df_raw2.index ) >0 :
			print("==========")
			print(df_raw2.iloc[0,:]  )
			if i == 0 :
				### 
				df_out = df_raw2 
				i= 1 
			else :
				df_out = df_out.append(df_raw2  ) 

			# 持有股票市值占基金净值比例(%) F_PRT_STKVALUETONAV
			file_name_out = "fund_stock_holdings_" + code_fund + ".csv"
			df_out.to_csv("D:\\" + file_name_out  )

































