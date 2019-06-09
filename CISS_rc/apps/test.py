
# import sys
# print( sys.path )

# sys.path.append("C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\")

# print( sys.path )

# from config.config_IO import config_IO


# 导入持仓数据，获取20070604的开盘价和收盘价


# path = "D:\\CISS_db\\port_rc1904_market_value_999\\accounts\\"


import sys
print( sys.path )

# sys.path.append("C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\")

# from db.data_io import data_wind
# dw0 = data_wind("")

# config_IO_0= { 'path_data':"D:\\data_Input_Wind\\"}
# code="000001.SZ"
# date_start = "2007-05-31"
# date_end ="2007-06-30"
# quote_type='CN_day'

# (code_head,code_df)= dw0.load_quotes(config_IO_0,code,date_start,date_end)

# print(code_head)

# print(code_df)


path = "D:\\CISS_db\\port_rc1904_market_growth_999\\accounts\\"
file = "id_account_1555232206_port_rc1904_market_growth_999_AS_20090519.csv"

import pandas as pd 
AS  = pd.read_csv( path +file,encoding="gbk" )
print( AS.info() )

AS2= AS[ AS["code"]=="600900.SH" ]
print( AS2 )
print(  len(AS2.index)   )
print(  len(AS2.index) <1  )

