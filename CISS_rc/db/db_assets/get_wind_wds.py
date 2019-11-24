# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
todo: 

功能：
ip:10.10.10.195
port:1521
sid:wind
visit user:wind/wind321
ps:wind
data user:windn
self:
connection = cx_Oracle.connect("wind", "wind", "10.10.10.195:1521/wind")
from Yang： 
外部咨询数据库登录用户为wind,各咨询数据分放在不同用户下：
1。老版本的万得数据放在winddb用户下，访问方式如下：
    select * from winddb.tb_object_1090;--注老版本数据早已停止同步更新
2。新版本的万得数据放在windn用户下，访问方式如下：
    select * from windn.AShareEODPrices;
3。天象咨询数据放在txdb用户下，访问方式如下：
    select * from txdb.t_asset_sum;

derived from test_oracle_wind.py
数据来源： Wind-wds 万得落地数据库
last update   | since 190830
/ 
===============================================
'''
import pandas as pd
import numpy as np
import json
import time
import datetime as dt
import os


class wind_wds():
    # 类的初始化操作
    def __init__(self):
        
        #################################################################################
        ### Initialization 
        import cx_Oracle
        # Connect as user "hr" with password "welcome" to the "oraclepdb" service running on this computer.
        connection = cx_Oracle.connect("wind", "wind", "10.10.10.195:1521/wind")
        self.cursor = connection.cursor()
        self.path_out ="D:\\db_wind\\"
        self.path_log_table = "C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\"
        self.col_list= ["No.","table_name","prime_key","prime_key_value","datetime_range","last_update"]
        # 数据更新的日志表格
        self.log_table_name = "wind_wds_tables.csv"

    def print_info(self):
        ### print all modules for current clss
        print("data_fetch |get_table_full: 获取整张表格，无参数")
        print("data_fetch |get_table_primekey:根据主键primekey和日期范围等获取表格")
        
        print("data_fetch |get_table_full_input:get_table_full带参数版本")
        print("data_fetch |get_table_primekey_input:get_table_primekey带参数版本")

        print("data_manage |   ")
        return 1 

    def get_table_full(self ) :
        ### 获取整张表格，无参数
        ### last | sicne 190830
        ### derived from test_oracle_wind.py

        table_name = input("table_name: e,g.中国A股日行情[AShareEODPrices]...")
        prime_key =  "full_table"
        prime_key_value =  "full_table"
        datetime_range = "ALL" # input("datetime_range: e.g. ALL or [201907 to 201908]...")
        # input("last_update:e.g. 20180930 ...")
        last_update = dt.datetime.strftime(dt.datetime.now(),format="%Y%m%d") 
     
        print("Working on table name ", table_name)
        temp_sql = "SELECT * FROM windn." + table_name
        temp_table = self.cursor.execute(temp_sql   )
        temp_data = temp_table.fetchall()
         
        temp_df = pd.DataFrame( temp_data )

        print("Length of data ",len(temp_data) )
        print( temp_df.tail(2) ) 

        file_name = "WDS_"+ prime_key +"_"+ prime_key_value+"_" +datetime_range +"_"+ last_update + ".csv"
        
        if not os.path.isdir( self.path_out + table_name ) :
            os.mkdir( self.path_out + table_name )

        temp_df.to_csv(self.path_out+ table_name+ '\\' +file_name  )

        ### Find table name in list and Save to log of table 
        # log_table_name = "wind_wds_tables.xlsx"
        # # 不指定sheet时，默认读取第一个sheet
        # df_log_table = pd.read_excel(self.path_log_table+log_table_name, sheet_name="wind_wds_tables" )

        
        # 不指定sheet时，默认读取第一个sheet
        df_log_table = pd.read_csv(self.path_log_table+self.log_table_name )
        ## keep only specific columns
        df_log_table = df_log_table[ self.col_list ]

        ### 对于多个股票，需要对一个表格多次取值
        if len( df_log_table.index ) > 0 :
            temp_df = df_log_table[df_log_table["table_name"] == table_name  ]

        if len(temp_df.index) < 1 :
            # add to new row 
            temp_i = df_log_table.index[-1]+1
            
        elif len(temp_df.index) == 1 :
            temp_i = temp_df.index[0]
            print("Current item", temp_df )
        else:
            print("More than 1 row and should delete redundant rows...")
            print( temp_df )
            temp_i = temp_df.index[0]

        df_log_table.loc[ temp_i,"table_name" ] = table_name
        df_log_table.loc[ temp_i,"prime_key" ] = prime_key
        df_log_table.loc[ temp_i,"prime_key_value" ] = prime_key_value
        df_log_table.loc[ temp_i,"datetime_range" ] = datetime_range
        df_log_table.loc[ temp_i,"last_updatee" ] = last_update
        print(df_log_table.tail(3))
        # header=0) #不保存列名
        df_log_table.to_csv(self.path_log_table+self.log_table_name)
        print( self.path_log_table+ self.log_table_name )

        return temp_df,df_log_table

    def get_table_primekey(self ) :
        ### 根据主键primekey和日期范围等获取表格
        ### last | sicne 190830
        ### derived from test_oracle_wind.py

        # table_name = "AShareEODPrices"
        # prime_key = "S_INFO_WINDCODE"
        # prime_key_value = "600036.SH"
        # datetime_range = "ALL"

        table_name = input("table_name: e,g.中国A股日行情[AShareEODPrices]...")
        prime_key =  input("prime_key: e,g. S_INFO_WINDCODE or TRADE_DT...")    
        prime_key_value =  input("prime_key: e,g. 600036.SH or 20190829...")    
        datetime_range = "ALL" # input("datetime_range: e.g. ALL or [201907 to 201908]...")
        # input("last_update:e.g. 20180930 ...")
        last_update = dt.datetime.strftime(dt.datetime.now(),format="%Y%m%d") 

        ### Input parameters for database 
        # prime_key used to be date_name
        # prime_key used to be params = {'TEMP_DATE':20190807 }
        params = {'TEMP_DATE': prime_key_value }

        print("Working on table name ", table_name)
        
        # 中国A股日行情 || AShareEODPrices || 交易日期 TRADE_DT,20190805
        # 香港股票日行情|| HKshareEODPrices || TRADE_DT
        # table_name = "AshareMSCIMembers"
        # date_name = "ENTRY_DT" 
        temp_sql = "SELECT * FROM windn." + table_name +" WHERE "+ prime_key +  "=:temp_date "
        temp_table1 = self.cursor.execute(temp_sql,params )

        temp_data = temp_table1.fetchall()
         
        temp_df = pd.DataFrame( temp_data )

        print("Length of data ",len(temp_data) )
        print( temp_df.tail(2) ) 

        file_name = "WDS_"+ prime_key +"_"+ prime_key_value+"_" +datetime_range +"_"+ last_update + ".csv"
        
        if not os.path.isdir( self.path_out + table_name ) :
            os.mkdir( self.path_out + table_name )

        temp_df.to_csv(self.path_out+ table_name+ '\\' +file_name  )

        ### Find table name in list and Save to log of table 
        # log_table_name = "wind_wds_tables.xlsx"
        # # 不指定sheet时，默认读取第一个sheet
        # df_log_table = pd.read_excel(self.path_log_table+log_table_name, sheet_name="wind_wds_tables" )

        
        # 不指定sheet时，默认读取第一个sheet
        df_log_table = pd.read_csv(self.path_log_table+self.log_table_name )
        ## keep only specific columns
        df_log_table = df_log_table[ self.col_list ]

        ### 对于多个股票，需要对一个表格多次取值
        if len( df_log_table.index ) > 0 :
            temp_df = df_log_table[df_log_table["table_name"] == table_name  ]
            if len( temp_df.index ) > 0 :
                temp_df = temp_df[temp_df["prime_key"] == prime_key  ]
                temp_df = temp_df[temp_df["prime_key_value"] == prime_key_value  ]

        if len(temp_df.index) < 1 :
            # add to new row 
            temp_i = df_log_table.index[-1]+1
            
        elif len(temp_df.index) == 1 :
            temp_i = temp_df.index[0]
            print("Current item", temp_df )
        else:
            print("More than 1 row and should delete redundant rows...")
            print( temp_df )
            temp_i = temp_df.index[0]

        df_log_table.loc[ temp_i,"table_name" ] = table_name
        df_log_table.loc[ temp_i,"prime_key" ] = prime_key
        df_log_table.loc[ temp_i,"prime_key_value" ] = prime_key_value
        df_log_table.loc[ temp_i,"datetime_range" ] = datetime_range
        df_log_table.loc[ temp_i,"last_updatee" ] = last_update
        print(df_log_table.tail(3))
        # header=0) #不保存列名
        df_log_table.to_csv(self.path_log_table+self.log_table_name)
        print( self.path_log_table+ self.log_table_name )

        return temp_df,df_log_table

    def get_table_full_input(self,table_name ) :
        ### 获取整张表格，无参数
        ### last | sicne 190830
        ### derived from test_oracle_wind.py

        prime_key =  "full_table"
        prime_key_value =  "full_table"
        datetime_range = "ALL" # input("datetime_range: e.g. ALL or [201907 to 201908]...")
        # input("last_update:e.g. 20180930 ...")
        last_update = dt.datetime.strftime(dt.datetime.now(),format="%Y%m%d") 
     
        print("Working on table name ", table_name)
        temp_sql = "SELECT * FROM windn." + table_name
        temp_table = self.cursor.execute(temp_sql   )
        temp_data = temp_table.fetchall()
         
        temp_df = pd.DataFrame( temp_data )

        print("Length of data ",len(temp_data) )
        print( temp_df.tail(2) ) 

        file_name = "WDS_"+ prime_key +"_"+ prime_key_value+"_" +datetime_range +"_"+ last_update + ".csv"
        
        if not os.path.isdir( self.path_out + table_name ) :
            os.mkdir( self.path_out + table_name )

        temp_df.to_csv(self.path_out+ table_name+ '\\' +file_name  )

        ### Find table name in list and Save to log of table 
        # log_table_name = "wind_wds_tables.xlsx"
        # # 不指定sheet时，默认读取第一个sheet
        # df_log_table = pd.read_excel(self.path_log_table+log_table_name, sheet_name="wind_wds_tables" )

        
        # 不指定sheet时，默认读取第一个sheet
        df_log_table = pd.read_csv(self.path_log_table+self.log_table_name )
        ## keep only specific columns
        df_log_table = df_log_table[ self.col_list ]

        ### 对于多个股票，需要对一个表格多次取值
        if len( df_log_table.index ) > 0 :
            temp_df = df_log_table[df_log_table["table_name"] == table_name  ]

        if len(temp_df.index) < 1 :
            # add to new row 
            temp_i = df_log_table.index[-1]+1
            
        elif len(temp_df.index) == 1 :
            temp_i = temp_df.index[0]
            print("Current item", temp_df )
        else:
            print("More than 1 row and should delete redundant rows...")
            print( temp_df )
            temp_i = temp_df.index[0]

        df_log_table.loc[ temp_i,"table_name" ] = table_name
        df_log_table.loc[ temp_i,"prime_key" ] = prime_key
        df_log_table.loc[ temp_i,"prime_key_value" ] = prime_key_value
        df_log_table.loc[ temp_i,"datetime_range" ] = datetime_range
        df_log_table.loc[ temp_i,"last_updatee" ] = last_update
        print(df_log_table.tail(3))
        # header=0) #不保存列名
        df_log_table.to_csv(self.path_log_table+self.log_table_name)
        print( self.path_log_table+ self.log_table_name )

        return temp_df,df_log_table


    def get_table_primekey_input(self,table_name,prime_key ,prime_key_value ,datetime_range ) :
        ### 根据主键primekey和日期范围等获取表格
        ### last | sicne 190830
        ### derived from test_oracle_wind.py

        # table_name = "AShareEODPrices"
        # prime_key = "S_INFO_WINDCODE"
        # prime_key_value = "600036.SH"
        # datetime_range = "ALL"
        # input("last_update:e.g. 20180930 ...")
        last_update = dt.datetime.strftime(dt.datetime.now(),format="%Y%m%d") 

        ### Input parameters for database 
        # prime_key used to be date_name
        # prime_key used to be params = {'TEMP_DATE':20190807 }
        params = {'TEMP_DATE': prime_key_value }

        print("Working on table name ", table_name)
        
        # 中国A股日行情 || AShareEODPrices || 交易日期 TRADE_DT,20190805
        # 香港股票日行情|| HKshareEODPrices || TRADE_DT
        # table_name = "AshareMSCIMembers"
        # date_name = "ENTRY_DT" 
        temp_sql = "SELECT * FROM windn." + table_name +" WHERE "+ prime_key +  "=:temp_date "
        temp_table1 = self.cursor.execute(temp_sql,params )

        temp_data = temp_table1.fetchall()
         
        temp_df = pd.DataFrame( temp_data )

        print("Length of data ",len(temp_data) )
        print( temp_df.tail(2) ) 

        file_name = "WDS_"+ prime_key +"_"+ prime_key_value+"_" +datetime_range +"_"+ last_update + ".csv"
        
        if not os.path.isdir( self.path_out + table_name ) :
            os.mkdir( self.path_out + table_name )

        temp_df.to_csv(self.path_out+ table_name+ '\\' +file_name  )

        ### Find table name in list and Save to log of table 
        # log_table_name = "wind_wds_tables.xlsx"
        # # 不指定sheet时，默认读取第一个sheet
        # df_log_table = pd.read_excel(self.path_log_table+log_table_name, sheet_name="wind_wds_tables" )

        
        # 不指定sheet时，默认读取第一个sheet
        df_log_table = pd.read_csv(self.path_log_table+self.log_table_name )
        ## keep only specific columns
        df_log_table = df_log_table[ self.col_list ]

        ### 对于多个股票，需要对一个表格多次取值
        if len( df_log_table.index ) > 0 :
            temp_df = df_log_table[df_log_table["table_name"] == table_name  ]
            if len( temp_df.index ) > 0 :
                temp_df = temp_df[temp_df["prime_key"] == prime_key  ]
                temp_df = temp_df[temp_df["prime_key_value"] == prime_key_value  ]

        if len(temp_df.index) < 1 :
            # add to new row 
            temp_i = df_log_table.index[-1]+1
            
        elif len(temp_df.index) == 1 :
            temp_i = temp_df.index[0]
            print("Current item", temp_df )
        else:
            print("More than 1 row and should delete redundant rows...")
            print( temp_df )
            temp_i = temp_df.index[0]

        df_log_table.loc[ temp_i,"table_name" ] = table_name
        df_log_table.loc[ temp_i,"prime_key" ] = prime_key
        df_log_table.loc[ temp_i,"prime_key_value" ] = prime_key_value
        df_log_table.loc[ temp_i,"datetime_range" ] = datetime_range
        df_log_table.loc[ temp_i,"last_updatee" ] = last_update
        print(df_log_table.tail(3))
        # header=0) #不保存列名
        df_log_table.to_csv(self.path_log_table+self.log_table_name)
        print( self.path_log_table+ self.log_table_name )

        return temp_df,df_log_table








