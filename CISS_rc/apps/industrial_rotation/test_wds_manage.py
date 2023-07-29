# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
# 中国A股日行情 || AShareEODPrices || 交易日期 TRADE_DT,20190805
# 香港股票日行情|| HKshareEODPrices || TRADE_DT
last 191124 | since 191119 

MENU :

wds数据需求|具体整理见0wds_191120.xlsx  ：
1,app应用\\中证指数增强和BL数据表需求，201911：
1.1，原始数据表的需求：
	file_path= 
	file_name=
	例如：
	序号 | 数据需要 | 对应表-CN | 对应表 | 
	0，基础数据
		0.1,交易日：市场*资产
		0.2，证券代码表：
		0.N, 其他：
			中国A股科创板所属新兴产业分类[AShareSTIBEmergingIndustries]
		0.1，指数定期更新{全A，csi300，csi500等}}
		0.2，指数成分

	1，个股需求|根据指数成分 ：
		1.1，行情类
			1，股票日行情 | 中国A股日行情，AShareEODPrices
			2，股票日市值，换手，pe，pb | 中国A股日行情估值指标[AShareEODDerivativeIndicator]
			3，股票日交易，根据level2 | 中国A股资金流向数据[AShareMoneyFlow]
			4，股票月收益率 | 中国A股月收益率[AShareMonthlyYield]

		1.2，个股历史财务数据和指标 | 
			1，主要财务指标：roe,研发费用，r增长率等 | 中国A股公布重要财务指标[AShareANNFinancialIndicator]
			2，BS  | 中国A股资产负债表[AShareBalanceSheet]
			3，Bank indi.  | 中国A股银行专用指标[AShareBankIndicator]
			4, CF | 中国A股现金流量表[AShareCashFlow] | 
			5，财务指标 | 中国A股财务指标[AShareFinancialIndicator]
		1.5，股东、重大事件、预告
		1.N,其他：
			中国A股证券交易异动营业部买卖信息[AShareStrangeTrade]
			中国A股交易异动[AShareStrangeTradedetail]
			
	2，一致预期：
		2.1，盈利预测汇总
		2.2，盈利预测明细
	3，基金数据
		2.1，基金基础数据：品质、分类风格、净值-排名
		2.2，持仓数据

1.2，主要功能和对应的方程、数据：
   序号  |	功能方程 | 功能 | 数据 | 数据表
	1，载入数据日志表格，确定需要做的事情，json or csv
	2，下载和保存外部数据，下载给定区间的表格更新数据，
	3，读取外部数据，根据内部数据库形式转存，


'''
#################################################################################
### Initialization 
import os 
# 获取当前目录 os.getcwd() =: G:\zd_zxjtzq\ciss_web
import sys
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )

from get_wind_wds import wind_wds
wind_wds1 = wind_wds()
### Print all modules 
wind_wds1.print_info()

import pandas as pd 
import numpy as np 

#################################################################################
### data admin : main function 
'''
func-Download: 
	input  :获取需要更新的表格名称，关键字，更新的日期范围
	cal    : ~
	output :保存直接数据至特定表格，更新数据维护表格。
fundc-data_operation：
	input  ：按日期排序的表格
	cal    ：根据给定关键词或特定规则，计算需要的指标数据等中间数据表格
	output ：保存中间数据表格到特定文件夹。
last | since 191119
'''

#################################################################################
### get input: 导入需要维护的数据表格
## 先在家把要下载的数据表格设置好，根据网址 http://wds.wind.com.cn/rdf/?#/main
file_name = ""
file_path = ""



file_name = ""
file_path = ""
 
#################################################################################
### ciss_web\data\wds wind数据运维：| test_wds_manage.py
### 1.1，wds_raw --> df --> csv;

#################################################################################
### 1.2, csv --> df --> pgsql;
'''Steps:
1,初始化；2,输入需要更新的区间或最新日期；
3，读取目标文件夹内csv；
4，按交易日添加到数据库内表格，分基础和应用表{针对特定策略或组合优化过的表格}；

例子：
app，BL+abm+指数增强组合：
1，

Ana：
1，现有的对“wind_wds_tables.csv”的分析:
file_py= get_wind_wds.py |按关键词下载数据
def_py = __init__() | 初始化数据库连接和日志文件
def_py = get_table_primekey_input() | 这是主要用到的方程
file_py2= | 主要
path= “I:\zd_zxjtzq\ciss_web\CISS_rc\db\db_assets”
Qs：1，从wds读取数据时容易出现连接错误，中断的问题，需要过一段时间再次尝试。
2，数据维护记录如果按照2005年以来的交易日下载数据，一个表格就有

TODO：
1，admin：充新设计工作内容
2，wds数据日志：重新设计下载列
	2.1，新建csv数据文件data_check_anndates.csv，
	按发布日期anndate核对所有表格的完整性；
	取值：1：已有；0：未下载；2：当日无数据；
	历史数据完整性：可以接受不完整{估计难以避免}，但是需要有标识；
	2.2，

'''
#################################################################################
### Choice 1 定期维护下载 
'''
Qs: 如何分表格下载数据？

'''


#################################################################################
### Import log_columns,tables,tables_columns, 
### get current directory 
import os 
print(os.getcwd())
#file_path_wds_manage G:\zd_zxjtzq\ciss_web\CISS_rc\apps\rc_data\ 
file_path_wds_manage = os.getcwd() +"\\"

file_log_columns ="log_data_wds_columns.csv"
file_tables ="log_data_wds_tables.csv"
file_tables_columns ="log_data_wds_tables_columns.csv"

# todo 只下载A股日行情
sub_table = input("Type 1 if wants to download specific tables...")
if sub_table == "1" :
	file_data_check_anndates = "data_check_anndates_sub.csv"
else :
	file_data_check_anndates = "data_check_anndates.csv"

# path of date file
file_path_date = os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\apps\\rc_data\\"
# file_path_wds = "I:\\db_wind\\" or "H:\\db_wind\\"
file_path_wds = os.getcwd()[:2] + "\\db_wind\\"

file_path_wds_log = os.getcwd()[:2] + "\\db_wind\\wds_log\\"

### 读取要跟踪的wds表格
df_tables = pd.read_csv( file_path_wds_manage+ file_tables,encoding="gbk"  )
print("df_tables \n" ,df_tables.head(5) )

### 下载交易日期，更新"data_check_anndates.csv" 表格 | 
### notes:all values in int type, 判断按最新更新日期算数据是否完整：1：已有；0：未下载；2：当日无数据；
# os.getcwd() 对应 G:\zd_zxjtzq\ciss_web\CISS_rc\apps\rc_data


#################################################################################
### 下载 GlobalWorkingDay 整个表格，交易时间 || notes：三个月更新一次就行了，下一次20200331
'''
# path = "G:\db_wind\GlobalWorkingDay\\"
# file = "WDS_full_table_full_table_ALL_20191125.csv"
# 20191125抓取数据，获得的日期范围是
# USA	20180102	20191231 | HKG	20180102	20191231
# CHN	20180102	20201231
'''
if_update_tradingday = input("Choose 1 to update trading day for CHN market|suggest@20200331: ")
if if_update_tradingday == "1" :
	table_name = "GlobalWorkingDay"
	
	(df_date_2years ,df_log_table) =wind_wds1.get_table_full(table_name)

	### 导入本地 GlobalWorkingDay 表格
	path_workingday = file_path_wds + "GlobalWorkingDay"+"\\"
	# file_workingday = "WDS_full_table_full_table_ALL_"+ 20191125+".csv"
	import datetime as dt 
	last_update = dt.datetime.strftime(dt.datetime.now(),format="%Y%m%d") 
	file_workingday = "WDS_full_table_full_table_ALL_"+  last_update +".csv"
	# header=0，表示把第一行作为列名
	df_date_2years = pd.read_csv(path_workingday+file_workingday,encoding="gbk")
	df_date_2years = df_date_2years.drop("Unnamed: 0",axis=1)

	### 替换columns
	df_date_2years.columns=["id","date","market","update_date","notes"]
	# df_date_2years["date"] # int64 type
	df_date_2years= df_date_2years[df_date_2years["market"]=="CHN" ]
	df_date_2years = df_date_2years.sort_values(by="date" )

	### 用新获取的表格更新本地的交易日列表
	# 交易日：   rc_WDS_indexdates_200501_20190831_ANN-DT.csv | col="2"
	# month:    rc_WDS_indexdates_200501_20190831_month.csv
	# quarter : rc_WDS_indexdates_200501_20190831_quarter.csv
	file_dates = "rc_WDS_indexdates_20050101_anndate.csv"
	df_dates = pd.read_csv( file_path_wds + file_dates ,header=None  )
	# columns 只有"2",改为date 
	temp_col = df_dates.columns[0]
	df_dates = df_dates.sort_values(by= temp_col )
	date_last = df_dates[temp_col].values[-1]
	print(" date_last \n", int(date_last), type(df_date_2years["date"].values[-1] )  )

	df_date_2years = df_date_2years[ df_date_2years["date"]> int(date_last)  ]
	df_dates = df_dates[temp_col].append( df_date_2years["date"] ,ignore_index=True)
	# df_dates 是没有column的 pd series
	# print("df_dates \n" ,df_dates.head()  )
	# print("df_dates \n" ,df_dates.tail()  )
	### Save to csv file without index 
	###  index=False 意味着没有columns，再次导入时是 0 
	df_dates.to_csv(file_path_wds+ file_dates, index=False)
	print( file_path_wds+ file_dates )
	# df_dates 时间从 20050101 to 20201231
	######################################################################
	### 更新数据下载的核对表格
	file_check = "data_check_anndates.csv"
	df_check = pd.read_csv( file_path_date + file_check  )
	print("df_check \n", df_check.tail()  )
	date_last = df_check["date"].values[-1]

	print("date_last ", date_last )
	list_dates = list( df_dates )
	list_date_slice=  [i for i in  list_dates if i>int(date_last) ]
	list_date_slice=  [i for i in  list_date_slice if i<=int( last_update) ]
	print(" list_date_slice \n", list_date_slice )
	# [20191126, 20191127, 20191128, 20191129, 20191202, 20191203, 20191204, 20191205, 20191206]
	temp_index = df_check.index[-1 ]
	for temp_date in list_date_slice :
		temp_index = temp_index + 1 
		df_check.loc[temp_index, "date" ] = temp_date

	df_check["date" ].astype( np.int32 )
	df_check["date" ].astype( str )
	df_check.to_csv(file_path_date+ file_check, index=False) 
	

else :
	### just import local date list  
	file_dates = "rc_WDS_indexdates_20050101_anndate.csv"
	df_dates = pd.read_csv( file_path_wds+ file_dates ,header=None  )



######################################################################
### 用df_dates更新表格 data_check_anndates.csv  
###  获取data_check_anndates
import os
if os.path.exists( file_path_wds_manage + file_data_check_anndates ) :
	df_data_check_anndates = pd.read_csv(file_path_wds_manage + file_data_check_anndates ,encoding="gbk",index_col="date" )
	# check if it contains all column in df_tables.name_table
	for temp_col in df_tables.name_table :
		if not temp_col in df_data_check_anndates.columns :
			### notes:all values in int type, 判断按最新更新日期算数据是否完整：1：已有；0：未下载；2：当日无数据；
			df_data_check_anndates[temp_col] = 0 

else :
	### 读取要跟踪的表格、日期，构建csv文件
	df_data_check_anndates = pd.DataFrame(columns=df_tables.name_table,index= df_dates[temp_col]  )
	print( df_data_check_anndates.head(3) )
	print( df_data_check_anndates.tail(3) )
	df_data_check_anndates.to_csv( file_path_wds_manage + file_data_check_anndates ,encoding="gbk"  )

### 获取data_check_anndates的index里最新日期，获取和最新日期的差异，将对应rows添加进去。

date_1 = input("Type in latest date for wds data. such as 20191125: ")
print( df_data_check_anndates.head(2) )
date_0 = df_data_check_anndates.index[-1]
if date_0 < int(date_1) :
	### at this case we need to request data 
	print( "data_0,date_1",date_0,date_1 ) 
	# df_dates 是没有column的 pd series
	temp_list = df_dates[ df_dates > date_0  ]
	temp_list = temp_list[ temp_list <= int(date_1)  ]
	# print( temp_list ) | 20190903 to 20191125 
	# append temp_list to df_data_check_anndates
	# check if it contains all date(index) in df_tables.name_table
	for temp_date in temp_list :
		if not temp_date in df_data_check_anndates.index :
			### notes:all values in int type, 判断按最新
			# 更新日期算数据是否完整：1：已有；0：未下载；2：当日无数据；
			df_data_check_anndates.loc[temp_date,:] = 0 
	print("debug=== \n" )
	print( df_data_check_anndates.head(3) )
	print( df_data_check_anndates.tail(3) )
	

### Save table df_data_check_anndates into csv
df_data_check_anndates.to_csv( file_path_wds_manage + file_data_check_anndates ,encoding="gbk" )

#################################################################
### 按照 df_data_check_anndates 下载表格数据
### todo 191126  | 1：已有；0：未下载；2：当日无数据
for temp_col in df_data_check_anndates.columns :
	# temp_col correspondes to table name 
	### 获取列中数值不等于1或2的部分
	# pandas 通过~取反，选取不包含数字1的行 | https://blog.csdn.net/luocheng7430/article/details/80330566
	# 如果某列都是数值【1,2】，则以下这行会把所有列都删了
	# temp_df = df_data_check_anndates[~ df_data_check_anndates[temp_col].isin([1,2])  ]
	# new temp_df is a series
	temp_df = df_data_check_anndates[temp_col]	
	temp_df = temp_df[~ temp_df.isin([1,2])  ] 


	for temp_index in temp_df.index :  
		### temp_date should be like 20190103,which is in index but not value!!
		temp_date =  temp_index 
		# temp_col correspondes to date
		print("Working on table "+ temp_col +",date "+ str(temp_date) )
	
		table_name = temp_col
		###todo,需要去对应列表里找到
		temp_table = df_tables[ df_tables["name_table"] == table_name ]
	
		if len( temp_table.index) == 1:
			# prime_key  = "TRADE_DT"
			prime_key  = temp_table["keyword_anndate"].values[0]
			
			### 这里主要是公募基金描述没有关键字~~~ 
			# 其实可以用这个： 公告日期 F_INFO_ANNDATE  VARCHAR2(8)
			# http://wds.wind.com.cn/rdf/?#/main
			# TODOTODO

			### type(prime_key) = float 时，prime_key 会等于 nan
			if prime_key == np.nan :
				print("Debug===================")
				print( temp_table )

		else :
			print("Error: multiple matched tables ")
			print(temp_table)
		### 
		prime_key_value = str(temp_date)
		datetime_range = "ALL"

		(temp_table,temp_df_log_table)= wind_wds1.get_table_primekey_input( table_name,prime_key ,prime_key_value ,datetime_range ) 
		
		
		### 更新数值
		if len( temp_table.index ) >= 1 :
			### 表示我们有正常返回的数据
			df_data_check_anndates.loc[temp_index, temp_col] = 1 
		else :
			df_data_check_anndates.loc[temp_index, temp_col] = 2

		### 对于每个column更新的每个日期，都要及时存csv
		df_data_check_anndates.to_csv(file_path_wds_manage + file_data_check_anndates ,encoding="gbk"  )

print("We have updated all dates for tables. ")



asd

#################################################################
### Save table df_data_check_anndates into postgresql
### step 1 连接postgresql

import psycopg2
conn = psycopg2.connect(database="ciss_db", user="ciss_rc", password="ciss_rc", host="127.0.0.1", port="5432")
# 创建表格
cur = conn.cursor()  

###  新建create_engine：
from sqlalchemy import create_engine
# 例子：engine = create_engine('postgresql+psycopg2://scott:tiger@localhost/mydatabase')
# source：https://blog.csdn.net/P01114245/article/details/89918197
engine = create_engine('postgresql+psycopg2://ciss_rc@localhost:5432/ciss_db')

### Save to pgsql
table_name = "data_check_anndates" 

# if_exists : {‘fail’, ‘replace’, ‘append’}, default ‘fail’
# 下边这行会报错
print(df_data_check_anndates.head(2)  )
df_data_check_anndates.to_sql( table_name, engine,index=False,if_exists='append')

### temp看看table是否成功新建
#编写Sql，只取前两行数据
table_name = "data_check_anndates"
sql = "select * from "+table_name+" where date>20190825 limit 20"

#数据库中执行sql命令
cur.execute(sql)
 
#获得数据
data = cur.fetchall()
print("results from pgsql ")
print(data)


#################################################################################
### 1.3, pgsql --> settings.py -->urls.py -->views.py --> templates.html 


















#################################################################################
### 