# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
功能：实现不同参数的wds数据获取功能

todo
1，必须在下载时就给table对应好列名columns
2，解决部分更新日期数据是float格式导致 "20191202.0"的情况
# 中国A股日行情 || AShareEODPrices || 交易日期 TRADE_DT,20190805
# 香港股票日行情|| HKshareEODPrices || TRADE_DT
last 20201019 | since 20191119 

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
		0.2，证券代码表：中国A股基本资料[AShareDescription]
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
	4，指数数据
	4.1，AIndexDescription


1.2，主要功能和对应的方程、数据：
   序号  |	功能方程 | 功能 | 数据 | 数据表
	1，载入数据日志表格，确定需要做的事情，json or csv
	2，下载和保存外部数据，下载给定区间的表格更新数据，
	3，读取外部数据，根据内部数据库形式转存，

功能规划：
csv --> df --> pgsql;
Steps:
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

notes：
必须在下载时就给table对应好列名columns

'''


#################################################################################
### Initialization 
import os 
# 获取当前目录 os.getcwd() =: G:\zd_zxjtzq\ciss_web
import sys
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )
sys.path.append(os.getcwd()[:2] + "\\ciss_web\\CISS_rc\\db\\db_assets\\" )

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
###

count  =0 
while count < 100 :
	get_wds_type = input("Type of data method:\n1 for given keyword \n2 for full hist. table\n3 for opdate periods \n4 for columns ofspecific table\n ")
#################################################################################
### Choice 1 下载特定表格-给定关键词
# 中国A股指数日行情[AIndexEODPrices]
# 关键词：Wind代码 S_INFO_WINDCODE 000300.SH
# or:  ChinaMutualFundStockPortfolio || F_PRT_ENDDATE

	if get_wds_type == "1" :
		print("AShareConsensusRollingData;EST_DT;AShareProfitNotice;公告日期S_PROFITNOTICE_DATE:  ")
		(temp_df,df_log_table)= wind_wds1.get_table_primekey()
	
	

#############################################################################
### Choice 2 下载多个完整表格-一次性 || Wind兼容代码[WindCustomCode]
	if  get_wds_type == "2" :

		if_CN= 1 # 决定导出数据时是否要用 gbk格式
		table_name = input("Type in table name for whole history,such as:ChinaMutualFundNAV: ")
		# result =wind_wds1.get_table_full(table_name,if_CN) 
		result =wind_wds1.get_table_full(table_name) 

#############################################################################
### Choice 3 按opdate下载区间表格
	if  get_wds_type == "3" :

		obj_in={}
		obj_in["dict"]={}
		obj_in["dict"]["table_name"] =  input("Type in table name，such as 中国A股盈利预测明细[AShareEarningEst]:  ")
		# "datetime_key"有3种选择："OPDATE" ，"TRADE_DT"，
		obj_in["dict"]["datetime_key"] = input("Type in keyword name,such as OPDATE,TRADE_DT,ANN_DT,EST_DT:  ")
		
		obj_in["dict"]["df2csv"] = input("Type in 1 if want to save opdate period data file and 0 else :  ")  
		# if df2csv == "1" :
		# 	obj_in["dict"]["df2csv"] = 1
		# else :

		### sub Choice 1 单一时间区间 
		obj_in["dict"]["datetime_value_lb"] = input("Type in date start,such as 20200801:  ")
		obj_in["dict"]["datetime_value_ub"] = input("Type in date end  ,such as 20201019:  ")
		obj_in["dict"]["if_opdate_2_prime_key"] = input("Type 1 if save opdate to prime_key files :")
		
		obj_in =wind_wds1.get_table_opdate(obj_in )

		### sub Choice 2 多年时间区间,
		# for year in range(2000,2021) :
		# 	obj_in["dict"]["datetime_value_lb"] = str(year) +"0101"
		# 	obj_in["dict"]["datetime_value_ub"] =  str(year) +"1231"
		# 	print("Debug=== ",obj_in["dict"]["datetime_value_lb"], obj_in["dict"]["datetime_value_ub"])
		# 	obj_in =wind_wds1.get_table_opdate(obj_in )


#################################################################################
### 只获取表格的列信息 |输入特定表格和关键词
# if_opdate_2_prime_key=1 意味着要转存成csv文件，0意味着不需要

	if  get_wds_type == "4" :
		temp_cols = wind_wds1.get_table_columns() 
		print(temp_cols )
	
	### 
	count = count+1 


asd



#################################################################################
### Choice 1 定期维护下载wds的数据表格
'''1,下载交易日期，更新交易日rc_WDS_indexdates_20050101_anndate.csv表格  
2,获取最新交易日和需要跟踪的tables，用df_dates更新表格 data_check_anndates.csv 
3,按照 df_data_check_anndates内的更新记录下载表格数据；cell values:1：已有；0：未下载；2：当日无数据
'''
sub_table = input("Type 1 if wants to download specific tables...")
if sub_table == "1" :
	file_data_check_anndates = "data_check_anndates_specific.csv"
else :
	file_data_check_anndates = "data_check_anndates.csv"

result = wind_wds1.manage_data_check_anndates(file_data_check_anndates )

asd



#################################################################
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