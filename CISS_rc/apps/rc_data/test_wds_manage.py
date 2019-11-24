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
import sys
sys.path.append( "C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )

# from get_wind_wds import wind_wds
# wind_wds1 = wind_wds()
# ### Print all modules 
# wind_wds1.print_info()

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
### Import log_columns,tables,tables_columns, 
file_path_wds_manage = "I:\\zd_zxjtzq\\ciss_web\\CISS_rc\\apps\\rc_data\\"

file_log_columns ="log_data_wds_columns.csv"
file_tables ="log_data_wds_tables.csv"
file_tables_columns ="log_data_wds_tables_columns.csv"
file_data_check_anndates = "data_check_anndates.csv"

file_path_wds = "I:\\db_wind\\"
file_path_wds_log = "I:\\db_wind\\wds_log\\"

### 读取要跟踪的wds表格
df_tables = pd.read_csv( file_path_wds_manage+ file_tables,encoding="gbk"  )
print("df_tables \n" ,df_tables.head(5) )

### 下载交易日期，更新"data_check_anndates.csv" 表格 | 
### notes:all values in int type, 判断按最新更新日期算数据是否完整：1：已有；0：未下载；2：当日无数据；
# 暂时先用已有的交易日列表 
# 交易日：   rc_WDS_indexdates_200501_20190831_ANN-DT.csv | col="2"
# month:    rc_WDS_indexdates_200501_20190831_month.csv
# quarter : rc_WDS_indexdates_200501_20190831_quarter.csv
file_dates = "rc_WDS_indexdates_200501_20190831_ANN-DT.csv"
df_dates = pd.read_csv( file_path_wds+ file_dates   )
print(" df_dates \n", df_dates["2"].head(5) )

date_list = df_dates["2"]

###  
import os
if os.path.exists( file_path_wds_manage + file_data_check_anndates ) :
	df_data_check_anndates = pd.read_csv(file_path_wds_manage + file_data_check_anndates ,encoding="gbk" )
	# check if it contains all column in df_tables.name_table
	for temp_col in df_tables.name_table :
		if not temp_col in df_data_check_anndates.columns :
			### notes:all values in int type, 判断按最新更新日期算数据是否完整：1：已有；0：未下载；2：当日无数据；
			df_data_check_anndates[temp_col] = 0 
	# check if it contains all date(index) in df_tables.name_table
	for temp_index in date_list :
		if not temp_index in df_data_check_anndates.index :
			### notes:all values in int type, 判断按最新更新日期算数据是否完整：1：已有；0：未下载；2：当日无数据；
			df_data_check_anndates.loc[temp_index,:] = 0 
	
else :
	### 读取要跟踪的表格、日期，构建csv文件
	df_data_check_anndates = pd.DataFrame(columns=df_tables.name_table,index=date_list  )
	print( df_data_check_anndates.head(3) )
	print( df_data_check_anndates.tail(3) )
	df_data_check_anndates.to_csv( file_path_wds_manage + file_data_check_anndates ,encoding="gbk"  )

### Save table df_data_check_anndates into postgresql
### step 1 连接postgresql
import psycopg2
conn = psycopg2.connect(database="ciss_db", user="ciss_rc", password="ciss_rc", host="127.0.0.1", port="5432")
# 创建表格
cur = conn.cursor()  

###  新建create_engine：
# from sqlalchemy import create_engine
# engine = create_engine('postgresql://ciss_rc@localhost:5432/ciss_db')

# table_name = "data_check_anndates"
# df_data_check_anndates.to_sql( table_name, engine,index=False,if_exists='append')

### temp看看table是否成功新建
#编写Sql，只取前两行数据
table_name = "data_check_anndates"
sql = "select * from "+table_name+" limit 2"
 
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