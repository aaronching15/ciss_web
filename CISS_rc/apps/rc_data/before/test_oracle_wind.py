'''
ip:10.10.10.195
port:1521
sid:wind
visit user:wind/wind321
ps:wind
data user:windn
self:
connection = cx_Oracle.connect("wind", "wind", "10.10.10.195:1521/wind")

Zhang Xuchuan: 都是risk info？
connection = cx_Oracle.connect("nwind", "wind321", "10.10.10.195:1521/wind")

from Yang： 
外部咨询数据库登录用户为wind,各咨询数据分放在不同用户下：
1。老版本的万得数据放在winddb用户下，访问方式如下：
	select * from winddb.tb_object_1090;--注老版本数据早已停止同步更新
2。新版本的万得数据放在windn用户下，访问方式如下：
	select * from windn.AShareEODPrices;
3。天象咨询数据放在txdb用户下，访问方式如下：
	select * from txdb.t_asset_sum;

last 190830 | since 190730
'''
### Initialization 
import sys
import os
sys.path.append( os.getcwd()[:2] +"\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )
from get_wind_wds import wind_wds
wind_wds1 = wind_wds()
### Print all modules 
wind_wds1.print_info()

### 根据提示输入表格和主键获取数据。
if_parameter = input("Choose full_table or with primekey: 0 or 1 ")
if if_parameter == "1" :
	# ### 获取整张表格，无参数
	wind_wds1.get_table_full("")
elif if_parameter == "0" :
	### 根据主键primekey和日期范围等获取表格
	wind_wds1.get_table_primekey()
	





asd
### todo: 根据代码列表，下载个股日行情数据

#################################################################################
### Previous original scripts 

#################################################################################
### Initialization 
import cx_Oracle
# Connect as user "hr" with password "welcome" to the "oraclepdb" service running on this computer.
connection = cx_Oracle.connect("wind", "wind", "10.10.10.195:1521/wind")
cursor = connection.cursor()

import datetime as dt 
import os
import pandas as pd
path_out =os.getcwd()[:2] +"\\db_wind\\"
log_table_path = os.getcwd()[:2] +"\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\"
col_list= ["No.","table_name","prime_key","prime_key_value","datetime_range","last_update"]

### Input parameters from users 
print("======(TYPE IN)PARAMETERS FOR TABLE========")
# 中国A股基本资料[AShareDescription] | 中国A股日行情[AShareEODPrices]
#  中国A股日行情估值指标[AShareEODDerivativeIndicator] 
if_parameter = input("Type in if we have parameter for this sql: 0 or 1 ")

if if_parameter == "1" :
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


	#################################################################################
	### CASE: with paras
	# 中国A股日行情 || AShareEODPrices || 交易日期 TRADE_DT,20190805
	# 香港股票日行情|| HKshareEODPrices || TRADE_DT

	# table_name = "AshareMSCIMembers"
	# date_name = "ENTRY_DT" 
	temp_sql = "SELECT * FROM windn." + table_name +" WHERE "+ prime_key +  "=:temp_date "
	temp_table1 = cursor.execute(temp_sql,params )
		
	temp_data1 = temp_table1.fetchall()
	# len( temp_df1[1].drop_duplicates()  ) = 3293

	temp_df1 = pd.DataFrame( temp_data1 )

	print(len(temp_data1) )
	print( temp_df1.tail(2) ) 

	file_name = "WDS_"+ prime_key +"_"+ prime_key_value+"_" +datetime_range +"_"+ last_update + ".csv"

	if not os.path.isdir( path_out + table_name ) :
		os.mkdir( path_out + table_name )

	temp_df1.to_csv(path_out+ table_name+ '\\' +file_name  )

	### Find table name in list and Save to log of table  
	# log_table_name = "wind_wds_tables.xlsx"
	# # 不指定sheet时，默认读取第一个sheet
	# df_log_table = pd.read_excel(log_table_path+log_table_name, sheet_name="wind_wds_tables" )

	log_table_name = "wind_wds_tables.csv"
	# 不指定sheet时，默认读取第一个sheet
	
	df_log_table = pd.read_csv(log_table_path+log_table_name )
	## keep only specific columns
	df_log_table = df_log_table[ col_list ]
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

	df_log_table.to_csv(log_table_path+log_table_name )  
	print( log_table_path+log_table_name )
	### 若1个交易日数据合计10mb，10年数据约 10*10*252=24.6GB

elif if_parameter == "0" :
	#################################################################################
	### 抓取整张表格
	# source:https://blog.csdn.net/qq_41797451/article/details/80230399
	# Wind一致预测指数指标[AIndexConsensusData];执行SQL语句;20171105至今共970w条数据，20190804一天9807条数据；
	### CASE: no paras
	# 中国A股MSCI成份股[AshareMSCIMembers] || 	Wind代码 S_INFO_WINDCODE or 纳入日期 ENTRY_DT 
	
	table_name = input("table_name: e,g.中国A股日行情[AShareEODPrices]...")
	prime_key =  "full_table"
	prime_key_value =  "full_table"
	datetime_range = "ALL" # input("datetime_range: e.g. ALL or [201907 to 201908]...")
	# input("last_update:e.g. 20180930 ...")
	last_update = dt.datetime.strftime(dt.datetime.now(),format="%Y%m%d") 
 
	print("Working on table name ", table_name)
	temp_sql = "SELECT * FROM windn." + table_name
	temp_table = cursor.execute(temp_sql   )
	temp_data = temp_table.fetchall()
	 
	temp_df = pd.DataFrame( temp_data )

	print("Length of data ",len(temp_data) )
	print( temp_df.tail(2) ) 

	file_name = "WDS_"+ prime_key +"_"+ prime_key_value+"_" +datetime_range +"_"+ last_update + ".csv"
	
	if not os.path.isdir( path_out + table_name ) :
		os.mkdir( path_out + table_name )

	temp_df.to_csv(path_out+ table_name+ '\\' +file_name  )

	### Find table name in list and Save to log of table 
	# log_table_name = "wind_wds_tables.xlsx"
	# # 不指定sheet时，默认读取第一个sheet
	# df_log_table = pd.read_excel(log_table_path+log_table_name, sheet_name="wind_wds_tables" )

	log_table_name = "wind_wds_tables.csv"
	# 不指定sheet时，默认读取第一个sheet
	df_log_table = pd.read_csv(log_table_path+log_table_name )
	## keep only specific columns
	df_log_table = df_log_table[ col_list ]

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
	df_log_table.to_csv(log_table_path+log_table_name)
	print( log_table_path+log_table_name )




asd





#################################################################################
'''
<h4>Wind本地数据抓取 </h4>
<h5>下载策略： </h5>
<li>不了解数据大小的表，先下载单日/周/月/年的数据，根据情况补全历史数据。</li>
<li>website :http://wds.wind.com.cn/rdf/?#/main </li>
<li>由于每张表表名都是唯一的，因此基于{table_name,columns}就可以定期抓取表格</li>
<li>### 若1个交易日数据合计10mb，10年数据约 10*10*252=24.6GB</li>
<li>数据都下载后，还要建立对应的索引，方便未来策略快速抓取。  </li>

<h5>WDS网站上所有买的表格内容分析 </h5>
<p>
1，维护本地数据表结构信息；
    将主表{key:value}、具体表格放在excel文件内。
    file_name= wind_wds_tables.xlsx
    file_path= C:\zd_zxjtzq\ciss_web\CISS_rc\db\db_assets
2，设置分月、季、年下载历史数据
3，按日下载数据


我们需要的数据：
1，week||0101_全部A股基础信息：代码和名称列表、所属行业和概念，交易日数据，当日日终
2，day||0102_交易日行情，除权除息数据，交易异动，停复牌，level2指标，当日日终
    0105_权益数据\\股本，分红，事件日期，自由流通股本，配股，增发，
3，day||0104_融资融券数据，次日早上8:00
4，day||0106_财务数据\\重要财务指标，BS,CF,income,indicator,indicator_derivative,date_issuing披露日期,业绩快报和预告！！，调整后财务指标-净资产，TTM与MQR，
    bank_indicator,securities_indi,insurance_indi,银行5级分类贷款，银行存款结构和贷款结构

5，week||01_07_财务附注：财务附注明细（这个比较全）、
    大股东欠款、预付账款、应付薪酬、资产减值准备、应付职工薪酬、财务费用明细，   坏账提取准备、研发支出、主营构成、税费和税率、应收账款前5，长期借款前5，营业收入前5.
6，week||01_08_公司治理：员工持股计划，股票买卖，员工激励计划，管理层，管理层持股和报酬，员工人数，员工变更、
7，week||01_09_股东数据：股东拟增减持、质押、高管增减持、重要股东增减持，
8，week||01_11_重大事件：A股证券投资、资本运作、控股参股、购买理财、担保事件、股东大会、参股公司经营情况(有助于分析参股公司部分的现金流是否可控)、重大事件汇总、要约收购、违规事件、运营事件(中标工程项目等)、诉讼、重大重组事件{资产总额、营业收入、净资产}、
11，week||01_11_并购重组：盈利承诺明细、承诺汇总、A股并购事件{标的、日期、估值等 }
12，week||01_12_机构调研：参与主体，问答明细
14，week||01_14_并购重组：股票风格{成长、价值、市值、营收增长率、净利润增长率、市值系数}{大中小市值的划分是【0.75,0.9】}{计算价值、混合、成长各自的Z分值，按1/3分三部分 }
15，week||01_15_指数数据：指数基本资料、日行情、指数成分股{纳入、剔除日期，最新标志；190806共52w条记录}、Wind指数成分
16，week||01_16，指数财务衍生指标、指数估值数据、行情衍生指标
98，week||01_98，第三方数据（中证指数）：备选成分股名单{300和905的备选池各有10只左右；}、100、500、700、800、1000、A股中证行业成分明细、MSCI成分股、红利指数。
======
2.0, 02_中国A股一致预测
2.1，02_01,A股Wind一致预测:预测指标、预测滚动指标、盈利预测汇总、个股指标、个股滚动指标。
    last_update,20190806{部分}
2.2，02_02,盈利预测明细：盈利预测明细{个股}、行业投资评级{报告分类及作者信息等}
======
3.0,债券数据库
3.15，03_15,中国债券指数数据，CBIndex：基本资料
    last_update,20190805
4.0,共同基金
4.1，04_01,基础信息：基金基本资料、基金经理、Wind基金分类
    last_update,20190805
4.4，04_04,市场表现： 上市基金日行情、共同基金净值、交易量和佣金、基金份额、基金经理业绩表现、基金业绩表现、
    last_update,20190805
4.07，投资组合：组合重大变动、资产配置、持券明细、行业配置、持股明细、其他证券、货币基金剩余期限
    last_update,20190805
4.08，共同基金第三方评级
4.09,共同基金-公允价值变动收益。
4.13，14，中港互认基金
4.16，QFII--只有额度
4.17，QDII：行业配置、地区配置、持券配置
4.18，基金指数：年跟踪基准指数偏离度、基金指数成分明细、指数基本资料、指数行情
5.0，期货数据
5.05，股指期货
5.06，国债期货

7，港股股票
7.01，上市公司基本资料{无wind代码}、股票基本资料{有wind代码}、面值及交易单位、代码变更表、AH关联证券、
7.03，行业数据：港股通成分股、wind-GICS行业分类
7.04，行情交易数据：港股交易日历、日行情、卖空成交量、停复牌信息、陆股通通道持股数量、
7.05，行情衍生：日行情估值指标
7.06，股本结构、自由流通股本
7.98，第三方价格：外汇交易行情、港股行业分类、恒生指数分类
7.07，权益事件
7.08，港股财务报表GSD
7。13，港股ETF日行情
7.14，指数数据：成分股、日行情

======
idea：1，对于抓取的数据表，先下载单日数据，根据大小评估是否一次性抓取；
2，始终对财务数据可靠性进行概率打分
============
source https://docs.quandl.com/docs/data-organization
quandl数据接口
1，End of Day US Stock Prices


</p>

'''
# # temp_table = cursor.execute("SELECT * FROM windn.AIndexMembers" )
'''
Wind一致预测指数指标[AIndexConsensusData]
# cursor.execute("SELECT S_INFO_WINDCODE,EST_D
>>> temp_table.description
[('OBJECT_ID', <class 'cx_Oracle.STRING'>, 38, 38, None, None, 0),
('S_INFO_WINDCODE', <class 'cx_Oracle.STRING'>, 40, 40, None, None, 1), 
('EST_DT', <class 'cx_Oracle.STRING'>, 8, 8, None, None, 1), 
('EST_REPORT_DT', <class 'cx_Oracle.STRING'>, 8, 8, None, None, 1), 
('NET_PROFIT', <class 'cx_Oracle.NUMBER'>, 26, None, 20, 4, 1), 
('EST_EPS', <class 'cx_Oracle.NUMBER'>, 26, None, 20, 4, 1), 
('EST_PE', <class 'cx_Oracle.NUMBER'>, 26, None, 20, 4, 1), 
('EST_PEG', <class 'cx_Oracle.NUMBER'>, 26, None, 20, 4, 1), 
('EST_PB', <class 'cx_Oracle.NUMBER'>, 26, None, 20, 4, 1), 
('EST_ROE', <class 'cx_Oracle.NUMBER'>, 26, None, 20, 4, 1), 
('EST_OPER_REVENUE', <class 'cx_Oracle.NUMBER'>, 26, None, 20, 4, 1), 
('EST_CFPS', <class 'cx_Oracle.NUMBER'>, 26, None, 20, 4, 1), 
('EST_DPS', <class 'cx_Oracle.NUMBER'>, 26, None, 20, 4, 1), 
('EST_BPS', <class 'cx_Oracle.NUMBER'>, 26, None, 20, 4, 1), 
('EST_EBIT', <class 'cx_Oracle.NUMBER'>, 26, None, 20, 4, 1), 
('EST_EBITDA', <class 'cx_Oracle.NUMBER'>, 26, None, 20, 4, 1), 
('EST_TOTAL_PROFIT', <class 'cx_Oracle.NUMBER'>, 26, None, 20, 4, 1), 
('EST_OPER_PROFIT', <class 'cx_Oracle.NUMBER'>, 26, None, 20, 4, 1), 
('EST_OPER_COST', <class 'cx_Oracle.NUMBER'>, 26, None, 20, 4, 1), 
('EST_SHR', <class 'cx_Oracle.NUMBER'>, 26, None, 20, 4, 1), 
('TYPE1', <class 'cx_Oracle.STRING'>, 10, 10, None, None, 1), 
('OPDATE', <class 'cx_Oracle.DATETIME'>, 23, None, None, None, 1), 
('OPMODE', <class 'cx_Oracle.STRING'>, 1, 1, None, None, 1)]

时间上，【20171115，20190805】
length：9755514


'''