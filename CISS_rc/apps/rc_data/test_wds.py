# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
# 中国A股日行情 || AShareEODPrices || 交易日期 TRADE_DT,20190805
# 香港股票日行情|| HKshareEODPrices || TRADE_DT
last 190830 | since 190730
'''
#################################################################################
### Initialization 
import sys
sys.path.append( "C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )
from get_wind_wds import wind_wds
wind_wds1 = wind_wds()
### Print all modules 
wind_wds1.print_info()

import pandas as pd 

#################################################################################
### Given table name 

# if_parameter = input("Choose full_table or with primekey: 0 or 1 ")
# if if_parameter == "0" :
# 	# ### 获取整张表格，无参数
# 	wind_wds1.get_table_full()
# elif if_parameter == "1" :
# 	### 根据主键primekey和日期范围等获取表格
# 	wind_wds1.get_table_primekey()
	 
# asd 
# ### todo: 根据代码列表，下载个股日行情数据

#################################################################################
### Step1:Get all stock symbol for Ashares 

### 按股票代码下载
# temp_path = "D:\\db_wind\\AShareDescription\\"
# temp_file = "WDS_full_table_full_table_ALL_20190830.csv"
# code_list =pd.read_csv(temp_path+ temp_file  )
# ### 第"9" 列是上市日期，A18087.SZ 这种未上市公司是空值
# # dropna(): axis=0: 删除包含缺失值的行 | axis=1: 删除包含缺失值的列
# # 	how: 与axis配合使用 how=‘any’ :只要有缺失值出现，就删除该行货列
# # subset: Define in which columns to look for missing values.
# code_list = code_list.dropna(subset=["9"],axis=0,how="any"  )
# print("We have "+ str(len(code_list.index))+ " stocks to fetch daily values"  )
# # print(code_list.head(3)  )

# i=0 
# print("中国A股日行情[AShareEODPrices] ")
# for temp_i in code_list.index :
# 	temp_code = code_list.loc[temp_i, "1" ]

# 	print("Working on "+str(i) +"th code "+ temp_code )
# 	# 中国A股日行情[AShareEODPrices]
# 	# 中国A股日行情估值指标[AShareEODDerivativeIndicator]
# 	# todo还要做一个每日更新维护的表；将字段改为 每个交易日保存一张表。

# 	table_name = "AShareEODDerivativeIndicator"
# 	prime_key  = "S_INFO_WINDCODE"
# 	prime_key_value = temp_code
# 	datetime_range = "ALL"

# 	(temp_df,df_log_table)= wind_wds1.get_table_primekey_input( table_name,prime_key ,prime_key_value ,datetime_range ) 

# 	i = i+1 

# asd
 

#################################################################################
### Step2:Get all Index symbol in SH or SZ for Ashares 
# Notes: MSCI的数据好像下载下来是空的
# 下载所有指数文件，包括了MSCI(3237个) 和上海、深圳交易所（709个，包括了主要中证指数，并且把h开头的没用的指数删了。）
# D:\db_wind\AIndexDescription

temp_path = "D:\\db_wind\\AIndexDescription\\"

### IMPORTANT CHOICE FOR SYMBOL LIST 
# temp_file = "WDS_full_table_full_table_ALL_20190902_rc_MSCI.csv"
temp_file = "WDS_full_table_full_table_ALL_20190902_rc_SHSZ.csv"

code_list =pd.read_csv(temp_path+ temp_file  )
### 第"9" 列是上市日期，A18087.SZ 这种未上市公司是空值
# dropna(): axis=0: 删除包含缺失值的行 | axis=1: 删除包含缺失值的列
# 	how: 与axis配合使用 how=‘any’ :只要有缺失值出现，就删除该行货列
# subset: Define in which columns to look for missing values.
code_list = code_list.dropna(subset=["1"],axis=0,how="any"  )
print("We have "+ str(len(code_list.index))+ " indexes to fetch daily values."  )
# print(code_list.head(3)  )

i=0 

table_name = "AIndexEODPrices"
prime_key  = "S_INFO_WINDCODE"
print( table_name ) 

### 按股票代码下载
for temp_i in code_list.index :
	temp_code = code_list.loc[temp_i, "1" ]

	print("Working on "+str(i) +"th code "+ temp_code )
	# 中国A股日行情[AShareEODPrices]
	# 中国A股日行情估值指标[AShareEODDerivativeIndicator]
	# todo还要做一个每日更新维护的表；将字段改为 每个交易日保存一张表。

	prime_key_value = temp_code
	datetime_range = "ALL"

	try :
		(temp_df,df_log_table)= wind_wds1.get_table_primekey_input( table_name,prime_key ,prime_key_value ,datetime_range ) 
	except:
		print( "Error code ", temp_code )
		pass
	i = i+1 
 







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