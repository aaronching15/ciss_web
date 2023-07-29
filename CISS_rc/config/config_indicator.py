# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
todo

功能：配置策略参数
Function:configuration for indicator setting
 
last update 201024 | since 181110
Menu :
1,config_indicator:基础配置
2,config_indi_mom：动量相关的参数配置
3,config_indi_financial：财务相关指标
3.1,指标信息：
	1，file=wds_表格列匹配.xlsx ；path=C:\ciss_web\CISS_rc\apps\rc_data
	2，file=wds_table_column_financial_indicators.csv ;path=C:\ciss_web\CISS_rc\config
	3,导入wds数据表里和财务分析有关的指标
	4, 数据表依据：C:\ciss_web\CISS_rc\apps\rc_data\wds_表格列匹配.xlsx
4，class config_indi_estimate：卖方预期数据相关指标配置

Notes:
===============================================
'''
import os 
import pandas as pd 
### 导入数据配置信息,大部分配置地址都在config_data里
from config_data import config_data
config_data_1 = config_data()
### 包括所有相关的文件目录和位置，self.obj_config["dict"]["path_config"]

###################################################
class config_indicator():
	def __init__(self):
		self.obj_config = {}  
		self.obj_config["dict"] ={}
		self.obj_config["path_config"] = config_data_1.obj_config["dict"]["path_config"]

		self.path_config = config_data_1.obj_config["dict"]["path_config"]
		# 设定单一指标似乎没什么意义：indicator_name='rc001',indicator_id='id_rc001'
        # self.indicator_name = indicator_name
        # self.indicator_id = indicator_id

###################################################
class config_indi_mom(config_indicator):
    # 动量相关的参数配置
	def __init__(self ):
		### 继承父类config_data的定义，等价于
		config_indicator.__init__(self) 

		### 设置交易日的参数
		# 移动平均交易日的数量,大中小三个，分为短期short 和长期long参数
		self.obj_config["dict"]['para_ma_short'] =[16,40,100]
		self.obj_config["dict"]['para_ma_long'] =[40,100,250]
		# 价格相对于均线价格的偏离百分比 relative value of price over moving average price 
		self.obj_config["dict"]['para_p_ma']=[0.005, -0.005 , 0 ]
		# 均线和前值比增加的百分比幅度,一般就是1分钱的变动，例如10.23上涨到10.24，涨幅0.000977
		self.obj_config["dict"]['para_ma_up']=[0.00005,0.00002,0.00001] 

		### 设置交易周的参数

		### 设置交易月的参数

	
	############################################################################
	# def technical_ma(self):
	# 	# generate parameters for technical analysis
	# 	technical_ma={}
	# 	# period of moving averagee
	# 	technical_ma['ma_x'] =[3,8,16,40,100]
	# 	# relative value of price over moving average price 
	# 	technical_ma['p_ma']=[0,0,0,0,0]
	# 	# status of moving average 
	# 	technical_ma['ma_up']=[1,1,1,1,1] 
	# 	return technical_ma


###################################################
class config_indi_financial(config_indicator):
	'''
	定义财务指标：
	file=wds_表格列匹配.xlsx
	path=C:\zd_zxjtzq\ciss_web\CISS_rc\apps\rc_data
	df_col_fi_indi_CN_factor ：名称带factor是因为有匹配指标对应的因子
	'''	
    # 财务相关的参数配置
	def __init__(self ):
		### 继承父类config_data的定义，等价于
		config_indicator.__init__(self) 

		### 导入核心的wds原始财务指标
		self.obj_config["df_col_fi_indi_CN_factor"] = self.import_wds_table_columns_fi()
		self.obj_config["df_col_esti_indi_CN_factor"] = self.import_wds_table_columns_esti()
		self.obj_config["df_col_mathch_estimate"] = self.set_estimate_stat_match()
        

	def print_info(self):   
		print("import_wds_table_columns_fi |导入wds数据表里和财务分析有关的指标:A股财务类 ") 
		print("import_rc_table_columns_fi | 导入自建的财务分析有关指标 ") 
		print("import_wds_table_columns_esti | 导入wds数据表里和财务分析有关的指标:A股财务类 ")  
		print("set_estimate_stat_match | 设置预期明细值和近30/90/180日统计值的一一匹配")

		return 1

	def import_wds_table_columns_fi(self):
		### 导入wds数据表里和财务分析有关的指标:A股财务类
		'''导入表格的信息：
		table	表格名称
		no.	序号
		if_needed	是否需要
		type_financial	分类
		factor	因子
		weight_indicator	重要权重
		name_CN	name_CN
		name	name
		notes	notes
		file_prefix	期末数据文件前缀
		file_sufix	期末数据文件后缀

		数据表依据：C:\ciss_web\CISS_rc\apps\rc_data\wds_表格列匹配.xlsx
		'''
		file_name= "wds_table_column_financial_indicators.csv"
		# path=C:\ciss_web\CISS_rc\config 
		### df名称带factor是因为有匹配指标对应的因子
		try :
			df_col_fi_indi_CN_factor = pd.read_csv(self.obj_config["path_config"] + file_name  )
		except :
			df_col_fi_indi_CN_factor = pd.read_csv(self.obj_config["path_config"] + file_name ,encoding="gbk" )

		return df_col_fi_indi_CN_factor

	def import_rc_table_columns_fi(self):
		### 导入自建的财务分析有关指标
		df_col_fi_indi_CN_factor = ""

		return df_col_fi_indi_CN_factor

	def import_wds_table_columns_esti(self):
		### 导入wds数据表里和财务分析有关的指标:A股财务类
		'''导入表格的信息： 

		数据表依据：C:\ciss_web\CISS_rc\apps\rc_data\wds_表格列匹配.xlsx
		'''
		file_name= "wds_table_column_ConsensusData.csv"
		# path=C:\ciss_web\CISS_rc\config 
		### df名称带factor是因为有匹配指标对应的因子
		print(self.obj_config["path_config"] + file_name )
		try :
			df_col_esti_indi_CN_factor = pd.read_csv(self.obj_config["path_config"] + file_name  )
		except :
			df_col_esti_indi_CN_factor = pd.read_csv(self.obj_config["path_config"] + file_name ,encoding="gbk" )

		return df_col_esti_indi_CN_factor

	def set_estimate_stat_match(self) :
		### 设置预期明细值和近30/90/180日统计值的一一匹配
		'''
		预测市盈率	S_EST_PE
		数据来源：wds_表格列匹配.xlsx ；path=C:\zd_zxjtzq\ciss_web\CISS_rc\apps\rc_data
		notes:市盈率PE指标需要自己用预测净利润计算，
		''' 

		### TODO:把列匹配写入配置对象 df

		file_name= "multi_table_mathch_estimate.csv"
		# path=C:\ciss_web\CISS_rc\config 
		### df名称带factor是因为有匹配指标对应的因子
		print(self.obj_config["path_config"] + file_name )
		try :
			df_col_mathch_estimate = pd.read_csv(self.obj_config["path_config"] + file_name  )
		except :
			df_col_mathch_estimate = pd.read_csv(self.obj_config["path_config"] + file_name ,encoding="gbk" )

		return df_col_mathch_estimate








###################################################################################################
























