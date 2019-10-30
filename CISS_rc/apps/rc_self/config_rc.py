# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
===============================================
todo:

功能：1,包括了本地文件的配置信息

数据来源： rc
last update  | since  190808
/
class rC_Database( ) :
	def __init__(self, code ,start ,end ):
	def 

===============================================
'''
import pandas as pd
import numpy as np

class config_rc( ) :
	# 类的初始化操作
	def __init__(self, info=''  ):
		self.info = info
		# self.start=start
		# self.end=end


	def print_info(self ):
		print("Menu of class data_edb")

		print("config_pc_cs  | 返回cs对应台式机内文档和数据的目录  ")

		print(" | ")


		return 1 

	def config_pc_cs(self):
		### 返回对应台式机内文档和数据的目录
		dict_cs ={}
		#####################################################
		### CORE 
		### 个人资料目录
		dict_cs["path_self"] = "C:\\rC_self\\"
		### 历史搜集的研究报告
		dict_cs["path_self_reports_past"] = "C:\\rc_touyan\\"
		### 历史研究报告个人部分
		dict_cs["path_self_reports_past_rc"] = "C:\\zd_zxjtzq\rc_历史研究报告\\"
		### CISS系统目录
		dict_cs["path_sys"] = "C:\\zd_zxjtzq\\ciss_web\\"
		
		### 在cs的研究报告个人部分
		dict_cs["path_self_reports "] = "C:\\zd_zxjtzq\\rc_reports_cs\\"
		
		#####################################################
		### Data
		### CISS相关数据
		dict_cs["path_data_CISS"] = "D:\\CISS_db\\"

		### cs公司相关文件

		### wind行情数据
		dict_cs["path_wind_quotes"] = "D:\\data_Input_Wind\\"
		### wind行情数据-临时
		dict_cs["path_wind_quotes_temp"] = "D:\\temp\\"

		### wind落地数据库转存数据
		dict_cs["path_wind_db"] = "D:\\db_wind\\"

		### cs的ETF相关文件
		dict_cs["path_etf"] = "D:\\ETF\\"

		### open-source modules
		dict_cs["path_modules"] = "D:\\py_modules\\"

		### cs和卖方研究报告
		dict_cs["path_outside_reports"] = "D:\\TOUYAN\\" 


		return dict_cs

	def config_pc_home(self):
		### 返回对应台式机内文档和数据的目录
		dict_home ={}
		#####################################################
		### CORE 
		### 个人资料目录





		return dict_home
