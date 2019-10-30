# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
===============================================
todo:

功能： 
数据来源： Wind
last update  | since  190802
/
class rC_Database( ) :
	def __init__(self, code ,start ,end ):
	def 

===============================================
'''
import pandas as pd
import numpy as np

class data_edb( ) :
	# 类的初始化操作
	def __init__(self, info=''  ):
		self.info = info
		# self.start=start
		# self.end=end



	def print_info(self ):
		print("Menu of class data_edb")
		print("gen_config |Generate configurations for edb data .")
		print("get_wind_quote_q  |下载季度收益数据 |一次性   ")
		print("trans_raw_edb  | 转换原始edb数据")
		print("cal_corr_Masset_Nfactors |计算M个资产季度价格和收益率分别与N个宏观经济指标的相关性   ")
		print("  |   ")

		print("corr_topN_factors  |6，读取相关系数，对每个资产收益率寻找相关性最高和最低的10个经济指标. ")


		return 1 

	def gen_config(self):
		### return a dcit object 
		dict_config ={}
		### Generate path 
		dict_config["path_quarter"] = "D:\\CISS_db\\keti_BL\\quote_quarter\\"
		dict_config["path_corr"] = "D:\\CISS_db\\keti_BL\\correlation\\"
		dict_config["path_dates"] = "C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_times\\"
		dict_config["path_wind"] = "D:\\CISS_db\\keti_BL\\quote_quarter\\"
		dict_config["path_in"] = "C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\apps\\black_litterman\\temp\\"

		file_asset = "asset_list.csv"
		dict_config["asset_list"] = pd.read_csv(dict_config["path_in"] + file_asset,encoding="gbk" )

		###
		### 导入日历日和交易日的季末日期
		file_date = "dates_quarter_0801_1906.csv"
		df_date = pd.read_csv( dict_config["path_dates"] +file_date )
		# 直接生成周、季度日期数据
		# pd.date_range(start='2018-05-01' ,periods = 100,freq = 'W')
		### start date 2008-03-31 
		### for economic data 
		dict_config["quarter_list_edb"] = df_date.quarter.values
		# dt_quarter_list_edb = pd.to_datetime(quarter_list_edb)
		### for quotation data
		dict_config["quarter_list_quote"] = df_date.quarter2.values


		return dict_config

	def get_wind_quote_q(self,date_start,date_end,dict_config):
		### 下载季度收益数据 |一次性
		asset_list =dict_config["asset_list"]
		path_quarter = dict_config["path_quarter"]
		import sys
		sys.path.append("C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\")
		from get_wind import wind_api
		wind_api1 = wind_api()

		items = "open,high,low,close,amt,pct_chg"
		# date_start = "20051231"
		for temp_code in asset_list.code : 
			winddata = wind_api1.GetWindData(temp_code, date_0, date_1, items,1)
			if winddata.ErrorCode == 0 :
				df0 = pd.DataFrame(winddata.Data,columns=winddata.Times ,index= winddata.Fields)
				df0 = df0.T
				
				df0.to_csv( path_quarter + "wind_"+temp_code+"_q"+".csv" )
				print( path_quarter + "wind_"+temp_code+"_q"+".csv" )

		return 1 

	def trans_raw_edb(self,file_name,file_date,file_name_out,dict_config):
		### 转换原始edb数据
		#  input：data_edb_raw_19Q1.xls
		# output：data_edb_quarter_19Q1.csv

		######################################################################
		### 4，Import date data change to smaller data table

		df_edb_q = pd.read_excel(dict_config["path_in"]+file_name, index_col="国家" ,encoding="gbk" )

		### Modify column names
		# "表名"\\"指标名称"" 1to1 "指标ID"

		indicator_list = df_edb_q.loc["指标名称",:]
		id_list = df_edb_q.loc["指标名称",:]
		df_edb_q.columns = indicator_list

		df_date = pd.read_csv(dict_config["path_date"] +file_date )
		# 直接生成周、季度日期数据
		# pd.date_range(start='2018-05-01' ,periods = 100,freq = 'W')
		### start date 2008-03-31 
		### drop cummy raws and modify column names
		# df_date.quarter2 is last trading date in quarter, df_date.quarter is last natural date

		### type of quarter_list_edb is str, but type of df_edb_q.index might be datetime 
		### for economic data 
		quarter_list_edb = df_date.quarter.values
		dt_quarter_list_edb = pd.to_datetime(quarter_list_edb)
		### for quotation data
		quarter_list_quote = df_date.quarter2.values
		dt_quarter_list_quote = pd.to_datetime(quarter_list_quote)

		print( df_edb_q.tail(3) )
		###notes:df_edb_q.columns中日期格式可能是 "2019-06-30 00:00:00",需要改为"2019-06-30"
		df_edb_q = df_edb_q.loc[ dt_quarter_list_edb  ,: ]


		df_edb_q.to_csv( path_in+file_name_out )


		return df_edb_q


	def str2float(self, input1 ) :
	### df_series
	#input1 是df里的单个值

		if type(input1) == str :
			output1 = input1.replace(",","").replace(" ","" )
		else :
			output1 = input1

		return output1

	def cal_corr_Masset_Nfactors(self,code,dict_config):
		### 计算M个资产季度价格和收益率分别与N个宏观经济指标的相关性。

		path_quarter = dict_config["path_quarter"]
		quarter_list_quote = dict_config["quarter_list_quote"]
		path_corr = dict_config["path_corr"]
		path_wind = dict_config["path_wind "]
		asset_list =dict_config["asset_list"]

		##################################################################
		### 4，按照 asset_list.csv,导入资产数据
		file_name = "wind_"+ code + "_q.csv"
		df_quote = pd.read_csv(path_quarter + file_name ,index_col="Unnamed: 0" )
		### start date 2008-03-31
		df_quote = df_quote.loc[ quarter_list_quote,: ]

		##################################################################
		### 改变数据格式，str2 float，剔除na值
		temp_col = df_edb_q.columns[0]
		temp_edb = df_edb_q.loc[ quarter_list_edb , temp_col ].astype(np.float64)
		### type of temp_edb is string, 
		temp_edb = temp_edb.fillna(method="pad")

		# 46个 || temp_length = len(temp_edb)# 54个 || len(df_quote.CLOSE)
		temp_close = df_quote.CLOSE
		##################################################################
		### 只保留“2011-06”这样月份信息，为了匹配季度末日期可能是非交易日的情况。
		#since 190802

		# type of var. is string 
		temp_edb.index = temp_edb.index.map(lambda x: x[:7])
		temp_close.index = temp_close.index.map(lambda x: x[:7])

		if len(temp_close) > len(temp_edb) :
		### type is series 
			temp_close = temp_close.loc[temp_edb.index ]

		temp_close.fillna(method="pad").corr( temp_edb )




		##################################################################
		### 5.1，收盘价和经济指标相关性
		### Generate DataFrame for correlation matrix

		df_corr = pd.DataFrame( index= df_edb_q.columns, columns= asset_list.code )

		for temp_code in asset_list.code :
			print("temp_code:", temp_code )

			file_name = "wind_"+ temp_code + "_q.csv"
			df_quote = pd.read_csv(path_wind + file_name ,index_col="Unnamed: 0" )
			df_quote = df_quote.loc[ quarter_list_quote,: ]
			temp_close = df_quote.CLOSE

			### 要使得 收盘价对应的日期和经济指标对应的日期长度一致
			print("length of temp_close:", len(temp_close) )

			for temp_col in df_edb_q.columns :
				# temp_col = df_edb_q.columns[0]
				# step: judge float or string

				# step" replace "," and " "
				# replace '195,422.20 ' to '195422.20'
				# lambda x: x.replace(",","").replace(" ","" ) 
				temp_edb = df_edb_q.loc[ quarter_list_edb , temp_col ].apply( self.str2float )
				# step: string to float 
				temp_edb = temp_edb.astype(np.float64)

				### type of temp_edb is string, 
				temp_edb = temp_edb.fillna(method="pad")

				if len(temp_close) > len(temp_edb) :
					### type is series 
					temp_close = temp_close.loc[temp_edb.index ]

				### Get correlation value 
				temp_value = temp_close.fillna(method="pad").corr( temp_edb )
				# print("temp_value ", temp_value )

				df_corr.loc[temp_col,temp_code] = temp_value


				df_corr.to_csv( path_corr+"df_corr.csv",encoding="gbk" )

		##################################################################
		### 5.2，价格涨跌幅和经济指标相关性
		### Generate DataFrame for correlation matrix
		### 判断变化率的相关矩阵

		df_corr_pct = pd.DataFrame( index= df_edb_q.columns, columns= asset_list.code )

		for temp_code in asset_list.code :
			print("temp_code:", temp_code )

			file_name = "wind_"+ temp_code + "_q.csv"
			df_quote = pd.read_csv(path_wind + file_name ,index_col="Unnamed: 0" )
			df_quote = df_quote.loc[ quarter_list_quote,: ]
			temp_close = df_quote.CLOSE

			temp_close_pct = temp_close.diff()/temp_close

			### 要使得 收盘价对应的日期和经济指标对应的日期长度一致
			print("length of temp_close:", len(temp_close) )

			for temp_col in df_edb_q.columns :
				# temp_col = df_edb_q.columns[0]
				# step: judge float or string

				# step" replace "," and " "
				# replace '195,422.20 ' to '195422.20'
				# lambda x: x.replace(",","").replace(" ","" ) 
				temp_edb = df_edb_q.loc[ quarter_list_edb , temp_col ].apply( self.str2float )
				# step: string to float 
				temp_edb = temp_edb.astype(np.float64)

				### type of temp_edb is string, 
				temp_edb = temp_edb.fillna(method="pad")

				temp_edb_pct =temp_edb.diff()/temp_edb

				if len(temp_close) > len(temp_edb) :
				### type is series 
					temp_close_pct = temp_close_pct.loc[temp_edb.index ]

				### Get correlation value 
				temp_value = temp_close_pct.fillna(method="pad").corr( temp_edb_pct )
				# print("temp_value ", temp_value )

				df_corr_pct.loc[temp_col,temp_code] = temp_value


				df_corr_pct.to_csv( path_corr+"df_corr_pct.csv",encoding="gbk" )

		###idea：todo。是否计算上涨和下跌时的相关性，还有分时期的相关性。
		return df_corr,df_corr_pct

	def corr_topN_factors(self,temp_code,file_corr,dict_config,N=10):
		### 6，读取相关系数，对每个资产收益率寻找相关性最高和最低的10个经济指标，进行线性/非线性回归分析
		# 1,标记df_corr_pct相关系数top5和tail5指标，对资产价格变动做线性回归模型
		# df_corr_pct = pd.read_csv( path_corr+"df_corr_pct.csv",encoding="gbk",index_col="指标名称" )
		N_head = round(N/2)
		N_tail = N-N_head

		path_corr = dict_config["path_corr"]
		path_wind = dict_config["path_wind"]
		path_quarter = dict_config["path_quarter"]
		quarter_list_quote = dict_config["quarter_list_quote"]
		quarter_list_edb = dict_config["quarter_list_edb"]

		df_corr_pct = pd.read_csv( path_corr+file_corr, encoding="gbk")


		asset0 = df_corr_pct.columns[0]
		# 默认是升序 ascending
		temp_df = df_corr_pct[asset0].sort_values().dropna()
		# Qs： index 相加会导致字符串合并,例如temp_df.index[:5] +temp_df.index[-5:] 
		# Ans: 先将index 对象转成list
		index_list =list(temp_df.index[:N_head] )+list(temp_df.index[-1*N_tail:] )
		
		col_list = temp_df.loc[index_list].values


		return index_list,col_list 

	
	def cal_linear_reg(self,temp_code,col_list,df_edb_q,dict_config):
		
		#########################################################################
		### prepare data for linear regression
		path_wind = dict_config["path_wind"]
		quarter_list_quote = dict_config["quarter_list_quote"]
		quarter_list_edb = dict_config["quarter_list_edb"]
		path_quarter = dict_config["path_quarter"]
		
		file_name = "wind_"+ temp_code + "_q.csv"
		df_quote = pd.read_csv(path_wind + file_name ,index_col="Unnamed: 0" )
		df_quote = df_quote.loc[ quarter_list_quote,: ]
		temp_close = df_quote.CLOSE

		temp_close_pct = temp_close.diff()/temp_close
		#########################################################################
		### Prepare x_train as input 
		### 6.1，把需要的10个经济指标数据准备好。
		temp_edb_all = pd.DataFrame( )		

		for temp_col in col_list :

			temp_edb = df_edb_q.loc[ quarter_list_edb , temp_col ].apply( self.str2float )
			# step: string to float 
			temp_edb = temp_edb.astype(np.float64)

			### type of temp_edb is string, 
			temp_edb = temp_edb.fillna(method="pad")

			temp_edb_pct =temp_edb.diff()/temp_edb

			if len(temp_close) > len(temp_edb) :
				### type is series 
				temp_close_pct = temp_close_pct.loc[temp_edb.index ]

			### add temp_edb to x_train 
			temp_edb_all =temp_edb_all.append( temp_edb_pct )

		temp_edb_all =temp_edb_all.T
		temp_edb_all = temp_edb_all.fillna(0)

		temp_edb_all.to_csv(path_quarter+"temp_edb_all.csv",encoding="gbk")

		### 6.2，regression: return = beta*factors
		# source https://www.cnblogs.com/pinard/p/6016029.html

		# Qs:是否要考虑非线性关系？
		# 划分训练集和测试集
		# 我们把X和y的样本组合划分成两部分，一部分是训练集，一部分是测试集，代码如下：
		# from sklearn.cross_validation import train_test_split
		# X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1)

		#######################################################################
		from sklearn.linear_model import LinearRegression
		
		### 创建线性回归对象
		linreg = LinearRegression()
		
		# linreg.fit([[8,2],[7,2],[4,2]],[1,4,9])
		# x_train.shape = (46,10) 
		x_train = temp_edb_all.iloc[:-4,:]
		y_train = temp_close_pct.fillna(0)
		y_train = y_train.iloc[:-4]

		# <class 'pandas.core.frame.DataFrame'> (46, 10)
		print( type(x_train), x_train.shape )

		### 训练模型
		linreg.fit( x_train, y_train) 

		### print regression output 
		print( linreg.intercept_ )
		print( linreg.coef_ )

		#######################################################################
		### 在测试集上进行预测

		x_test = temp_edb_all.iloc[-4:,:]
		y_test = temp_close_pct.fillna(0)
		y_test =y_test.iloc[-4:]
		y_predict= linreg.predict(x_test)

		print("y_predict ",y_predict )
		print("y_test ",y_test )
		print( 'Coefficients: \n', linreg.coef_  )

		from sklearn.metrics import mean_squared_error, r2_score
		print("Mean squared error: %.2f" % mean_squared_error( y_test, y_predict)  )
		# Explained variance score: 1 is perfect prediction
		print('Variance score: %.2f' % r2_score(y_test, y_predict))

		# plot绘制
		import matplotlib.pyplot as plt
		plt.scatter( [0,1,2,3],  y_test,  color='black')
		plt.plot( [0,1,2,3], y_predict, color='blue', linewidth=3)
		plt.xticks(())
		plt.yticks(())
		plt.show()



		return linreg