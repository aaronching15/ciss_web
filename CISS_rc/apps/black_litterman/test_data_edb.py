# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
需求：
实现black litterman模型

last 190529 || since 190529

Function:
功能：

todo:

Notes:
##############################################

##################################################################
Data Description
1,date range : 19781231 to 20190630
2,source:Wind Terminal\\EDB 数据浏览终端
3，indicators：China Economic Data
4，header：{国家 中国
表名 国内生产总值(季)
指标名称 GDP:不变价:当季同比
频率 季
单位 %
指标ID M0039354
时间区间 1992-01:2019-03
来源 国家统计局
更新时间 2019-04-17
}
5，date frequency：week，month，quarter，year

##################################################################
'''

##################################################################
### Initialization
import json
import pandas as pd 
import numpy as np 
import math

##################################################################
### 下载季度收益数据 |一次性

path_in = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\apps\\black_litterman\\temp\\"
file_asset = "asset_list.csv"

asset_list = pd.read_csv(path_in + file_asset,encoding="gbk" )

# import WindPy as wp 
# wp.w.start()
# items = "open,high,low,close,amt,pct_chg"
# date_start = "20051231"

# for temp_code in asset_list.code :
# print(temp_code)
# # wp.w.wsd("000985.CSI", "open,high,low,close,amt,pct_chg,vwap", "2018-05-29", "2019-06-27", "Period=Q;PriceAdj=F")
# winddata = wp.w.wsd(temp_code, items, date_start, "", "Period=Q;PriceAdj=F")
# ### Save to csv file 
# if winddata.ErrorCode == 0 :
# df0 = pd.DataFrame(winddata.Data,columns=winddata.Times ,index= winddata.Fields)
# df0 = df0.T
# df0.to_csv( "D:\\data_Input_Wind\\"+ "wind_"+temp_code+"_q"+".csv" )

##################################################################
### TODO 导入资产季度收益率数据 "CLOSE"


##################################################################

path_in = "C:\\zd_zxjtzq\\RC_trashes\\temp\\ciss_web\\CISS_rc\\apps\\black_litterman\\temp\\"

##################################################################
### Import raw EDB data 
file_name = "data_edb_raw_19Q1.csv"

df_raw = pd.read_csv(path_in+file_name, index_col="国家" ,encoding="gbk" )

### Modify column names
# "表名"\\"指标名称"" 1to1 "指标ID"
df_raw1 = df_raw

indicator_list = df_raw1.loc["指标名称",:]
id_list = df_raw1.loc["指标名称",:]
df_raw1.columns = indicator_list

##################################################################
### Import date data 
file_date = "date_quarter_0801_1906.csv"
df_date = pd.read_csv(path_in+file_date )

### start date 2008-03-31
### drop cummy raws and modify column names
# df_date.quarter2 is last trading date in quarter, df_date.quarter is last natural date
### for economic data 
quarter_list_edb = df_date.quarter.values
### for quotation data
quarter_list_quote = df_date.quarter2.values

df_raw1 = df_raw1.loc[ quarter_list_edb ,: ]

file_name_out = "data_edb_quarter_19Q1.csv"
df_raw1.to_csv( path_in+file_name_out )

##################################################################
### todo，为经济指标划分大类，并设置英文名，

 

 

##################################################################
### todo 按照 asset_list.csv,导入资产数据
path_wind ="D:\\data_Input_Wind\\"
# wind_CBA02003.CS_q.csv
code = "CBA02003.CS"
file_name = "wind_"+ code + "_q.csv"
df_quote = pd.read_csv(path_wind + file_name ,index_col="Unnamed: 0" )


##################################################################
### Qs:问题：严格按照季末最后一天，会出现当天非交易日无收盘价的情况，需要向前取收盘价。
# 填充缺失值方法 || pad/fill 填充方法向前 || bfill/backfill 填充方法向后
# 问题，这个方法会导致填充的总是前一个季度的值 df_quote.loc[ quarter_list,: ].fillna(method="pad")

### start date 2008-03-31
df_quote = df_quote.loc[ quarter_list_quote,: ]


##################################################################
### get corr.
# df_quote.CLOSE and df_raw1.iloc[:,0]
temp_col = df_raw1.columns[0]
temp_edb = df_raw1.loc[ quarter_list_edb , temp_col ].astype(np.float64)
### type of temp_edb is string, 
temp_edb = temp_edb.fillna(method="pad")

# 46个 || temp_length = len(temp_edb)
# 54个 || len(df_quote.CLOSE)

temp_close = df_quote.CLOSE


##################################################################
### 为了匹配季度末日期可能是非交易日的情况，需要新增一列，只保留“2011-06”这样月份信息
temp_edb.index = temp_edb.index.map(lambda x: x[:7])
temp_close.index = temp_close.index.map(lambda x: x[:7])

if len(temp_close) > len(temp_edb) :
### type is series 
temp_close = temp_close.loc[temp_edb.index ]

# print( temp_close )

temp_close.fillna(method="pad").corr( temp_edb )
# 0.09989790693788471

##################################################################
### df_series
def str2float( input1 ) :
# input1 是df里的单个值

if type(input1) == str :
output1 = input1.replace(",","").replace(" ","" )
else :
output1 = input1

return output1


##################################################################
### Generate DataFrame for correlation matrix

# df_corr = pd.DataFrame( index= df_raw1.columns, columns= asset_list.code )

# for temp_code in asset_list.code :
# print("temp_code:", temp_code )

# file_name = "wind_"+ temp_code + "_q.csv"
# df_quote = pd.read_csv(path_wind + file_name ,index_col="Unnamed: 0" )
# df_quote = df_quote.loc[ quarter_list_quote,: ]
# temp_close = df_quote.CLOSE

# ### 要使得 收盘价对应的日期和经济指标对应的日期长度一致
# print("length of temp_close:", len(temp_close) )

# for temp_col in df_raw1.columns :
# # temp_col = df_raw1.columns[0]
# # step: judge float or string

# # step" replace "," and " "
# # replace '195,422.20 ' to '195422.20'
# # lambda x: x.replace(",","").replace(" ","" ) 
# temp_edb = df_raw1.loc[ quarter_list_edb , temp_col ].apply( str2float )
# # step: string to float 
# temp_edb = temp_edb.astype(np.float64)

# ### type of temp_edb is string, 
# temp_edb = temp_edb.fillna(method="pad")

# if len(temp_close) > len(temp_edb) :
# ### type is series 
# temp_close = temp_close.loc[temp_edb.index ]

# ### Get correlation value 
# temp_value = temp_close.fillna(method="pad").corr( temp_edb )
# # print("temp_value ", temp_value )

# df_corr.loc[temp_col,temp_code] = temp_value


# df_corr.to_csv( "D:\\df_corr.csv",encoding="gbk" )

##################################################################
### Generate DataFrame for correlation matrix
### 判断变化率的相关矩阵

# df_corr_pct = pd.DataFrame( index= df_raw1.columns, columns= asset_list.code )

# for temp_code in asset_list.code :
# print("temp_code:", temp_code )

# file_name = "wind_"+ temp_code + "_q.csv"
# df_quote = pd.read_csv(path_wind + file_name ,index_col="Unnamed: 0" )
# df_quote = df_quote.loc[ quarter_list_quote,: ]
# temp_close = df_quote.CLOSE

# temp_close_pct = temp_close.diff()/temp_close

# ### 要使得 收盘价对应的日期和经济指标对应的日期长度一致
# print("length of temp_close:", len(temp_close) )

# for temp_col in df_raw1.columns :
# # temp_col = df_raw1.columns[0]
# # step: judge float or string

# # step" replace "," and " "
# # replace '195,422.20 ' to '195422.20'
# # lambda x: x.replace(",","").replace(" ","" ) 
# temp_edb = df_raw1.loc[ quarter_list_edb , temp_col ].apply( str2float )
# # step: string to float 
# temp_edb = temp_edb.astype(np.float64)

# ### type of temp_edb is string, 
# temp_edb = temp_edb.fillna(method="pad")

# temp_edb_pct =temp_edb.diff()/temp_edb

# if len(temp_close) > len(temp_edb) :
# ### type is series 
# temp_close_pct = temp_close_pct.loc[temp_edb.index ]

# ### Get correlation value 
# temp_value = temp_close_pct.fillna(method="pad").corr( temp_edb_pct )
# # print("temp_value ", temp_value )

# df_corr_pct.loc[temp_col,temp_code] = temp_value


# df_corr_pct.to_csv( "D:\\df_corr_pct.csv",encoding="gbk" )

 


##################################################################
### 读取相关系数，对每个资产收益率寻找相关性最高和最低的10个经济指标，进行线性/非线性回归分析
# 1,标记df_corr_pct相关系数top5和tail5指标，对资产价格变动做线性回归模型
df_corr_pct = pd.read_csv( "D:\\df_corr_pct.csv",encoding="gbk",index_col="指标名称" )

asset0 = df_corr_pct.columns[0]
# 默认是升序 ascending
temp_df = df_corr_pct[asset0].sort_values().dropna()
# Qs： index 相加会导致字符串合并,例如temp_df.index[:5] +temp_df.index[-5:] 
# Ans: 先将index 对象转成list
index_list =list(temp_df.index[:5] )+list(temp_df.index[-5:] )

### import close 
temp_code = "000985.SH"
print("temp_code:", temp_code )

file_name = "wind_"+ temp_code + "_q.csv"
df_quote = pd.read_csv(path_wind + file_name ,index_col="Unnamed: 0" )
df_quote = df_quote.loc[ quarter_list_quote,: ]
temp_close = df_quote.CLOSE

temp_close_pct = temp_close.diff()/temp_close

### Prepare x_train as input 
# 把需要的10个经济指标数据准备好。
temp_edb_all = pd.DataFrame( )

for temp_col in index_list :
temp_edb = df_raw1.loc[ quarter_list_edb , temp_col ].apply( str2float )
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

temp_edb_all.to_csv("D:\\temp.csv",encoding="gbk")


#################################################################
### regression: return = beta*factors
# source https://www.cnblogs.com/pinard/p/6016029.html

# Qs:是否要考虑非线性关系？
# 划分训练集和测试集
# 我们把X和y的样本组合划分成两部分，一部分是训练集，一部分是测试集，代码如下：
# from sklearn.cross_validation import train_test_split
# X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1)

from sklearn.linear_model import LinearRegression
linreg = LinearRegression()
# linreg.fit([[8,2],[7,2],[4,2]],[1,4,9])
# x_train.shape = (46,10) 
x_train = temp_edb_all
y_train = temp_close_pct.fillna(0)

print( type(x_train), x_train.shape )

linreg.fit( x_train, y_train)

print(linreg.intercept_ )
print( linreg.coef_ )

### RESULT
'''
0.010151688018739046
[-6.99550458e-02 2.60681778e-02 -1.30587124e-01 -3.35870888e-02
-1.00713275e-03 3.46630772e+00 -1.44314087e+00 1.46730730e-02
8.48579617e-02 1.79310642e-01]
'''
### todo 保存回归模型相关信息，像excel或者stata那样

 

 

 

##################################################################
### todo 
''' 已经计算了的是绝对值，
还需要对：
1，相关系数&预测能力：变化率的百分比求相关关系，判断经济指标对未来走势的预测能力。 最高点、最低点出现的时间；
2，在10个大类经济指标的group里发现影响因子最大的5~10个指标，生成10个汇总因子；
3，根据10个汇总因子分别对股票、债券、存款和现金进行配置。How？
4，


sub todo
1，temp_pct = temp_close.diff()/temp_close

 

'''

##################################################################
### todo
 

 

 

##################################################################
### todo 
''' 已经计算了的是绝对值，
还需要对：
1，相关系数&预测能力：变化率的百分比求相关关系，判断经济指标对未来走势的预测能力。 最高点、最低点出现的时间；
2，在10个大类经济指标的group里发现影响因子最大的5~10个指标，生成10个汇总因子；
3，根据10个汇总因子分别对股票、债券、存款和现金进行配置。How？
4，


sub todo
1，temp_pct = temp_close.diff()/temp_close

 

'''

##################################################################
### todo

 