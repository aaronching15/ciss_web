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
import json
import pandas as pd 
import numpy as np 
import math

from data_edb import data_edb 
data_edb1 =data_edb() 

##################################################################
### 1,Get configuration for Path infomation
dict_config = data_edb1.gen_config() 


##################################################################
### 2，下载季度收益数据 |一次性

# date_start = "20051231"
# date_end ="" 
# result = data_edb1.get_wind_quote_q(date_start,date_end,dict_config)

##################################################################
###,3，Import raw EDB data and transform to specific range,一次性

# file_name = "data_edb_raw_19Q1.xls"
# file_date = "dates_quarter_0801_1906.csv"
# file_name_out = "data_edb_quarter_19Q1.csv"
# df_edb_q = data_edb1.trans_raw_edb(file_name,file_date,file_name_out,dict_config)

##################################################################
###3，Import sorted quarterly edb data 
file_name = "data_edb_quarter_19Q1.csv"
df_edb_q = pd.read_csv(dict_config["path_in"]+file_name,index_col="Unnamed: 0")


##################################################################
### 4，按照 asset_list.csv,计算资产数据，一次性

# (df_corr,df_corr_pct) = data_edb1.cal_corr_Masset_Nfactors(code,dict_config)


##################################################################
### 6，读取相关系数，对每个资产收益率寻找相关性最高和最低的10个经济指标，进行线性/非线性回归分析
# 1,标记df_corr_pct相关系数top5和tail5指标，对资产价格变动做线性回归模型
# df_corr_pct = pd.read_csv( path_corr+"df_corr_pct.csv",encoding="gbk",index_col="指标名称" )

file_corr ="df_corr_pct.csv"
### import close 
temp_code = "000300.SH"
print("temp_code:", temp_code )

(index_list,col_list ) = data_edb1.corr_topN_factors(temp_code,file_corr,dict_config)


#################################################################
### 6.2，regression: return = beta*factors
# source https://www.cnblogs.com/pinard/p/6016029.html

linreg = data_edb1.cal_linear_reg(temp_code,col_list,df_edb_q,dict_config)

### RESULT
'''
0.010151688018739046
[-6.99550458e-02 2.60681778e-02 -1.30587124e-01 -3.35870888e-02
-1.00713275e-03 3.46630772e+00 -1.44314087e+00 1.46730730e-02
8.48579617e-02 1.79310642e-01]
'''



#################################################################
### TODO
#1, 尝试将宏观经济指标延迟1,2个lag后计算 
#2，保存回归模型相关信息，像excel或者stata那样
#3，topic 机器学习：sklearn模型指标和特征贡献度查看
# source: https://blog.csdn.net/opp003/article/details/84983370











# Qs:是否要考虑非线性关系？
# 划分训练集和测试集
# 我们把X和y的样本组合划分成两部分，一部分是训练集，一部分是测试集，代码如下：
# from sklearn.cross_validation import train_test_split
# X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1)
# 





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
 

 

 

 