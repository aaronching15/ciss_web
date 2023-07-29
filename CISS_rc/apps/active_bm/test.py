# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"






#################################################################################
### Initialization 
import os 
# 获取当前目录 os.getcwd() =: G:\zd_zxjtzq\ciss_web
import sys
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\" )
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )
sys.path.append(os.getcwd()[:2] + "\\ciss_web\\CISS_rc\\db\\" )
sys.path.append(os.getcwd()[:2] + "\\ciss_web\\CISS_rc\\db\\db_assets\\" )

import pandas as pd 
import numpy as np 
import math
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

### 
from analysis_indicators import indicator_ashares,analysis_factor
indicator_ashares_1 = indicator_ashares()
analysis_factor_1 = analysis_factor()

from transform_wind_wds import transform_wds
transform_wds1 = transform_wds()


#########################################################
### Calcualte factor Othogonal

# df_factor = pd.read_csv( "D:\\df_20050501_000300.SH.csv"  )

# print( df_factor.head() )
# # df_factor["ep_ttm"] = 1/ df_factor["S_VAL_PE_TTM"]
# # col_list_to_zscore =  ["ep_ttm","S_VAL_PCF_OCFTTM","roe_ttm"  ] 
# # obj_out = analysis_factor_1.indicator_data_adjust_zscore(df_factor,col_list_to_zscore)


# obj_in = {}
# obj_in["df_factor"] = df_factor
# obj_in["col_list_zscore"] =[]
# col_list =  ["ep_ttm","S_VAL_PCF_OCFTTM","roe_ttm"  ] 
# for temp_col in col_list :
#     obj_in["col_list_zscore"] =obj_in["col_list_zscore"] + ["zscore_"+temp_col]

# obj_out = analysis_factor_1.indicator_data_orthogonal(obj_in )


# asd


########################################
### 2.6.3,根据IC_IR的值，计算股票i在因子k{1，2,...,K}上的因子权重，

# # 导入历史ic_ir数值
# code_index ="000300.SH"
# temp_file= "df_IC_adj_20191031_000300.SH_20191031.csv"

# path_factor_model = "D:\\CISS_db\\factor_model\\"
# path_factor_model_sub = path_factor_model+ "000300.SH\\"
# # file_name_output= "df_IC_adj_" +str( date_last_month) +"_"+ code_index+ "_"+ str( date_last_month) +".csv"
# df_ic_ir = pd.read_csv( path_factor_model_sub + temp_file,index_col=0 ) 
# # df_ic_ir的列包括了 wind_code,date,ic_adj_, ic_ir_ ...
# print("df_ic_ir \n " ,df_ic_ir.tail().T  )
# date_list = list( df_ic_ir["date"].drop_duplicates() )
# date_list.sort() 
# # notes:难点：每一期的股票代码list都不一样

# ### date_list_sub是要取平均值的滚动日期列表
# date_list_sub =date_list[6:]
# print("date_list_sub \n", date_list_sub )

# def cal_replace_extreme_value_mad(df_factor,col_name ):
#     #对 df_factor[col_name] 用MAD方法替代异常值
#     # temp_median= np.median( code_list )
#     df_factor.to_csv("D:\\temp_df_factor.csv")
#     temp_median = df_factor[col_name].median()
#     temp_mad = np.median(  np.abs( df_factor[col_name] -temp_median  ) )

#     #########################################################
#     ### 计算上限和下限并替代极端值：upper_limit,lower_limit
#     # 感觉大概率不会超过极端值
#     upper_limit = temp_median+3*1.4826*temp_mad
#     lower_limit = temp_median-3*1.4826*temp_mad 

#     ### 先为所有值取下限值 
#     df_factor[col_name+"_mad"] = 0.0

#     # 若最大最小值没有超过就不需要调整
#     list_adj = []

#     for temp_i in df_factor.index : 
#         temp_value = df_factor.loc[temp_i, col_name]
#         if temp_value > upper_limit :
#             df_factor.loc[temp_i, col_name+"_mad"] = upper_limit
#         elif temp_value < lower_limit :
#             df_factor.loc[temp_i, col_name+"_mad"] = lower_limit
#         else :
#             df_factor.loc[temp_i, col_name+"_mad"] =df_factor.loc[temp_i, col_name]  

#     return df_factor

# # generate df =df_factor_weight
# index_i = 0 
# df_factor_weight= pd.DataFrame(index=[index_i], columns=["wind_code","date"] )

# for temp_date in date_list_sub :
#     ### 取最近的6~12期作为date_pre_temp_date
#     date_pre_temp_date = [ date for date in date_list if date<=temp_date]
#     date_pre_temp_date.sort()
#     date_pre_temp_date= date_pre_temp_date[-12:]

#     df_ic_ir_date = df_ic_ir[ df_ic_ir["date"] ==temp_date  ]
#     code_list = list( df_ic_ir["wind_code"].drop_duplicates() )

#     for temp_code in code_list : 
#         ### 只取最近的12期
#         df_ic_ir_sub_s = df_ic_ir [ df_ic_ir["wind_code"] == temp_code  ]
#         #
#         df_ic_ir_sub_s= df_ic_ir_sub_s[ df_ic_ir_sub_s["date"].isin(date_pre_temp_date) ]
        
#         ### 2，对于单只个股i，计算个股i单个指标ic_ir均值/个股i所有指标ic_ir均值之和
#         # temp_ic_ir = "ic_ir_ret_mdd_20d_120d"
#         # 求所有股票在过去T(6~12)期的平均值的绝对值之和
        
#         df_factor_weight.loc[index_i,"wind_code"] = temp_code
#         df_factor_weight.loc[index_i,"date"] = temp_date
        
#         ic_ir_list= []
#         sum_ic_ir_median = 0 
#         # df_factor_weight
#         for temp_ic_ir in df_ic_ir_sub_s.columns:
#             if temp_ic_ir[:5] =="ic_ir" :
#                 ic_ir_list= ic_ir_list + [ temp_ic_ir ]
#                 df_ic_ir_sub_s = cal_replace_extreme_value_mad(df_ic_ir_sub_s,temp_ic_ir )
#                 # 用np.nan无法识别，用fillna的方式
#                 temp_median = df_ic_ir_sub_s[temp_ic_ir+"_mad"].fillna(0.0).median()
#                 print("temp_median:" ,temp_median,type(temp_median) )
#                 if not temp_median == np.nan :
#                     df_factor_weight.loc[index_i, "f_w_"+temp_ic_ir ] = temp_median
#                     sum_ic_ir_median = sum_ic_ir_median + abs(temp_median)
#                 else :
#                     df_factor_weight.loc[index_i, "f_w_"+temp_ic_ir ] = 0.0

#         # 最后统一除以均值的绝对值之和
#         for temp_ic_ir in ic_ir_list:
#             df_factor_weight.loc[index_i,"f_w_"+temp_ic_ir]= df_factor_weight.loc[index_i,"f_w_"+temp_ic_ir]/sum_ic_ir_median

#         index_i = index_i + 1  

#     ### save to csv file 
#     file_name_output = "df_factor_weight_" +str( temp_date) +"_"+ code_index+ ".csv"
#     df_factor_weight.to_csv( path_factor_model_sub + file_name_output )
    
# # mean 和median可以自动剔除NaN值，不知道能否剔除异常尾部值。
# asd

########################################
### 3，设计多因子基础模型：构建沪深300与中证500增强基准策略。
'''
3，设计多因子基础模型：构建沪深300与中证500增强基准策略。

优化模型：
Obj function: max sum( ret_k *w_k)
s.t. 1,𝑠_𝑙≤𝑋(𝑤−𝑤𝑏)≤𝑠_ℎ
    2,ℎ_𝑙≤𝐻(𝑤−𝑤𝑏)≤𝐻_ℎ
    3,𝑤_𝑙≤𝑤−𝑤𝑏≤𝑤_ℎ
    4,𝑤≥0
    5,𝑊_𝑙≤𝟏_T*𝑤≤𝑊_ℎ
    6,∑|𝑤_s_total_t − 𝑤_s_total_t_pre |≤ turnover_limit

notes:
1,目标方程：ret_k 是ICIR加权后的复合因子值，w_k是求解得到的最优化因子权重
    根据过去T期的值，在t时点预测t+1时点的股票收益率，也就是在20151030月末数据可得后，计算
    20151131月份的最优权重。
    分析：w：自变量组合在N个股票上的权重；w_b:市场组合在N个股票上的权重
2，因子约束条件：s_l，s_h是因子暴露的上下限；一般只对市值因子设置上下限，如市值中性设置：
    s_l_mv=0 and s_h_mv = 0 ；只限制市值因子，也就是 0<= sum{x_i_k,k=mv} <=0
    改写：s_l+ X*w_b <= X*w <= s_h+ X*w_b , 
    where X= factor_weight_np ,factor_weight_np from df_factor_weight;因子暴露矩阵,N*K matrix
3,行业暴露矩阵，设置组合相对于基准行业权重的上限和下线ℎ_𝑙，𝐻_ℎ，例如行业中性设置：
    ℎ_𝑙=0.0，𝐻_ℎ = 0。0
    改写：h_l+ H*w_b <= H*w <= h_h+ H*w_b 
4,个股相对于基准指数中权重暴露的上下限，例如上下限+2%/-2%；
    w_L = -0.02,w_h=0.02
5,总仓位的上下限，例如最低80%，最高95%；
    w_total_l = 0.8, w_total_h= 0.95 
6,当期权重变动，当期个股权重减上期个股权重的绝对值之和,例如每个季度60%对应每年240%，买入和卖出都算。
    sum( abs(𝑤_s_total_t − 𝑤_s_total_t_pre )) ≤ turnover_limit 
'''
#################################################################################
### 设定变量和导入数据
path_factor_model = "D:\\CISS_db\\factor_model\\"
path_factor_model_sub = path_factor_model+ "000300.SH\\"
code_index = "000300.SH"
file_name_date_list = "date_list_m_050501_200404.csv"
df_date_month = pd.read_csv(path_factor_model + file_name_date_list )
date_list_month = df_date_month["date"].values
# 日期升序排列
date_list_month.sort()

date_last_month = 20060228 # 20060228是第一次有factor weight的月份
input1= input("Check date_last_month to proceed "+str(date_last_month) )


########################################################################
### 给定日期，导入相关数据，20151231时count_month=6了，
# 导入t时期指数成分 w_b=w_stock_bm,df_factor_weight，导入t+1，下个月末20160131时的股票收益率 ret_stock_change_np
# notes:假定factor_weight_np第一行factor_weight_np[0] 对应的是每个股票在市值因子上的暴露
# date_list_month_pre 是已经计算过的月份
date_list_month_pre = [date for date in date_list_month if date<= date_last_month ]
# date_list_month_pre 是根据最新日期还未计算的月份
date_list_month = [date for date in date_list_month if date> date_last_month ]

count_month = len(date_list_month_pre )
# 取最后一个日期
temp_date = date_list_month_pre[-1]
temp_date_pre = date_list_month_pre[-2]

########################################################################
### 导入指数成分 df_index_consti
obj_in_index={} 
obj_in_index["date_start"] = temp_date
obj_in_index["code_index"] = code_index
obj_in_index["table_name"] = "AIndexHS300FreeWeight"

obj_out_index = indicator_ashares_1.ashares_index_constituents(obj_in_index) 
# code_list = obj_out_index["df_ashares_index_consti"]["S_CON_WINDCODE"].values
# 获取指数成分股、权重
df_index_consti = obj_out_index["df_ashares_index_consti"].loc[:, ["S_CON_WINDCODE","I_WEIGHT","TRADE_DT" ] ]
### 注意，将wind_code 按升序排列
df_index_consti = df_index_consti.sort_values(by="S_CON_WINDCODE"  )
print("df_index_consti   \n", df_index_consti.head().T )

### 指数权重成分 w_index_consti ,np.array | notes: "I_WEIGHT"的值7.5等，需要除以100
w_index_consti = df_index_consti["I_WEIGHT"].values /100
print("w_index_consti ", type(w_index_consti ) ) 

### 仅保留沪深300成分股
code_list_csi300 = list( df_index_consti[ "S_CON_WINDCODE"].values )

########################################################################
### 1,factor_weight_np
# Import df_factor_weight_20060228_000300.SH.csv
file_name_output= "df_factor_weight_" +str( date_last_month) +"_"+ code_index +".csv"
df_factor_weight  = pd.read_csv( path_factor_model_sub + file_name_output,index_col=0 )
df_factor_weight = df_factor_weight[ df_factor_weight["wind_code"].isin(code_list_csi300 ) ]

print("df_factor_weight_",len(df_factor_weight ) ) 
# df_factor_weight to factor_weight_np
# a.remove(value):删除列表a中第一个等于value的值；a.pop(index):删除列表a中index处的值；del(a[index]):删除列表a中index处的值
# sub step 1:wind_code 升序排列：
df_factor_weight = df_factor_weight.sort_values(by="wind_code")
# sub step 2:剔除非因子值；因子icir不包括市值，需要先导入！
col_list = list( df_factor_weight.columns.values ) 
col_list.remove( "wind_code" )
col_list.remove( "date" )
print("col_list",col_list )

########################################################################
### 导入因子权重矩阵 factor_weight_np
### 导入总市值因子、中信一级行业和其他因子 | "zscore_S_DQ_MV" | df_factor_20060228_000300.SH_20060228.csv
file_name_output= "df_factor_" +str( date_last_month) +"_"+ code_index +"_"+str( date_last_month) +".csv"
temp_df_factor = pd.read_csv( path_factor_model_sub + file_name_output,index_col=0 )

for temp_i in df_factor_weight.index :
    temp_code = df_factor_weight.loc[temp_i, "wind_code" ]
    # find code and "zscore_S_DQ_MV" in temp_df_factor
    # print("temp_code ",temp_code )
    temp_df_factor_sub = temp_df_factor[ temp_df_factor["wind_code"]==temp_code ]
    if len( temp_df_factor_sub.index ) > 0 :
        temp_j = temp_df_factor_sub.index[0]
        df_factor_weight.loc[ temp_i, "zscore_S_DQ_MV" ] = temp_df_factor.loc[temp_j, "zscore_S_DQ_MV"  ]
        
    else :
        print("No record for code ", temp_code  )
        df_factor_weight.loc[ temp_i, "zscore_S_DQ_MV" ] = -1

### 第一行要设置为市值因子; notes: szcore不是ic_ir,可能会有点问题。
col_list= [ "zscore_S_DQ_MV" ] + col_list
### 和其他因子
df_factor_weight_values = df_factor_weight.loc[:,col_list ]
df_factor_weight_values.index = df_factor_weight["wind_code"]
print( "df_factor_weight_values"  )
print(  df_factor_weight_values.head().T )
# example,from 300*16 to 16*300,shape of factor_weight_np 16*300
factor_weight_np = df_factor_weight_values.T.values

# 把 factor_weight_np 中nan替换为较低值 -0.5
# where_are_NaNs = np.isnan(d)  >>> d[where_are_NaNs] = 0
where_nan = np.isnan( factor_weight_np ) 
factor_weight_np[ where_nan] = -0.5 

print("Shape of factor_weight_np" ,factor_weight_np.shape )

# 中信一级行业 || df_ind_code 是代码升序排列的行业分类
df_ind_code = temp_df_factor[ temp_df_factor["date"]==date_last_month ]  
df_ind_code = df_ind_code.loc[:, ["wind_code","citics_ind_code_s_1"] ]  
df_ind_code = df_ind_code.sort_values(by="wind_code")

########################################################################
### 2,ret_stock_change_np | 给定日期，导入指数成分股、权重和下个月的收益率
obj_in_index={} 
obj_in_index["date_pre"] = temp_date_pre
obj_in_index["date"] = temp_date
obj_in_index["df_change"] = df_index_consti
obj_in_index["df_change"]["wind_code"] = obj_in_index["df_change"]["S_CON_WINDCODE"]
print("df_index_consti" ,obj_in_index["df_change"].head()  ) 

obj_in_index = indicator_ashares_1.ashares_stock_price_vol_change( obj_in_index )

print( obj_in_index["df_change"].head().T )

ret_stock_change_np =  obj_in_index["df_change"]["s_change_adjclose"].values

########################################################################
### 构建行业暴露矩阵ind_code_np和上下限ind_lb,ind_ub;ind_code_np矩阵取值都是1或0，其中每一行对应一个行业，
# 每一行与最优权重变量相乘得到最优组合在某一行业的权重，ind_lb[i]对应了第i个行业的权重下限
# ind_ub[i]对应第i个行业的权重上限。ind_bm:基准组合的行业权重。
# 从 df_factor_20060228.csv 里获取行业分类 "citics_ind_code_s_1"
# df_ind_code.columns ["wind_code","citics_ind_code_s_1"]
# notes: df_ind_code一般多少有几个code是空值，np.nan,也应该给与一定的权重。例如200602的600087.SH长油。
# notes:ind_code_list数值从小到大排列，最后一个可能是nan
ind_code_list = list( df_ind_code["citics_ind_code_s_1" ].drop_duplicates() )
ind_code_list.sort()
print("ind_code_list",ind_code_list ) 

len_stock = len(df_ind_code.index )
ind_code_np = np.zeros( (len(ind_code_list),len_stock ) )

df_ind_code_m = pd.DataFrame(ind_code_np ,index=ind_code_list,columns = df_ind_code["wind_code"] )

for temp_i in df_ind_code.index :
    temp_code = df_ind_code.loc[temp_i, "wind_code"] 
    temp_ind_code = df_ind_code.loc[temp_i, "citics_ind_code_s_1"] 
    df_ind_code_m.loc[temp_ind_code,temp_code] = 1 

print("df_ind_code_m"  )
# from df to np.arrays 
ind_code_np = df_ind_code_m.values

########################################################################
### 构建个股权重矩阵 w_stock_np，对于N个股票，N*N矩阵，每行仅对角线1个值为1，其余为0

w_stock_np = np.zeros( (len_stock ,len_stock ) )
for temp_i in range( len_stock ) :
    w_stock_np[temp_i][temp_i] = 1 

print("w_stock_np")
########################################################################
### 导入相关包 | from BL 模型
from scipy import optimize
### 定义目标方程：obj_fun 求最小值的目标函数| # optimize.minimize第一个input：obj_fun: 
obj_fun = lambda w_stock_opt: -1* np.matmul( ret_stock_change_np, w_stock_opt ).sum()   

### 定义限制条件 1~ C：constraint_c
# 生成一列都是-1或1的数组 ，或1*5尺寸的nan：np.full([1,len() ], np.nan )
len_factor = np.size(factor_weight_np,0 )
print("len_factor ", len_factor )
s_l = np.full([1, len_factor ], -1 )
s_l[0] = 0 
s_h = np.full([1,len_factor ], 1 )
s_h[0] = 0 
# 'type':'ineq' # >= #  'type':'eq'   # == 

def constraint_1_factor(w_stock_opt):
    ''' 1,X*w -X*w_b -s_l >= 0 
    因子约束条件:s_l，s_h是因子暴露的上下限；一般只对市值因子设置上下限，如市值中性设置：
    s_l_mv=0 and s_h_mv = 0 ；只限制市值因子，也就是 0<= sum{x_i_k,k=mv} <=0
    改写：s_l+ X*w_b <= X*w <= s_h+ X*w_b , 
    where X= factor_weight_np ,factor_weight_np from df_factor_weight;因子暴露矩阵,N*K matrix
    # notes:factor_weight_np第一行是市值因子
    '''
    # print("666 ")
    # shape of factor_weight_np 16*300
    result = np.matmul( factor_weight_np,w_stock_opt) -np.matmul( factor_weight_np,w_index_consti) - s_l
    if len(result) == 1 :
        result = result[0]
    # print("1 factor ",len(result),result)
    return result

def constraint_2_factor(w_stock_opt):
    ''' 2, s_h+ X*w_b - X*w >= 0 
    因子约束条件: X*w <= s_h+ X*w_b ,    '''
    result = -1*np.matmul( factor_weight_np,w_stock_opt) +np.matmul( factor_weight_np,w_index_consti) + s_h
    if len(result) == 1 :
        result = result[0]
    return result

def constraint_3_ind(w_stock_opt):
    '''行业暴露矩阵ind_code_np，设置组合相对于基准行业权重的上限和下线:ind_lb[i]对应了第i个行业的权重下限
    ind_ub[i]对应第i个行业的权重上限。，例如行业中性设置：ind_lb=0.0，ind_ub= 0.0
    ind_bm:基准组合的行业权重
    from  ind_lb <= ind_code_np*( w_stock_opt - w_index_consti )   <= ind_ub
    to: 1,ind_code_np*w_stock_opt - ind_code_np*w_index_consti - ind_lb >= 0
    2,-1*ind_code_np*w_stock_opt + ind_code_np*w_index_consti + ind_ub >= 0
    notes:ind_code_list 行业分类数值从小到大排列，最后一个可能是nan, len(`)=30
    notes:ind_ub不能是 np.ones( (len(ind_code_list,1 ) ) )
    ''' 
    
    # ind_lb = np.zeros( (1,len(ind_code_list ) ) ) 
    # 例：任意一个行业权重不应该超过30%
    ind_lb = np.ones( (1,len(ind_code_list ) ) ) 
    ind_lb = ind_lb*0.3
    result = np.matmul( ind_code_np,w_stock_opt) - np.matmul( ind_code_np,w_index_consti) - ind_lb
    if len(result) == 1 :
        result = result[0]
    return result  

def constraint_4_ind(w_stock_opt):
    '''行业暴露矩阵ind_code_np
    from  ind_lb <= ind_code_np*( w_stock_opt - w_index_consti )   <= ind_ub
    to: 2,-1*ind_code_np*w_stock_opt + ind_code_np*w_index_consti + ind_ub >= 0
    # notes:ind_code_list 行业分类数值从小到大排列，最后一个可能是nan
    notes:ind_ub不能是 np.ones( (len(ind_code_list,1 ) ) )
    '''     
    ind_ub = np.ones( (1,len(ind_code_list ) ) )
    result = -1*np.matmul( ind_code_np,w_stock_opt) + np.matmul( ind_code_np,w_index_consti) + ind_ub
    if len(result) == 1 :
        result = result[0]
    return result  
# 变量的上限和下限似乎不是必须的，因为 optimize.minimize的bounds条件可以直接限制
def constraint_5_s(w_stock_opt):
    '''4,个股相对于基准指数中权重暴露的上下限，w_l <= 1*w <= w_h
    例如上下限+2%/-2%；w_L = -0.02,w_h=0.02
    '''
    w_l = np.zeros( len_stock)
    result = np.matmul(w_stock_np, w_stock_opt) - w_l 
    return result  

def constraint_6_s(w_stock_opt):
    '''4,个股相对于基准指数中权重暴露的上下限，w_l <= 1*w <= w_h
    例如上下限+2%/-2%；w_L = -0.02,w_h=0.02
    # 例如：个股权重上限w_s_h不超过10%
    '''
    w_h = np.ones( len_stock)
    w_s_h = 0.1
    w_h = w_h * w_s_h
    result = -1*np.matmul(w_stock_np, w_stock_opt) + w_h
    return result  

def constraint_7_port(w_stock_opt):
    '''5,总仓位的上下限，例如最低80%，最高95%；w_total_l <= sum(w) <= w_total_h
    w_total_l = 0.8, w_total_h= 0.95 
    np.sum(rr),对array的rr的所有值求和， np.sum(rr,axis=0)对每一列求和， np.sum(rr，axis=1)对每一行求和
    '''
    w_total_l = 0.8
    result = np.sum( w_stock_opt ) - w_total_l
    return result  

def constraint_8_port(w_stock_opt):
    '''5,总仓位的上下限，例如最低80%，最高95%；w_total_l <= sum(w) <= w_total_h
    w_total_l = 0.8, w_total_h= 0.95 
    np.sum(rr),对array的rr的所有值求和， np.sum(rr,axis=0)对每一列求和， np.sum(rr，axis=1)对每一行求和
    '''
    w_total_h = 1.0
    result = -1*np.sum( w_stock_opt ) + w_total_h
    return result  

# 换手率限制turnover_limit 
turnover_limit = 1.0
# 上一期股票权重：
w_stock_opt_pre = np.zeros( len_stock)

w_stock_opt_pre = np.zeros( len_stock)
def constraint_9_turnover(w_stock_opt):
    '''6,当期权重变动，当期个股权重减上期个股权重的绝对值之和,例如每个季度60%对应每年240%，买入和卖出都算。
    sum( abs(𝑤_s_total_t − 𝑤_s_total_t_pre )) ≤ turnover_limit 
    上一期股票权重：w_stock_opt_pre,如果是第一期可以取0.0
    abs(array1-array2 ) 会取两列的每一个差值
    换手率限制turnover_limit 第一期应该是100%，之后按单季度应该是75%，按单月应该是30%。
    turnover_limit = 1.0
    '''
    print("666 ",np.sum( abs( w_stock_opt-w_stock_opt_pre )) )
    result = -1*np.sum( abs( w_stock_opt-w_stock_opt_pre )) + turnover_limit

    print("9 tunrover  "  )
    return result  

# eq表示 函数结果等于0 ； ineq 表示 表达式大于等于0 ||  'type':'ineq' >= ; 'type':'eq' ==  
# 例子：{'type': 'ineq', 'fun': lambda w_stock_opt:  -1*np.matmul( x,np.matmul(cov_asset_df,x) )+ var_bench  }
cons = ({'type': 'ineq', 'fun': constraint_1_factor },
        {'type': 'ineq', 'fun': constraint_2_factor },
        {'type': 'ineq', 'fun': constraint_3_ind },
        {'type': 'ineq', 'fun': constraint_4_ind },
        {'type': 'ineq', 'fun': constraint_7_port },
        {'type': 'ineq', 'fun': constraint_8_port },
        {'type': 'ineq', 'fun': constraint_9_turnover } ) 

# ：个股权重上限w_s_h不超过10%
w_s_h = 0.1
bnds = [(0,w_s_h )] * len_stock
# 设置所有股票的初始权重，可以单股票为1，均匀权重，直接取市场基准权重等 
# w_init = np.ones( (len_stock) )/len_stock
w_init =  w_index_consti 
# optimize.minimize第一个input：obj_fun: 求最小值的目标函数
res = optimize.minimize( obj_fun , w_init, method='SLSQP', bounds=bnds,constraints=cons)

print("result")
print(res.success,res.message)
# False Positive directional derivative for linesearch

w_mkt = res.x
print("return of opt portfolio")
print( res.fun )
print( np.matmul( ret_stock_change_np,w_mkt ).sum()   )
print("weights of opt portfolio")
print( np.round(w_mkt,4 ) )

application_id = "200418"
file_output = "temp.csv"
if not os.path.exists( path_factor_model_sub + application_id+"\\" ) :
    os.mkdir(path_factor_model_sub + application_id+"\\" )

pd.DataFrame(w_mkt).to_csv(path_factor_model_sub + application_id+"\\"+file_output )
TODO

########################################################################
### 
'''todo
1,
2,
'''










































