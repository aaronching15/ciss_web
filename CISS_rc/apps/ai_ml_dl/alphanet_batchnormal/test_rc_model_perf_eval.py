# -*- coding: utf-8 -*-
__author__ = " ruoyu.Cheng"
"""
todo
1，

功能：0，是test_rc_train_model_v3.py 的下一步
1，导入最优参数.h5 文件，并对不同区间因子数据计算出预测值y_pred 
steps：1，导入股票代码和日期，与 fac1.csv 文件里的行和列匹配
2，对每个日期，500个入选个股进行排序：按数值大小分5档、10档，计算至下一个更新日期的区间收益，分别计算模拟组合
3，按年度统计收益率



last  | since 20201201
derived from test_model.py
"""

# from keras.models import load_model
# from custom_layer import *
# from util import *
import numpy as np
from scipy import stats
import pandas as pd
import scipy.io as sio
import h5py

import matplotlib.pyplot as plt

#####################################################################
### time
import datetime as dt 
# 20201129_161059
temp_date = dt.datetime.strftime( dt.datetime.now(),"%Y%m%d_%H%M%S")
print("Curent time is ", temp_date)

input_ID = input("type ID name for this training，example=20201206_102305_fac1.csv...")
if len( input_ID) <12:
    input_ID = "20201201_132913"
###
file_output_ret_port = input_ID +"_port_ret_weighted.csv"
file_output_factor = input_ID +"_factor_weighted.csv"
file_output_weight = input_ID +"_port_weight.csv"
print( file_output_ret_port , file_output_factor,file_output_weight )
input_check = input("Check output file name:" )

#####################################################################
### 设置模型位置path
# model_path = 'model10d/regress10/1/' 'model20d/regress6/' 'model5d/regress1/'
# 'model10d/v2/' ; 'model10d/v3/'

path0 = "D:\\CISS_db\\outside_sellside\\AI_alphanet_huatai\\"
path_zz500 = path0+ "\\ZZ500\\"
path_data = path0+ "\\data2\\"
#
# path_model_10d_v3 = path_zz500 + "model10d\\v3\\"
path_v3 = path_zz500 + "model10d\\v3\\"

### 模型的保存目录
path_model_output_ID = path_v3+"model_" + input_ID  + "\\"
### 输出数据、统计指标的保存目录
path_data_output_ID = path_v3+"output_" + input_ID  + "\\" 

#####################################################################
### 1，导入模型最优参数.h5 文件，并对不同区间因子数据计算出预测值y_pred 
# model_v2_fac1.csv 
# 自己计算的：20201201_132913_fac1.csv ； 测试数据="20201206_102305_fac2.csv"
file_name = "20201201_132913_fac1.csv"
### 不带index导入
df_factor = pd.read_csv( path_data_output_ID  + file_name,index_col=0   )

# number of index = 3962; columns= 472~1000
print("df_factor,index,columns \n", len(df_factor.index),len(df_factor.columns)   ) 

#####################################################################
### steps：1，导入股票代码和日期，与 fac1.csv 文件里的行和列匹配
# ntoes:日期数据是从mat文件里导出后 -693960 后手工在excel里用日期转换的
file_date = "date_match.csv"
df_date =pd.read_csv(path_zz500 + file_date )
### columns ="date","value" 
### 由于数据文件中日期菜用整数顺序，也就是0，1，2...，需要用index来替代
df_date["date_int"] = df_date.index

# print("df_date \n", df_date.head() ,df_date.tail() )

file_code = "code.csv"
df_code = pd.read_csv(path_zz500 + file_code)
# print("df_code \n", df_code.head() )

#####################################################################
### 导入全区间股票收益率 ；index=0,1,2.. 对应列"wind_code";columns= 3161,...5456,wind_code
# file=20201201_132913_ashare_ret.csv;path=D:\CISS_db\outside_sellside\AI_alphanet_huatai\ZZ500\model10d\v3\output_20201201_132913
file_return = "20201201_132913_ashare_ret.csv"
df_return = pd.read_csv( path_data_output_ID + file_return,index_col=0 )
print("df_return ",df_return.shape )
# 将NAN值转换为0
df_return = df_return.fillna(0)

#####################################################################
### 设置参数 parameter
### 1,百分比分组
num_group = 5
# list( range(0,10,int(10/10) ) );注意：range只能用整数
# 第一个值 [0.2, 0.4, 0.6, 0.8]
list_step = list( range(1, num_group , 1  ) )
list_step = [ x/num_group for x in list_step ] + [1.0]

num_group = 10
list_step_10 = list( range(1, num_group , 1  ) )
list_step_10 = [ x/num_group for x in list_step ]+ [1.0]

### 定义分组收益率 df_ret_port
df_ret_port = pd.DataFrame(index=df_factor.columns, columns= list_step   ) 

##################################
### 对df_factor的某一列col1进行5档权重生成

def get_weight( obj_factor ):
    ### 
    col_name = obj_factor["col_name"] 
    df_factor = obj_factor["df_factor"]
    list_step = obj_factor["list_step"] 
    weight_level = 1
    ### 
    df_factor["weight_f"] = 0.0
    for temp_pct in list_step :
        # df_factor[col_name]
        temp_f_value = df_factor[col_name].quantile( temp_pct )
        ### first range
        if temp_pct == list_step[0] :
            ### 设置加权方式：1，等权重；2，因子大小加权；3，...
            ### 1，等权重
            # df_factor[temp_pct] = df_factor[col_name].apply(lambda x : 1 if x <= temp_f_value else 0  ) 
            ### 2，因子大小加权:把区间内的因子值划分为5档权重 
            # df_factor["weight_f"] = df_factor.apply(lambda x : weight_level if x[col_name] <= temp_f_value else ( x["weight_f"] if x["weight_f"]>0 else 0  ) )  
            df_factor["weight_f"] = df_factor[col_name].apply(lambda x : weight_level if x <= temp_f_value else 0 )  
            
            ###
            # index_series = df_factor[ df_factor[temp_pct]==1 ].index
            
        else :
            temp_f_value_pre = df_factor[col_name].quantile( temp_pct_pre )
            ### 1，等权重
            # df_factor[temp_pct] = df_factor[col_name].apply(lambda x : 1 if ( x <= temp_f_value and x > temp_f_value_pre ) else 0  ) 
            ### 2，因子大小加权:把区间内的因子值划分为5档权重
            df_factor["temp"] = df_factor[col_name].apply(lambda x : weight_level if ( x<= temp_f_value and x  > temp_f_value_pre ) else 0 )  
            # 两列数字进行大小比较;a['max'] = a.apply(lambda x: max(x['a'], x['b']), axis=1) ;b['min'] = b.apply(lambda x: min(x['a'], x['b']), axis=1)
            df_factor["weight_f"]  = df_factor.apply(lambda x: max(x["temp"], x["weight_f"]), axis=1)
                        
            ### 

        temp_pct_pre = temp_pct
        weight_level = weight_level + 1
    
    ###########################################################################
    ### sum of weight = 100%
    df_factor["weight_f"] = df_factor["weight_f"]/df_factor["weight_f"].sum()
    ### save to obj_factor 
    obj_factor["df_factor"] = df_factor

    return obj_factor

#####################################################################
### 2，对每个日期，500个入选个股进行排序：按数值大小分5档、10档，计算至下一个更新日期的区间收益，分别计算模拟组合
# 总共约有400多个
df_weight = pd.DataFrame(index=df_factor.index, columns= ["wind_code"] ] )

temp_list = df_factor.columns[-10:]
# for temp_date_int in df_factor.columns :
for temp_date_int in temp_list :
    # temp_date_int=3161,...
    print("temp_date_int  ",temp_date_int   ) 
    df_1d = df_factor[temp_date_int]

    #####################################################################
    ### 对成分股票进行5档、10档分层,并新建columns
    list_ret_pct = []
    ### 百分比数值为升序：df_factor[temp_date_int].quantile(x),x取 0.2、0.5、0.8数值分别为 -0.16，-0.009，+0.129
    temp_pct_pre = 0.0 
    # list_step =[0.2, 0.4, 0.6, 0.8]
    for temp_pct in list_step :
        # df_factor[temp_date_int]
        temp_f_value = df_factor[temp_date_int].quantile( temp_pct )
        ### first range
        if temp_pct == list_step[0] :
            ### 设置加权方式：1，等权重；2，因子大小加权；3，...
            df_factor[temp_pct] = df_factor[temp_date_int].apply(lambda x : 1 if x <= temp_f_value else 0  ) 
            index_series = df_factor[ df_factor[temp_pct]==1 ].index
            ### 1，等权重
            # df_factor.loc[index_series,"weight_f_"+ str(temp_pct)] = df_factor.loc[index_series,temp_pct] /len( index_series )

            ### 2，因子大小加权:把区间内的因子值划分为5档权重;temp_date_int 对应的是因子值            
            obj_factor ={}
            obj_factor["col_name"] = temp_date_int
            obj_factor["df_factor"] = df_factor[ df_factor[temp_pct]== 1 ]
            obj_factor["list_step"] = list_step
            obj_factor = get_weight( obj_factor )
            df_weight.loc[index_series, str(temp_date_int)+ "_"+ str(temp_pct) ]  = obj_factor["df_factor"].loc[index_series,"weight_f"]  
            ###      

        else :
            temp_f_value_pre = df_factor[temp_date_int].quantile( temp_pct_pre )
            ### 设置加权方式：1，等权重；2，因子大小加权；3，...
            df_factor[temp_pct] = df_factor[temp_date_int].apply(lambda x : 1 if ( x <= temp_f_value and x > temp_f_value_pre ) else 0  ) 
            index_series = df_factor[ df_factor[temp_pct]==1 ].index
            ### 1，等权重
            # df_factor.loc[index_series,"weight_f_"+ str(temp_pct)] = df_factor.loc[index_series,temp_pct] /len( index_series )

            ### 2，因子大小加权:把区间内的因子值划分为5档权重
            obj_factor ={}
            obj_factor["col_name"] = temp_date_int
            obj_factor["df_factor"] = df_factor[ df_factor[temp_pct]== 1 ]
            obj_factor["list_step"] = list_step
            obj_factor = get_weight( obj_factor )
            df_weight.loc[index_series, str(temp_date_int)+ "_"+ str(temp_pct) ]  = obj_factor["df_factor"].loc[index_series,"weight_f"]  
            ###  
        
        #####################################################################
        ### 计算当前分组的收益率：等权重
        # ret_series = df_return.loc[index_series, temp_date_int ] * df_factor.loc[index_series,temp_pct] 
        # ret_series = df_return.loc[index_series, temp_date_int ] * df_factor.loc[index_series, "weight_f_"+ str(temp_pct) ] 
        ret_series = df_return.loc[index_series, temp_date_int ] * df_weight.loc[index_series, str(temp_date_int)+ "_"+ str(temp_pct)  ] 

        #####################################################################
        ### 赋值给组合收益
        print("temp_date_int  ",temp_date_int ,round(ret_series.sum()*100,2 )  ) 
        df_ret_port.loc[temp_date_int, temp_pct ] = ret_series.sum()
        # type(temp_ret_port) = <class 'pandas.core.series.Series'>
        # 选出了100个股票，例：500个样本数据：收益率最高 0.1002，最低 -0.096； 分组的100个股票:收益率最高 0.08569，最低 -0.04338
        
        ### 
        temp_pct_pre =temp_pct
    
    #####################################################################
    ### save to csv 
    if int(temp_date_int) % 20 == 0 :
        df_factor.to_csv( path_data_output_ID +  file_output_factor  )
        df_ret_port.to_csv( path_data_output_ID + file_output_ret_port  )
        df_weight.to_csv( path_data_output_ID +  file_output_weight  )
    
### save to csv 
df_factor.to_csv( path_data_output_ID + file_output_factor  )
df_ret_port.to_csv( path_data_output_ID + file_output_ret_port  )
df_weight.to_csv( path_data_output_ID +  file_output_weight  )

asd


#####################################################################
### 3，按年度统计收益率
# TODO
### 导入收益率数据文件 y_10.npy 








































































#####################################################################
