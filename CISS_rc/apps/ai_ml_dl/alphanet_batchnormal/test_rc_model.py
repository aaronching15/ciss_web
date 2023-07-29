# -*- coding: utf-8 -*-
__author__ = " ruoyu.Cheng"
"""
todo
1，考虑如何将其改造为基金持仓股票分析的模板

功能：0，是test_rc_train_model_v3.py 的下一步
1，导入最优参数.h5 文件，并对不同区间因子数据计算出预测值y_pred 
2，
notes:
date_list只有10几个日期的原因是：每个代表一个日期的模型，现在是训练一个模型后用半年再重新训练

last  | since 20201201
derived from test_model.py
"""

from keras.models import load_model
from custom_layer import *
from util import *
import numpy as np
from scipy import stats
import pandas as pd
import scipy.io as sio
import h5py

import matplotlib.pyplot as plt

import os 


#####################################################################
### time
### time
import datetime as dt 
# 20201129_161059
temp_date = dt.datetime.strftime( dt.datetime.now(),"%Y%m%d_%H%M%S")
print("Curent time is ", temp_date)
input_ID = input("type ID name for this training，example=20201201_132913_4126.h5...")
if len( input_ID) <12:
    input_ID = "20201201_132913"
#####################################################################
### 设置模型位置
# model_path = 'model10d/regress10/1/' 'model20d/regress6/' 'model5d/regress1/'
# 'model10d/v2/' ; 'model10d/v3/'

path0 = "D:\\CISS_db\\outside_sellside\\AI_alphanet_huatai\\"
path_zz500 = path0+ "\\ZZ500\\"
path_data = path0+ "\\data2\\"

# path_model_10d_v3 = path_zz500 + "model10d\\v3\\"
path_v3 = path_zz500 + "model10d\\v3\\"

### 模型的保存目录
path_model_output_ID = path_v3+"model_" + input_ID  + "\\"

### 输出数据、统计指标的保存目录
path_data_output_ID = path_v3+"output_" + input_ID  + "\\"
if not os.path.exists(path_data_output_ID ) :
    os.mkdir(path_data_output_ID )

#####################################################################
dependencies = {
    'pearson_r': pearson_r,
    
    'ts_cov3':ts_cov3,
    'ts_corr3':ts_corr3,
    'ts_std3':ts_std3,
    'ts_mean3':ts_mean3,
    'ts_sum3':ts_sum3,
    'ts_max3':ts_max3,
    'ts_min3':ts_min3,
    'ts_zscore3':ts_zscore3,
    'ts_return3':ts_return3,
    'ts_decay_linear3':ts_decay_linear3,
    
    'ts_cov5':ts_cov5,
    'ts_corr5':ts_corr5,
    'ts_std5':ts_std5,
    'ts_mean5':ts_mean5,
    'ts_sum5':ts_sum5,
    'ts_max5':ts_max5,
    'ts_min5':ts_min5,
    'ts_zscore5':ts_zscore5,
    'ts_return5':ts_return5,
    'ts_decay_linear5':ts_decay_linear5,
    
    'ts_cov10':ts_cov10,
    'ts_corr10':ts_corr10,
    'ts_std10':ts_std10,
    'ts_mean10':ts_mean10,
    'ts_sum10':ts_sum10,
    'ts_max10':ts_max10,
    'ts_min10':ts_min10,
    'ts_zscore10':ts_zscore10,
    'ts_return10':ts_return10,
    'ts_decay_linear10':ts_decay_linear10,
    'LayerNormalization':LayerNormalization,
    
    'pearson_r_loss':pearson_r_loss,
}

### 读取中证500数据文件，里边记得是0和1取值的矩阵
f = h5py.File( path_zz500+'ZZ500.mat')
# f.keys() =KeysViewHDF5 ['index_mat']

### 标识成分股：index_loc每1列之和都是501
index_loc = f['index_mat'].value.T

### X是股票的因子时间序列数据
X = np.load( path_data + 'X.npy')
X = np.swapaxes(X,0,1)
### Y是股票的收益率时间序列数据，收益率值在 -6.5 ~ +11.4，不是绝对的百分位数据
Y = np.load( path_data + 'y_10.npy')
### 标识成分股：只取
Y = Y*index_loc

print('data loaded')
### note:和train_model里的不一样。
### load_data 是按照30天窗口，把全量数据表X和Y里
def load_data(end_date):    
    ### 根据截至日期，导入因子数据和预测数据
    end_date = end_date+1
    len_date = 30
    ### 取t-30 - t 日
    start_date = end_date-len_date 
    x = X[:,start_date:end_date,:]
    y = Y[:,end_date-1]
    ind = index_loc[:,end_date-1]
    ### x.shape, y.shape: (3989, 30, 15) (3989,) 
    x_in_sample = np.zeros(x.shape)
    y_in_sample = np.zeros(y.shape)
    nonan_index = []
    
    ### n_sample是股票样本数量，截至2020-7是3989
    n_sample = 0
    for i in range(y.shape[0]):
        x_one = x[i, :, :]
        y_one = y[i]
        i_one = ind[i]
        
        ### nonan_index 对应有数值的index 
        if np.isnan(x_one).any() or np.isnan(i_one).any() :
            continue
        
        x_in_sample[n_sample, :,:] = x_one
        y_in_sample[n_sample] = y_one
        nonan_index.append(i)
        n_sample += 1
    
    ### 数据长度 3989
    x_in_sample = x_in_sample[:n_sample, :]
    y_in_sample = y_in_sample[:n_sample]    
    return x_in_sample, y_in_sample, nonan_index


### sio访问.mat文件；import scipy.io as sio 
### notes:dailyinfo_dates.mat里是matlab格式的日期数据要和excel的进行转换
# 数字来表示日期，2010年10月4日，在Excel中为40455，在Matlab中为734415，在SQL里为40453。故对于一般的日期（1900-03-01以后）有以下关系式：
# Matlab_datetime = Excel_datetime + 693960;SQL_datetime = Excel_datetime - 2;Matlab_datetime = SQL_datetime + 693962;
date_mat = sio.loadmat(path_zz500+'dailyinfo_dates.mat')['dailyinfo_dates']
date_mat = np.append(date_mat, date_mat[-1][-1]+3)
trade_date_df = pd.DataFrame(np.nan * np.zeros((2,1000)))
# trade_date_df.shape= (2, 1000)

### 仅仅测试部分日期：每个代表一个日期的模型，现在是训练一个模型后用半年再重新训练
# Qs:X.shape[1] 在.h5文件中不存在
# date_list = [X.shape[1], 5342, 5220, 5099]
### 
# date_list = [X.shape[1], 5342, 5220, 5099, 4977, 4857, 4735, 4613, 4491, 4370, 4248, 4126, 4004, 3883, 3761, 3644, 3522, 3403, 3281, 3160]
###
date_list = [X.shape[1],5342, 5220, 5099, 4977, 4857, 4735, 4613, 4491, 4370, 4248, 4126, 4004, 3883, 3761, 3644, 3522, 3403, 3281, 3160]

date_list.reverse()
i_list=0
i_panel=0

### 新建 df_stat,包括 y_true, y_pred[:,-1])[0]，rank_ic, ic_list
df_stat = pd.DataFrame( columns=["y_true","y_pred","rank_ic","ic_list"] )

### 将日期序列转化为list
data_index = list(sio.loadmat( path_zz500+ 'dailyinfo_dates.mat')['week_index'][0])
day_test_list = data_index[data_index.index(date_list[0]+1):data_index.index(X.shape[1])]

############################################################################
### 创建因子数据fac_1, ic指标list变量
rank_ic_list = []
ic_list = []
### fac_1.shape = (3989, 1000)
# fac_1 = pd.DataFrame(np.nan * np.zeros((X.shape[0],1000)))
fac_1 = pd.DataFrame(np.nan * np.zeros((X.shape[0], len(day_test_list) ) ), columns=  day_test_list  )

# day_test_list length 474 ; day_test_list 取值是基于date_list;
print("day_test_list length", len(day_test_list) ) 

### date是day_test_list每一个交易日：
### date是遍历470多个周，但是对应的预测模型是每季度，或者每半年才预测一次
for date in day_test_list: 
    ### date是前一交易日，date是最新交易日
    date_pre = date - 1   
    ####################################################################
    ### 定期导入模型文件，Load model file; 半年内都使用同一个模型，因为每半年才预测一次
    # 例子：date = date_list[i_list] = 3160  
    if not (date_pre >= date_list[i_list] and date_pre < date_list[i_list+1]):  
        i_list+=1
    
        ### 根据ID输入前缀;such as 20201201_132913_4126.h5 or 4126.h5
        file_name =input_ID +"_" + str(date_list[i_list]) + '.h5' 
        # print('--------------------------------')
        ### 导入模型配置数据 
        # model = load_model(path_data_model + file_name, custom_objects=dependencies) 
        model = load_model(path_model_output_ID  + file_name, custom_objects=dependencies) 
    else :
        ### 判断一个变量model是否存在："aa" in locals().keys() ||  "aa" in dir()
        if not "model" in dir() :
            file_name =input_ID +"_" + str(date_list[i_list]) + '.h5' 
            print( path_model_output_ID  , file_name  )
            # model = load_model(path_data_model + file_name, custom_objects=dependencies) 
            model = load_model(path_model_output_ID  + file_name, custom_objects=dependencies) 
    
    print("Date==",date,date_pre, i_list,i_panel)
    
    ####################################################################           
    ### 根据前一交易日date_pre，获取因子数据x和当期收益率数据y       
    x_test, y_true, nonan_index = load_data(date_pre)      
    
    ### y_pred 是393个股票的百分比预测值，y_pred的值在 -0.49 ~ 0.385 之间；
    y_pred = model.predict(x_test)
    
    ### nonan_index 对应有数值的index，len(nonan_index)=500
    ### i_panel的数值从0，1增加到73
    ### y_pred[:,-1]对应的是所有股票的最后1天的预测收益率， len(y_pred[:,-1])=500 
    # fac_1.iloc[nonan_index,i_panel] = y_pred[:,-1]
    # notes: y_pred基于截至date-1的数据，预测的是date日收益率
    fac_1.loc[nonan_index, date ] = y_pred[:,-1]
    
    ### trade_date_df没啥用，计划删了
    # date_pre=3165 ,date_mat[date_pre]=734545，=date_mat[date_pre+1] 734548    
    trade_date_df.iloc[0,i_panel] = date_mat[date_pre]
    trade_date_df.iloc[1,i_panel] = date_mat[date_pre+1]
    
    ### 统计指标：
    ### y_true和 y_pred的数据长度=3989
    df_stat.loc[date, "y_true_ave"] = np.mean(y_true)
    df_stat.loc[date,"y_pred_ave"] = np.mean( y_pred[:,-1] )
    if not np.isnan(y_true).any():
        rank_ic_list.append(stats.spearmanr(y_true, y_pred[:,-1])[0])
        ic_list.append(stats.pearsonr(y_true, y_pred[:,-1])[0])

        df_stat.loc[date, "rank_ic"] = stats.spearmanr(y_true, y_pred[:,-1])[0]
        # 皮尔森相关系数
        df_stat.loc[date, "ic_list"] = stats.pearsonr(y_true, y_pred[:,-1])[0]
    
    ### 计算rank_ic_cumsum
    df_stat.loc[date, "rank_ic_cumsum"] = np.cumsum(rank_ic_list)[-1]
    ### rank_ic_list，计算
    # print(i_panel,"Mean rank_ic:",np.mean(rank_ic_list),"Mean ic:", np.mean(ic_list))

    #############################################################
    ###  




    #############################################################
    ### save to csv 
    # fac_1 = fac_1.iloc[:,:i_panel] 
    # trade_date_df = trade_date_df.iloc[:,:i_panel]       
    ### 日期转换：matlab to excel = Matlab_datetime = Excel_datetime + 693960
    # trade_date_df_out = trade_date_df.iloc[:,:i_panel] - 693960
    ### 每100个交易日导出一次    
    if i_panel % 100 == 0 :
        trade_date_df_out = trade_date_df - 693960
        # fac_1.to_csv(path_data_output_ID + input_ID+'_fac1.csv', index=None, header=None)
        fac_1.to_csv(path_data_output_ID + input_ID+'_fac1.csv' )
        # ,header=False,index=Falses
        trade_date_df_out.to_csv(  path_data_output_ID +  input_ID+'_trade_date.csv')
        df_stat.to_csv(path_data_output_ID +  input_ID+ "_df_stat.csv" )
        print("output dir:", path_data_output_ID + file_name )

    i_panel+=1      
    ###

#############################################################
### final output
trade_date_df_out = trade_date_df - 693960
# fac_1.to_csv(path_data_output_ID + input_ID+'_fac1.csv', index=None, header=None)
fac_1.to_csv(path_data_output_ID + input_ID+'_fac1.csv' )
trade_date_df_out.to_csv(  path_data_output_ID +  input_ID+'_trade_date.csv' )
df_stat.to_csv(path_data_output_ID +  input_ID+ "_df_stat.csv" )
print("output dir:",path_data_output_ID + file_name )

### 统计不同截面的相关系数
temp=0
fac_1=fac_1.dropna()
for i in range(fac_1.shape[1]-1):
    temp+=stats.pearsonr(fac_1.iloc[:,i],fac_1.iloc[:,i+1])[0]
    print(i,'相关系数:',temp/(i+1))

### plot 
df_stat["rank_ic_cumsum"].plot()
plt.show()

