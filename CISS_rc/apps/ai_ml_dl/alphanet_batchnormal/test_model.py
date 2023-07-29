# -*- coding: utf-8 -*-
"""
Created on Tue Apr  7 17:32:17 2020

@author: lzy
"""

from keras.models import load_model
from custom_layer import *
from util import *
import numpy as np
from scipy import stats
import pandas as pd
import scipy.io as sio
import h5py

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

f = h5py.File('ZZ500.mat')
index_loc = f['index_mat'].value.T

X = np.load('../data2/X.npy')
X = np.swapaxes(X,0,1)
Y = np.load('../data2/y_10.npy')
Y = Y*index_loc

print('data loaded')
### note_rc
### load_data 是按照30天窗口，把全量数据表X和Y里
def load_data(end_date):    
    end_date = end_date+1
    len_date = 30

    start_date = end_date-len_date
    x = X[:,start_date:end_date,:]
    y = Y[:,end_date-1]
    ind = index_loc[:,end_date-1]
        
    x_in_sample = np.zeros(x.shape)
    y_in_sample = np.zeros(y.shape)
    nonan_index = []
    
    n_sample = 0
    for i in range(y.shape[0]):
        x_one = x[i, :, :]
        y_one = y[i]
        i_one = ind[i]
        
        if np.isnan(x_one).any() or np.isnan(i_one).any() :
            continue
        
        x_in_sample[n_sample, :,:] = x_one
        y_in_sample[n_sample] = y_one
        nonan_index.append(i)
        n_sample += 1
    
    x_in_sample = x_in_sample[:n_sample, :]
    y_in_sample = y_in_sample[:n_sample]    
    return x_in_sample, y_in_sample, nonan_index


rank_ic_list = []
ic_list = []
fac_1 = pd.DataFrame(np.nan * np.zeros((X.shape[0],1000)))
date_mat = sio.loadmat('dailyinfo_dates.mat')['dailyinfo_dates']
date_mat = np.append(date_mat, date_mat[-1][-1]+3)
trade_date_df = pd.DataFrame(np.nan * np.zeros((2,1000)))

#model_path = 'model10d/regress10/1/'
model_path = 'model10d/v2/'
#model_path = 'model20d/regress6/'
#model_path = 'model5d/regress1/'

#end_list = [X.shape[1], 5342, 5220, 5099]
end_list = [X.shape[1], 5342, 5220, 5099, 4977, 4857, 4735, 4613, 4491, 4370, 4248, 4126, 4004, 3883, 3761, 3644, 3522, 3403, 3281, 3160]
end_list.reverse()
i_list=0
i_panel=0


data_index = list(sio.loadmat('../dailyinfo_dates.mat')['week_index'][0])
day_test_list = data_index[data_index.index(end_list[0]+1):data_index.index(X.shape[1])]

model = load_model(model_path + str(end_list[0]) + '.h5', custom_objects=dependencies) 

for date in day_test_list:
    print(date)
    end = date - 1   
    if not (end >= end_list[i_list] and end < end_list[i_list+1]):  
        i_list+=1
        print('--------------------------------')
        print(model_path + str(end_list[i_list]) + '.h5')
        print('--------------------------------')

        model = load_model(model_path + str(end_list[i_list]) + '.h5', custom_objects=dependencies) 
        
        
        
    
    x_test, y_true, nonan_index = load_data(end)      
    y_pred=model.predict(x_test)
    fac_1.iloc[nonan_index,i_panel] = y_pred[:,-1]
    
    trade_date_df.iloc[0,i_panel] = date_mat[end]
    trade_date_df.iloc[1,i_panel] = date_mat[end+1]
    
    if not np.isnan(y_true).any():
        rank_ic_list.append(stats.spearmanr(y_true, y_pred[:,-1])[0])
        ic_list.append(stats.pearsonr(y_true, y_pred[:,-1])[0])
    
    print(np.mean(rank_ic_list))
    i_panel+=1    
 
fac_1 = fac_1.iloc[:,:i_panel]
trade_date_df = trade_date_df.iloc[:,:i_panel]       

fac_1.to_csv(model_path + 'fac1.csv', index=None, header=None)
trade_date_df.to_csv(model_path + 'trade_date.csv',header=False,index=False)


#统计不同截面的相关系数
temp=0
fac_1=fac_1.dropna()
for i in range(fac_1.shape[1]-1):
    temp+=stats.pearsonr(fac_1.iloc[:,i],fac_1.iloc[:,i+1])[0]
print('相关系数:',temp/(i+1))

df = pd.DataFrame()
df['cumsum'] = np.cumsum(rank_ic_list)
df.plot()