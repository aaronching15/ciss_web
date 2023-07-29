# -*- coding: utf-8 -*-
"""
Created on Mon Apr  6 16:21:50 2020

@author: lzy
"""

from keras.layers import Input, Dense, Flatten, BatchNormalization, concatenate, Dropout, LSTM, GRU
from keras import regularizers
from keras.models import Model
import numpy as np
from custom_layer import *
from util import pearson_r, standardize_and_weight
from keras import optimizers
from keras.callbacks import EarlyStopping
from scipy import stats
import tensorflow as tf
import pandas as pd
import h5py

len_date = 30


f = h5py.File('ZZ500.mat')
index_loc = f['index_mat'].value.T

X = np.load('../data2/X.npy')
X = np.swapaxes(X, 0, 1)
Y = np.load('../data2/y_10.npy')
Y = Y*index_loc


def load_data(end_date):
    end_date1 = end_date - 15
    start_date = end_date - 1500
    x = X[:, start_date:end_date1, :]
    y = Y[:, start_date:end_date1]

    step = 1
    x_in_sample = np.zeros((int(x.shape[0] * x.shape[1] / 3), len_date, x.shape[2]))
    y_in_sample = np.zeros((int(y.shape[0] * y.shape[1] / 3), 1))
    w_in_sample = np.zeros((int(y.shape[0] * y.shape[1] / 3), 1))

    n_sample = 0
    for j in range(0, y.shape[1] - len_date + 1, step):
        s_index = n_sample
        for i in range(y.shape[0]):
            x_one = x[i, j:j + len_date]
            y_one = y[i, j + len_date - 1]

            if np.isnan(x_one).any() or np.isnan(y_one).any():
                continue

            x_in_sample[n_sample, :, :] = x_one
            y_in_sample[n_sample, :] = y_one
            n_sample += 1
        e_index = n_sample
        y_in_sample[s_index:e_index, 0], w_in_sample[s_index:e_index,0] = standardize_and_weight(y_in_sample[s_index:e_index, 0])

    x_in_sample = x_in_sample[:n_sample, :]
    y_in_sample = y_in_sample[:n_sample, :]
    w_in_sample = w_in_sample[:n_sample, ]

    split = int(y_in_sample.shape[0] * 0.8)
    x_train = x_in_sample[:split, :, :]
    x_val = x_in_sample[split:, :, :]
    y_train = y_in_sample[:split, :]
    y_val = y_in_sample[split:, :]
    w_train = w_in_sample[:split, 0]
    w_val = w_in_sample[split:, 0]
    return x_train, x_val, y_train, y_val, w_train, w_val




#end_list = [5342, 5220, 5099, 4977, 4857, 4735, 4613, 4491, 4370, 4248, 4126, 4004, 3883, 3761, 3644, 3522, 3403, 3281, 3160]
end_list = [5342]

df_val_loss = pd.DataFrame()
df_val_ic = pd.DataFrame()
df_weight = pd.DataFrame()

for end in end_list:
    x_train, x_val, y_train, y_val, w_train, w_val = load_data(end)
    print('data finish')
    
    
    input_1 = Input(shape=(len_date,x_train.shape[2]))
    corr1 = BatchNormalization()(ts_corr10()(input_1))
    cov1 = BatchNormalization()(ts_cov10()(input_1))
    std1 = BatchNormalization()(ts_std10()(input_1))
    zscore1 = BatchNormalization()(ts_zscore10()(input_1))
    decay_linear1 = BatchNormalization()(ts_decay_linear10()(input_1))
    return1 = BatchNormalization()(ts_return10()(input_1))
    add1 = concatenate([corr1, cov1,std1,zscore1,decay_linear1,return1]) 
    hidden1 = BatchNormalization()(GRU(30, input_shape=(int(add1.shape[1]), int(add1.shape[2])))(add1))
    
    corr2 = BatchNormalization()(ts_corr5()(input_1))
    cov2 = BatchNormalization()(ts_cov5()(input_1))
    std2 = BatchNormalization()(ts_std5()(input_1))
    zscore2 = BatchNormalization()(ts_zscore5()(input_1))
    decay_linear2 = BatchNormalization()(ts_decay_linear5()(input_1))
    return2 = BatchNormalization()(ts_return5()(input_1))
    add2 = concatenate([corr2, cov2,std2,zscore2,decay_linear2,return2])
    hidden2 = BatchNormalization()(GRU(30, input_shape=(int(add2.shape[1]), int(add2.shape[2])))(add2))
    
    hidden3 = concatenate([hidden1, hidden2])    
    hidden = Dense(1, kernel_initializer='truncated_normal', activation='linear')(hidden3)
    # notes：该模型没用到池化层ts_mean,ts_max,ts_min;是不是还不涉及到这几个函数
    
    ###
    ### 模型定义
    model = Model(inputs=input_1, outputs=hidden)
    model.summary()

    ### Adam()是解决这个问题的一个方案。其大概的思想是开始的学习率设置为一个较大的值，
    # 然后根据次数的增多，动态的减小学习率，以实现效率和效果的兼得。
    #opt = optimizers.RMSprop(lr=0.0001)
    ### lr：float> = 0.学习率;decay：float> = 0,每次更新时学习率下降
    # 刚开始训练时：学习率以 0.01 ~ 0.001 为宜;一定轮数过后：逐渐减缓;结束衰减应该在100倍以上
    opt = optimizers.Adam(lr=0.00005)
    model.compile(loss="mse", optimizer=opt, metrics=[pearson_r])
    early_stopping = EarlyStopping(monitor='val_loss', patience=8, restore_best_weights=True)  
    hist = model.fit(x_train, y_train, batch_size=500, epochs=100, validation_data=(x_val, y_val), callbacks=[early_stopping])

    
    df_val_loss[end] = hist.history['val_loss'][-8:]
    df_val_ic[end] = hist.history['val_pearson_r'][-8:]
    df_weight[end] = model.layers[-1].get_weights()[0][:,0]
    
    model_path = 'model10d/v3/'
    model.save(model_path + str(end) + '.h5')
    
    del x_train, x_val, y_train, y_val
    


df_val_loss.to_csv(model_path + 'df_val_loss.csv')
df_val_ic.to_csv(model_path + 'df_val_ic.csv')
df_weight.to_csv(model_path + 'df_weight.csv')