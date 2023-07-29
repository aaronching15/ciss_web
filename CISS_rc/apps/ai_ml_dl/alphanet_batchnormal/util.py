# -*- coding: utf-8 -*-
__author__ = " ruoyu.Cheng"
"""
todo：

last || since 20201115



derived from Apr 202,author: lzy
"""

#####################################################################
### 1，导入keras和tensorflow相关模块；配置
import keras.backend as K
import tensorflow as tf
import numpy as np
import pandas as pd

#####################################################################
### 
def standardize_x(x_in_sample):
    x_in_sample_out = x_in_sample.copy()
    shape = x_in_sample.shape[1]
    for i in range(x_in_sample.shape[2]):
        factor_input = x_in_sample[:, :, i].reshape(-1)
        factor_output = (factor_input - np.nanmean(factor_input))/(np.nanstd(factor_input)+1e-4)
        x_in_sample_out[:,:,i] = factor_output.reshape(-1,shape)
    return x_in_sample_out

def standardize_and_weight(factor_input):
    ### 
    ### 计算输入变量的标准化值，N(0,1)
    factor_output = (factor_input - np.nanmean(factor_input))/np.nanstd(factor_input)
    weight = factor_output.copy()
    ### 二分法对中位数上下的数值分别取2和1
    weight[factor_output >= np.nanmedian(factor_output)] = 2
    weight[factor_output < np.nanmedian(factor_output)] = 1
    return factor_output, weight

def multi_bin_label(factor_input):
    factor_output = factor_input.copy()
#    factor_output[factor_input >= np.quantile(factor_input, 0.8)] = 2
#    factor_output[(factor_input >= np.quantile(factor_input, 0.6)) & (factor_input < np.quantile(factor_input, 0.8))] = 1
#    factor_output[(factor_input >= np.quantile(factor_input, 0.4)) & (factor_input < np.quantile(factor_input, 0.6))] = 0
#    factor_output[(factor_input >= np.quantile(factor_input, 0.2)) & (factor_input < np.quantile(factor_input, 0.4))] = -1
#    factor_output[factor_input < np.quantile(factor_input, 0.2)] = -2
    factor_output[factor_input >= np.quantile(factor_input, 0.75)] = 3
    factor_output[(factor_input >= np.quantile(factor_input, 0.5)) & (factor_input < np.quantile(factor_input, 0.75))] = 1
    factor_output[(factor_input >= np.quantile(factor_input, 0.25)) & (factor_input < np.quantile(factor_input, 0.5))] = -1
    factor_output[factor_input < np.quantile(factor_input, 0.25)] = -3
    
    weight = factor_output.copy()
    return factor_output, weight
    

def bin_label(factor_input):
    factor_output = factor_input.copy()
    factor_output[factor_input >= np.nanmedian(factor_input)] = 1
    factor_output[factor_input < np.nanmedian(factor_input)] = 0
    weight = factor_output.copy()
    #weight[factor_input >= np.nanmedian(factor_input)] = 1.5
    #weight[factor_input < np.nanmedian(factor_input)] = 1
    return factor_output, weight


def standardize(factor_input):
    factor_output = factor_input.copy()
    for i in range(0,factor_input.shape[1]):
        factor_output[:,i] = (factor_input[:,i] - np.nanmean(factor_input[:,i]))/np.nanstd(factor_input[:,i])
    return factor_output

def auc(y_true, y_pred):
    auc = tf.metrics.auc(y_true, y_pred)[1]
    K.get_session().run(tf.local_variables_initializer())
    return auc

def pearson_r(y_true, y_pred):
    ### pearson相关系数衡量的是线性相关关系。若r=0，只能说x与y之间无线性相关关系，不能说无相关关系。
    # 相关系数的绝对值越大，相关性越强：相关系数越接近于1或-1，相关度越强
    x = y_true
    y = y_pred
    ### keras.backend.mean(x, axis=None, keepdims=False)；张量在某一指定轴的均值。
    mx = K.mean(x, axis=0)
    my = K.mean(y, axis=0)
    xm, ym = x - mx, y - my
    r_num = K.sum(xm * ym)
    x_square_sum = K.sum(xm * xm)
    y_square_sum = K.sum(ym * ym)
    r_den = K.sqrt(x_square_sum * y_square_sum)
    r = r_num / r_den
    return K.mean(r)

def pearson_r_loss(y_true, y_pred):
    x = y_true
    y = y_pred
    mx = K.mean(x, axis=0)
    my = K.mean(y, axis=0)
    xm, ym = x - mx, y - my
    r_num = K.sum(xm * ym)
    x_square_sum = K.sum(xm * xm)
    y_square_sum = K.sum(ym * ym)
    r_den = K.sqrt(x_square_sum * y_square_sum)
    r = r_num / r_den
    return -K.mean(r)


def ts_corr(x1,x2,window):
    ### 计算correlation(x1,x2)
    ### window是变量x1,x2中第二个维度内部的区间长度
    tmparray = []
    def corr_np(x, y):
        ### 计算x和y的标准差
        std_x = np.std(x,axis=1) + 0.00001
        std_y = np.std(y,axis=1) + 0.00001

        x_mul_y = x * y
        E_x_mul_y = np.mean(x_mul_y, axis=1)
        mean_x = np.mean(x, axis=1)
        mean_y = np.mean(y, axis=1)
        cov = E_x_mul_y - mean_x * mean_y

        out = cov / (std_x * std_y)
        return out
    
    ### 对第二个维度的滚动区间矩阵计算相关关系
    for k in range (0, x1.shape[1]-window+1):
        data1 = x1[:, k:window + k]
        data2 = x2[:, k:window + k] 
        ###           
        tmparray.append(corr_np(data1, data2))
        # print("tmparray ",type(tmparray), tmparray )
        
        ### np.stack：对指定axis增加维度
        ### 例子：x1是(3*3)，x2是(3*3)，y,注意x1和x2有相同维度
        # y:(2,3,3) if =np.stack((x1,x2),axis=0)
        # y:(3,2,3) if =np.stack((x1,x2),axis=1)
        # y:(3,3,2) if =np.stack((x1,x2),axis=2)
        out=np.stack(tmparray,axis=1)     
    return out

def ts_stddev(x1, window):
    interval = x1.shape[1]
    tmparray=[]
    def _df_ts_stddev(k):   
        return np.nanstd(x1[:, k:window + k], axis=1, ddof=1)
    for k in range(0, interval-window+1):
        tmparray.append(_df_ts_stddev(k))
    out=np.stack(tmparray,axis=1) 
    return out

def prepare_data(X,y):
    close = X[:,:,3]
    volume = X[:,:,5]
    free_turn = X[:,:,[0,2,3,4,5,6,7,8]]
    alpha1 = ts_corr(close, volume, 10)
    alpha2 = ts_stddev(free_turn, 10)
    alpha1_s = standardize(alpha1)
    alpha2_s = standardize(alpha2)    
    alpha2_s = np.swapaxes(alpha2_s, 1, 2)[:,:,0]
    return alpha1_s, alpha2_s, y
