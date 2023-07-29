# -*- coding: utf-8 -*-
__author__ = " ruoyu.Cheng"
"""
todo
改造 20201114 

功能：1，


last 20201116 | since 20201114

derived from train_model_v3.py
"""
#####################################################################
### 1，导入keras和tensorflow相关模块；配置

from keras.layers import Input, Dense, Flatten, BatchNormalization, concatenate, Dropout, LSTM, GRU
from keras import regularizers
from keras.models import Model
import numpy as np
from custom_layer import *
# standardize_and_weight:计算输入变量的标准化值，N(0,1), 二分法对中位数上下的数值分别取2和1
from util import pearson_r, standardize_and_weight
from keras import optimizers
from keras.callbacks import EarlyStopping
from scipy import stats
import tensorflow as tf
import pandas as pd
import h5py

path0 = "D:\\CISS_db\\outside_sellside\\AI_alphanet_huatai\\"
path_zz500 = path0+ "\\ZZ500\\"
path_data = path0+ "\\data2\\"

###
from keras.engine import Layer
from keras import backend as K
import itertools
from keras.initializers import Ones, Zeros
import tensorflow as tf
from keras import initializers

#####################################################################
### add_weight源码
def add_weight(self,
                   name,
                   shape,
                   dtype=None,
                   initializer=None,
                   regularizer=None,
                   trainable=True,
                   constraint=None):
    """Adds a weight variable to the layer.
    # Arguments
        name: String, the name for the weight variable.
        shape: The shape tuple of the weight.
        dtype: The dtype of the weight.
        initializer: An Initializer instance (callable).
        regularizer: An optional Regularizer instance.
        trainable: A boolean, whether the weight should
            be trained via backprop or not (assuming
            that the layer itself is also trainable).
        constraint: An optional Constraint instance.
    # Returns
        The created weight variable.
    """
    initializer = initializers.get(initializer)
    if dtype is None:
        dtype = K.floatx()
    weight = K.variable(initializer(shape),
                        dtype=dtype,
                        name=name,
                        constraint=constraint)
    if regularizer is not None:
        self.add_loss(regularizer(weight))
    if trainable:
        self._trainable_weights.append(weight)
    else:
        self._non_trainable_weights.append(weight)
    return weight

#####################################################################
### BatchNormalization() 实例源码??
# url=http://codingdict.com/sources/py/keras.layers/6202.html
class batch_normalization_1():
    def __init__(self, input_shape, lr=0.01, n_layers=2, n_hidden=8, rate_dropout=0.2, loss='risk_estimation'):
        print("initializing..., learing rate %s, n_layers %s, n_hidden %s, dropout rate %s." %(lr, n_layers, n_hidden, rate_dropout))
        self.model = Sequential()
        self.model.add(Dropout(rate=rate_dropout, input_shape=(input_shape[0], input_shape[1])))
        for i in range(0, n_layers - 1):
            self.model.add(LSTM(n_hidden * 4, return_sequences=True, activation='tanh',
                                recurrent_activation='hard_sigmoid', kernel_initializer='glorot_uniform',
                                recurrent_initializer='orthogonal', bias_initializer='zeros',
                                dropout=rate_dropout, recurrent_dropout=rate_dropout))
        self.model.add(LSTM(n_hidden, return_sequences=False, activation='tanh',
                                recurrent_activation='hard_sigmoid', kernel_initializer='glorot_uniform',
                                recurrent_initializer='orthogonal', bias_initializer='zeros',
                                dropout=rate_dropout, recurrent_dropout=rate_dropout))
        self.model.add(Dense(1, kernel_initializer=initializers.glorot_uniform()))
        # self.model.add(BatchNormalization(axis=-1, moving_mean_initializer=Constant(value=0.5),
        #               moving_variance_initializer=Constant(value=0.25)))
        self.model.add(BatchRenormalization(axis=-1, beta_init=Constant(value=0.5)))
        self.model.add(Activation('relu_limited'))
        opt = RMSprop(lr=lr)
        self.model.compile(loss=loss,
                      optimizer=opt,
                      metrics=['accuracy'])

#####################################################################
### Keras model.fit() 函数
rr= fit(x=None, y=None, batch_size=None, epochs=1, verbose=1, callbacks=None, 
validation_split=0.0, validation_data=None, shuffle=True, class_weight=None, 
sample_weight=None, initial_epoch=0, steps_per_epoch=None, validation_steps=None, validation_freq=1)

'''
x：输入数据。如果模型只有一个输入，那么x的类型是numpy
array，如果模型有多个输入，那么x的类型应当为list，list的元素是对应于各个输入的numpy array
y：标签，numpy array
batch_size：整数，指定进行梯度下降时每个batch包含的样本数。训练时一个batch的样本会被计算一次梯度下降，使目标函数优化一步。
epochs：整数，训练终止时的epoch值，训练将在达到该epoch值时停止，当没有设置initial_epoch时，它就是训练的总轮数，否则训练的总轮数为
epochs - inital_epoch
verbose：日志显示，0为不在标准输出流输出日志信息，1为输出进度条记录，2为每个epoch输出一行记录
callbacks：list，其中的元素是keras.callbacks.Callback的对象。这个list中的回调函数将会在训练过程中的适当时机被调用，参考回调函数
validation_split：0~1之间的浮点数，用来指定训练集的一定比例数据作为验证集。验证集将不参与训练，并在每个epoch结束后测试的模型的指标，
如损失函数、精确度等。注意，validation_split的划分在shuffle之前，因此如果你的数据本身是有序的，需要先手工打乱再指定validation_split，
否则可能会出现验证集样本不均匀。
validation_data：形式为（X，y）的tuple，是指定的验证集。此参数将覆盖validation_spilt。
shuffle：布尔值或字符串，一般为布尔值，表示是否在训练过程中随机打乱输入样本的顺序。若为字符串“batch”，则是用来处理HDF5数据的特殊情况，
它将在batch内部将数据打乱。
class_weight：字典，将不同的类别映射为不同的权值，该参数用来在训练过程中调整损失函数（只能用于训练）
sample_weight：权值的numpy array，用于在训练时调整损失函数（仅用于训练）。可以传递一个1D的与样本等长的向量用于对样本进行1对1的加权，或者在
面对时序数据时，传递一个的形式为（samples，sequence_length）的矩阵来为每个时间步上的样本赋不同的权。这种情况下请确定在编译模型时添加了
sample_weight_mode=’temporal’。
initial_epoch: 从该参数指定的epoch开始训练，在继续之前的训练时有用。
fit函数返回一个History的对象，其History.history属性记录了损失函数和其他指标的数值随epoch变化的情况，如果有验证集的话，也包含了验证集的这些
指标变化情况

作者：默写年华Antifragile
链接：https://www.jianshu.com/p/9ba27074044f  
'''
### 返回;一个 History 对象。其 History.history 属性是连续 epoch 训练损失和评估值，以及验证集损失和评估值的记录（如果适用）
# {‘val_loss’: [2.095771548461914, 1.9237295974731445], 
# ‘val_accuracy’: [0.21889999508857727, 0.29760000109672546], 
# ‘loss’: [2.2294568004608153, 1.9951313903808594], 
# ‘accuracy’: [0.1602, 0.2692]}
































