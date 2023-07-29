# -*- coding: utf-8 -*-
__author__ = " ruoyu.Cheng"
"""
todo
1，考虑如何将其改造为基金持仓股票分析的模板
 
功能：
1，用历史数据训练出最优参数，并将最优参数保存至 .h5 文件

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
#
path_model_10d_v3 = path_zz500 + "model10d\\v3\\"

### 个股数据图片的长度，取30天
len_date = 30


#####################################################################
### time
import datetime as dt 
# 20201129_161059
temp_date = dt.datetime.strftime( dt.datetime.now(),"%Y%m%d_%H%M%S")
print("Curent time is ", temp_date)
input_ID = input("type ID name for this training...")

path_model_output = path_zz500 + "model10d\\v3\\"
path_model_output_ID = path_model_output+ "model_" + input_ID  + "\\"
if not os.path.exists(path_model_output_ID ) :
    os.mkdir(path_model_output_ID )

#####################################################################
### 2，导入数据和数据格式转换
# Keras也需要一个numpy数组作为输入而不是pandas数据帧.首先使用df.values将df转换为numpy数组
#####################################
### 2.1，导入中证500成分股：
# f对象内只有1个key='index_mat'；index_loc is 'numpy.ndarray'
# 变量描述\\index_loc：一个行是股票，列是日期的表格，股级列表示的是1个1个交易日；从第2187列开始，每一列取值1或"",列的和为501.
f = h5py.File( path_zz500 + 'ZZ500.mat')
# index_loc.shape = (3989, 5456) 
index_loc = f['index_mat'].value.T

#####################################
### 2.2，导入因子特征等数据
### 9+6个特征列表：open, close, high, low, volume，return1，vwap，turn, free_turn，？？？
# 个股日频开盘价、收盘价、最高价、最低价、成交量，个股日频收益率，成交量加权平均价，换手率、自由流通股换手率
# X = np.load('../data2/X.npy')
# X.shape，(5456, 3989, 15) ；
# rr.reshape((2,3,4)).transpose() 会把一个 2个矩阵，3行，4列变成4个矩阵，3行，2列

### X是股票的因子时间序列数据
X = np.load( path_data + 'X.npy')
# np.swapaxes会把前2个维度互换： X.shape (3989, 5456, 15)
X = np.swapaxes(X, 0, 1)

### Y是股票的收益率时间序列数据，收益率值在 -6.5 ~ +11.4，不是绝对的百分位数据
# Y.shape  (3989, 5456)；Y是股票10天累计收益率，且是百分比的收益率数值。
Y = np.load(path_data + 'y_10.npy')


### 使用全部数据。 

# 缩小导入数据的大小,没用
Y2 = Y[:300,:1700]
X2 = X[:300,:1700, :]
print("X2 " ,X2.shape )
print( "Y2 ",Y2.shape )

# 仅保留成分股的数值，收益率？
# Y.shape  (3989, 5456)
Y = Y*index_loc

#####################################################################
### 定义逐期数据
def load_data(end_date):
    ###  end_date 对应分析时间t,例如 5342
    ### end_date1，例：5342-15 , 5342-1500
    end_date1 = end_date - 15
    start_date = end_date - 1500

    ### 获取过去约1500-15个交易日的股票15个因子数据和股票收益率数据
    # x.shape (3989, 1485, 15), y.shape (3989, 1485)
    x = X[:, start_date:end_date1, :]
    y = Y[:, start_date:end_date1]   
    ### 对于每只股票，将其量价数据拼接成 9个因子*30天 的“数据图片”，30 为历史时间天数。
    # x.shape[0] * x.shape[1]：变量x的总数据长度= 3989* 1485，股票数量*交易日; X.shape[2]=15  
    
    ### step 是选取交易日的间隔
    step = 1 

    ### Shape of x_ , y_, w_ =   (1974555, 30, 15) (1974555, 1) (1974555, 1)
    x_in_sample = np.zeros((int(x.shape[0] * x.shape[1] / 3), len_date, x.shape[2]))
    y_in_sample = np.zeros((int(y.shape[0] * y.shape[1] / 3), 1))
    w_in_sample = np.zeros((int(y.shape[0] * y.shape[1] / 3), 1))
    
    ### n_sample：对应了给定交易日和给定股票的位置，取值范围例如 1~ 1974555
    n_sample = 0
    ### j对应交易日，从0开始，步长为1，逐步增加至 1485-30个交易日
    for j in range(0, y.shape[1] - len_date + 1, step):
        s_index = n_sample
        ### i对应所有股票，包括当期不属于成分股的股票
        for i in range(y.shape[0]):
            ### x_one,y_one 是单个股票i的数据图片：9个因子和
            x_one = x[i, j:j + len_date]
            y_one = y[i, j + len_date - 1]

            ### 避免存在包含nan空值的数据
            if np.isnan(x_one).any() or np.isnan(y_one).any():
                continue
            
            ### 在给定交易日和给定股票的位置 n_sample处，填入小矩阵的值
            x_in_sample[n_sample, :, :] = x_one
            y_in_sample[n_sample, :] = y_one
            n_sample += 1
        
        ### y_in_sample[s_index:e_index, 0]
        ### s_index = n_sample对应的是当前一个位置，例如股票i的t期，
        ### e_index 对应的是下一个位置，例如股票i的t+1期，
        e_index = n_sample
        # standardize_and_weight:计算输入变量的标准化值，N(0,1), 二分法对中位数上下的数值分别取2和1
        # y_in_sample[s_index:e_index, 0], w_in_sample[s_index:e_index,0] 的维度是1维的 (497,) (497,)
        y_in_sample[s_index:e_index, 0], w_in_sample[s_index:e_index,0] = standardize_and_weight(y_in_sample[s_index:e_index, 0])
        # standardize_and_weight(y_in_sample[s_index:e_index, 0])

    ### 计算结束后， n_sample = 
    ### 对全部个股和交易日计算后，对x,y,z_in_sample重新赋值：
    # x_in_sample：(725892, 30, 15)；y_in_sample (725892, 1)；w_in_sample (725892, 1)
    x_in_sample = x_in_sample[:n_sample, :]
    y_in_sample = y_in_sample[:n_sample, :]
    w_in_sample = w_in_sample[:n_sample, ]
    ### y_in_sample 最大值 20.678607251529517 ；最小值 -15.9812
    
    ### 数据分割
    ### train_pct 是训练集的数据占原始数据百分比
    train_pct = 0.8 
    ### split 训练集长度
    split = int(y_in_sample.shape[0] * train_pct )
    ### 训练数据：x_train,y_train
    # 变量大小：x_train：(580713, 30, 15)；y_train (580713, 1)
    x_train = x_in_sample[:split, :, :]    
    y_train = y_in_sample[:split, :]
    w_train = w_in_sample[:split, 0]

    ### 测试数据：x_train,y_train
    x_val = x_in_sample[split:, :, :]
    y_val = y_in_sample[split:, :]
    w_val = w_in_sample[split:, 0]
    return x_train, x_val, y_train, y_val, w_train, w_val

#####################################################################
### end_list 的值似乎都是对应年末的日期？？？
# end_list = [5342, 5220, 5099, 4977, 4857, 4735, 4613, 4491, 4370, 4248, 4126, 4004, 3883, 3761, 3644, 3522, 3403, 3281, 3160]
# end_list = [5342]
end_list = [ 3160]

df_val_loss = pd.DataFrame()
df_val_ic = pd.DataFrame()
df_weight = pd.DataFrame()

### 对选定的日期对应数值进行计算：
for end in end_list:
    print("end date :" ,end )
    
    #####################################################################
    ### 导入训练和测试数据 
    # 变量大小：x_train：(580713, 30, 15)；y_train (580713, 1)
    x_train, x_val, y_train, y_val, w_train, w_val = load_data(end)
    print('data finish  '  )

    '''规范化BatchNormalization:BN层的作用
    （1）加速收敛 （2）控制过拟合，可以少用或不用Dropout和正则 
    （3）降低网络对初始化权重不敏感 （4）允许使用较大的学习率
    参数
    axis: 整数，指定要规范化的轴，通常为特征轴。例如在进行data_format="channels_first的2D卷积后，一般会设axis=1。
    momentum: 动态均值的动量
    epsilon：大于0的小浮点数，用于防止除0错误
    center: 若设为True，将会将beta作为偏置加上去，否则忽略参数beta
    scale: 若设为True，则会乘以gamma，否则不使用gamma。当下一层是线性的时，可以设False，因为scaling的操作将被下一层执行。
    beta_initializer：beta权重的初始方法
    gamma_initializer: gamma的初始化方法
    moving_mean_initializer: 动态均值的初始化方法
    moving_variance_initializer: 动态方差的初始化方法 
    '''
    ### input1是训练集x_train 的输入形状
    ### 什么是 BatchNormalization ？让每一层输入值转化为N(0,1)的正太分布， url=https://www.cnblogs.com/guoyaohua/p/8724433.html
    ### input1 =  Tensor("input_1:0", shape=(None, 30, 15), dtype=float32)
    input_1 = Input(shape=(len_date,x_train.shape[2]))
    
    #####################################################################
    ### hidden1 特征提取层；类比CNN中卷积层
    # 构建标准化layer：BatchNormalization()：输出shape，与输入shape相同
    ### 这个环节其实没有进行计算，只是相当于生成了要计算的函数。
    corr1 = BatchNormalization()(ts_corr10()(input_1))
    cov1 = BatchNormalization()(ts_cov10()(input_1))
    std1 = BatchNormalization()(ts_std10()(input_1))
    zscore1 = BatchNormalization()(ts_zscore10()(input_1))
    # 线性衰减
    decay_linear1 = BatchNormalization()(ts_decay_linear10()(input_1))
    return1 = BatchNormalization()(ts_return10()(input_1))
    
    #####################################################################
    ### 简单地拼接多个layer：它接受一个张量的列表， 除了连接轴之外，其他的尺寸都必须相同， 然后返回一个由所有输入张量连接起来的输出张量。
    ### 默认 axis=-1 
    add1 = concatenate([corr1, cov1,std1,zscore1,decay_linear1,return1]) 

    ### hidden1 是 特征提取层1
    # TODO: 关于GRU和LTSM模型介绍：GRU可以保存时间序列信息
    ### 要比较 LTSM和GRU两种模型的区别：
    # https://blog.csdn.net/violethan7/article/details/79675307
    ### GRU替代LTSM模型，优点是减少模型参数；
    hidden1 = BatchNormalization()(GRU(30, input_shape=(int(add1.shape[1]), int(add1.shape[2])))(add1))
    ''' GRU是干嘛的：门限循环单元网络；url=https://keras.io/zh/layers/recurrent/#gru
    class GRU(units, activation='tanh', recurrent_activation='sigmoid', use_bias=True, 
    kernel_initializer='glorot_uniform', recurrent_initializer='orthogonal', bias_initializer='zeros', 
    kernel_regularizer=None, recurrent_regularizer=None, bias_regularizer=None, activity_regularizer=None,
    kernel_constraint=None, recurrent_constraint=None, bias_constraint=None, dropout=0., recurrent_dropout=0., 
    implementation=2, return_sequences=False, return_state=False, go_backwards=False, stateful=False, unroll=False, 
    time_major=False, reset_after=True, **kwargs)
    '''
    '''Check... corr1 Tensor("batch_normalization/batchnorm/add_1:0", shape=(None, 3, 105), dtype=float32)
    Check... cov1 Tensor("batch_normalization_1/batchnorm/add_1:0", shape=(None, 3, 105), dtype=float32)
    Check... std1 Tensor("batch_normalization_2/batchnorm/add_1:0", shape=(None, 3, 15), dtype=float32)
    Check... zscore1 Tensor("batch_normalization_3/batchnorm/add_1:0", shape=(None, 3, 15), dtype=float32) 
    '''
    #####################################################################
    ### hidden2 特征提取层2
    corr2 = BatchNormalization()(ts_corr5()(input_1))
    cov2 = BatchNormalization()(ts_cov5()(input_1))
    std2 = BatchNormalization()(ts_std5()(input_1))
    zscore2 = BatchNormalization()(ts_zscore5()(input_1))
    decay_linear2 = BatchNormalization()(ts_decay_linear5()(input_1))
    return2 = BatchNormalization()(ts_return5()(input_1))
    
    ### 拼接多个layer：
    add2 = concatenate([corr2, cov2,std2,zscore2,decay_linear2,return2]) 

    hidden2 = BatchNormalization()(GRU(30, input_shape=(int(add2.shape[1]), int(add2.shape[2])))(add2))

    #####################################################################
    ### 
    ### hidden3 是 hidden1 和hidden2两个层
    hidden3 = concatenate([hidden1, hidden2])    

    #####################################################################
    ### 池化层，一般存在两种池化方式，分别是均值池化和最大池化。
    # 报告里：ts_mean(ts_corr(X, Y, 3),3) ？？
    # 李子钰反馈说也可以嵌套 ts_mean(  BatchNormalization()(ts_corr10()(input_1)) )，但报告里没用的原因是没有显著的结果 
    ### TODO，尝试嵌套, ts_corr10( ts_return15()(input_1)) 会报错，维度不对了。
    
    ### 全连接层=核心网络层=Dense；
    hidden = Dense(1, kernel_initializer='truncated_normal', activation='linear')(hidden3)
    
    '''units: 正整数，输出空间维度。
    activation: 激活函数 (详见 activations)。 若不指定，则不使用激活函数 (即，「线性」激活: a(x) = x)。
    use_bias: 布尔值，该层是否使用偏置向量。
    kernel_initializer: kernel 权值矩阵的初始化器 (详见 initializers)。
    bias_initializer: 偏置向量的初始化器 (see initializers).
    kernel_regularizer: 运用到 kernel 权值矩阵的正则化函数 (详见 regularizer)。
    bias_regularizer: 运用到偏置向的的正则化函数 (详见 regularizer)。
    activity_regularizer: 运用到层的输出的正则化函数 (它的 "activation")。 (详见 regularizer)。
    kernel_constraint: 运用到 kernel 权值矩阵的约束函数 (详见 constraints)。
    bias_constraint: 运用到偏置向量的约束函数 (详见 constraints)。
    '''
    #####################################################################
    ### 定义模型的input和output
    model = Model(inputs=input_1, outputs=hidden)

    ### model.summary()输出模型各层的参数状况，
    model.summary()
    
    ### 最优化模型
    # Adam()是解决这个问题的一个方案。其大概的思想是开始的学习率设置为一个较大的值，
    # 然后根据次数的增多，动态的减小学习率，以实现效率和效果的兼得。
    #opt = optimizers.RMSprop(lr=0.0001)
    ### lr：float> = 0.学习率;decay：float> = 0,每次更新时学习率下降
    # 刚开始训练时：学习率以 0.01 ~ 0.001 为宜;一定轮数过后：逐渐减缓;结束衰减应该在100倍以上
    ### 优化器和学习速率：RMSProp，0.0001
    # opt = optimizers.RMSprop(lr=0.0001)
    opt = optimizers.Adam(lr=0.00005)
    
    ### 设置损失函数：均方误差MSE，
    model.compile(loss="mse", optimizer=opt, metrics=[pearson_r])
    ### 设置提前停止参数
    early_stopping = EarlyStopping(monitor='val_loss', patience=8, restore_best_weights=True)  
    
    ### hist 是连续 epoch 训练损失和评估值，以及验证集损失和评估值的记录
    ### batch_size=500~1000
    hist = model.fit(x_train, y_train, batch_size=500, epochs=100, validation_data=(x_val, y_val), callbacks=[early_stopping])
    # type(hist)=<class 'tensorflow.python.keras.callbacks.History'>
    print("history: ",hist)

    df_val_loss[end] = hist.history['val_loss'][-8:]
    df_val_ic[end] = hist.history['val_pearson_r'][-8:]
    # 获得输入张量、输出张量、输入数据的形状和输出数据的形状
    df_weight[end] = model.layers[-1].get_weights()[0][:,0]
    
    #####################################################################
    ### save to output 
    print("path_model_output_ID  :",path_model_output_ID  )
    postfix =  input_ID +"_"
    print(   postfix  )
    df_val_loss.to_csv( path_model_output_ID + postfix + 'df_val_loss'+'.csv')
    df_val_ic.to_csv( path_model_output_ID  + postfix  + 'df_val_ic' +'.csv')
    df_weight.to_csv( path_model_output_ID  + postfix  + 'df_weight' +'.csv')

    ### save given end day to model path
    model.save( path_model_output_ID  + postfix  + str(end) +'.h5')
    
    # TODO,会报错暂时不用了。
    ### keras使用plot_model绘制网络模型图
    # from keras.utils import plot_model
    # plot_model(model, path0 + 'model.png', show_shapes=True)

    ### delete variables
    del x_train, x_val, y_train, y_val

    
#####################################################################
### Qs:全连接隐藏层为什么说有30个神经元？？
'''1.激活函数：RELU。
2.DropOut 比率：0.5。
3.权重初始化方式：truncated_normal


'''
#####################################################################3
### output
# 20201129_161059
# postfix = "_" + temp_date 
# df_val_loss.to_csv(path0 + 'df_val_loss'+ postfix +'.csv')
# df_val_ic.to_csv(path0 + 'df_val_ic'+ postfix +'.csv')
# df_weight.to_csv(path0 + 'df_weight'+ postfix +'.csv')

