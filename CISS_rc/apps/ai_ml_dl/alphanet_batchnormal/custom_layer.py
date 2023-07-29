# -*- coding: utf-8 -*-
__author__ = " ruoyu.Cheng"
'''
1，ts_corr这些function既可以在特征提取层用，也可以在池化层用
2, add_weight()是keras里用来增加可训练权重。
3，super 用来调用父类的方法,例如uper().add(m) 调用父类方法 def add(self, m)

关于下划线变量：
_xxx，单下划线开头的变量，标明是一个受保护(protected)的变量，原则上不允许直接访问，但外部类还是可以访问到这个变量。这只是程序员之间的一个约定，用于警告说明这是一个私有变量，外部类不要去访问它。
__xxx，双下划线开头的，表示的是私有类型(private)的变量。只能是允许这个类本身进行访问了, 连子类也不可以,用于命名一个类属性（类变量）
例如 类Student内部，__name变成_Student__name,如 self._Student__name)

'''


from keras.engine import Layer
from keras import backend as K
import itertools
from keras.initializers import Ones, Zeros
import tensorflow as tf

def calc_std(tensor):
    ### 内部工具型函数。计算输入tensor的标准差
    ### 使用 keras.backend 后端,功能包括 浮点数数据类型
    ### 计算标准差
    x_std = K.std(tensor,axis=1)
    ### keras.backend.switch(condition, then_expression, else_expression)
    ### 参数：condition: 张量 (int 或 bool)。then_expression: 张量或返回张量的可调用函数。else_expression: 张量或返回张量的可调用函数。
    ### switch接口，顾名思义，就是一个if/else条件判断语句。不过要求输入和输出都必须是张量
    ### 根据一个标量值在两个操作之间切换。
    x_std = K.switch(tf.math.is_nan(x_std), K.mean(tensor, axis=1) - K.mean(tensor, axis=1), x_std)
    return x_std

########################################################################
### 使用keras自定义神经网络层
'''
url= https://blog.csdn.net/kongfangyi/article/details/108102493
keras提供了自定义层的编程范式，但是很多书都没有介绍，可能是一般的应用用不到。另一方面效果不一定好，需要有一定的理论功底才能设计新的模型。
# 自定义层
#自定义层继承自keras.engine.topology的Layer类
#自定义Layer中需要定义至少三个函数__init__、build、call、如果返回形状发生了改变需要定义compute_output_shape层
class MyLayer(Layer):
	#__init__定义了需要初始化的参数
    def __init__(self, activation = None, **kwargs):
        self.activation = activation
        super(interActivate, self).__init__(**kwargs)
    #build函数定义了权重等值
    def build(self, input_shape):
        self.shape = input_shape
        self.w = self.add_weight(name = "w",shape = (input_shape[0], input_shape[1]), initializer="normal",trainable=True)
        super(interActivate, self).build(input_shape)
	
	#call函数定义了具体的计算过程,x为输入值（一般为上一层计算结果）
    def call(self, x):
        front_tanh = K.tanh(x)   
	    return front_tanh

    #注意：如果输出形状不变，则不需要；如果输出的形状发生改变，此处一定要标明
    def compute_output_shape(self, input_shape):
        return (input_shape[0],165,165)

'''
class ts_corr(Layer):
    '''过去 d 天 X 值构成的时序数列和 Y 值构成的时序数列的相关系数。
    研究报告：ts_corr(X, Y, 3)网络层的工作机制。ts_corr(X, Y, 3)会在时间维度
    上和特征维度上对二维的数据进行遍历运算，与 CNN 类似，步进大小 stride 是可调参数，
    例如 stride=1 时，下一次计算在时间维度上往右步进一步。在特征维度上的计算则体现出
    了与 CNN 卷积的不同之处，CNN 卷积运算只能进行局部感知，但是 ts_corr(X, Y, 3)会对
    所有类型的数据进行遍历，其计算区域不一定要相邻
    '''
    ### strides是步长，window相当于回望窗口，比如取过去15天均价。
    ### 参数**kwargs代表按字典方式继承父类
    def __init__(self, window=5, strides=1, **kwargs):
        ### 必备组件：__init__
        ### 这个方法是用来初始化并自定义自定义层所需的属性，比如output_dim，以及一个必需要执行的super().__init __(**kwargs)，
        # 这行代码是去执行Layer类中的初始化函数，当它执行了你就没有必要去管input_shape,weights,trainable等关键字参数了因为父类(Layer)的初始化函数实现了它们与layer实例的绑定。

        self.strides = strides
        self.window = window
        ### super... 是必备项
        ### 首先找到test的父类（比如是类A），然后把类test的对象self转换为类A的对象，然后“被转换”的类A对象调用自己的__init__函数
        super(ts_corr, self).__init__(**kwargs)

    def build(self, input_shape):
        ### 必备组件：build 添加可训练参数 Create a trainable weight variable for this layer.
        ### 创建层权重的地方。你可以通过Layer类的add_weight方法来自定义并添加一个权重矩阵。这个方法一定有input_shape参数。
        # 必须设self.built = True，目的是为了保证这个层的权重定义函数build被执行过了，这个self.built其实是个标记而已，当然
        # 也可以通过调用super([MyLayer], self).build(input_shape)来完成。build这个方法是用来创建权重的，在这个函数中我们需要
        # 说明这个权重各方面的属性比如shape,初始化方式以及可训练性等信息，这也是为什么keras设计单独的一个方法来定义权重。
        ''' 添加可训练参数，例如 self.kernel = self.add_weight(name='kernel', 
            shape=(input_shape[1], self.strides),initializer='uniform',trainable=True)
            可以用父类的self.add_weight() 函数来初始化数据, 该函数必须将 self.built 设置为True, 以保证该 Layer 已经成功 build ,
            通常如上所示, 使用 super(MyLayer, self).build(input_shape) 来完成。
        '''

        ### super... 是必备项
        super(ts_corr, self).build(input_shape)

    def compute_corr(self, x, y):
        ### 计算2个因子序列数据的相关系数
        ### 内部工具型函数。输入变量x,y都是tensor，out是2个tensor的相关系数
        ### 计算标准差，用keras后端的计算方式
        std_x = calc_std(x) + 0.00001
        std_y = calc_std(y)  + 0.00001

        x_mul_y = x * y
        E_x_mul_y = K.mean(x_mul_y, axis=1)
        mean_x = K.mean(x, axis=1)
        mean_y = K.mean(y, axis=1)
        cov = E_x_mul_y - mean_x * mean_y

        out = cov / (std_x * std_y)
        return out

    def call(self, tensors):
        ### 必备组件：层的功能逻辑，定义功能，相当于Lambda层的功能函数；只需要关注传入call的第一个参数：输入张量tensors或者叫x
        ### call() 用来执行 Layer 的职能, x就是该层的输入，x与权重kernel做点积，生成新的节点层，即当前 Layer 所有的计算过程均在该函数中完成。
        ### tensors = x_train：变量大小：x_train：(580713, 30, 15)；y_train (580713, 1)
        ''' 例如输入input=【5,128】，5是batch_size,128是embedding向量的维度，input_shape[0]=5，input_shape[1]=128，假如output_dim=256，
        所以self.kernel的维度就是【128,256】，最后compute_output_shape的输出维度就是【5,256】。调用方式：Mylayer(256)(input)
        tensors只能是一种形式不能是具体的变量也就是它说它不能被定义。如果你希望你的层能支持masking，我们建议直接使用官方给的Masking层即可。
        这个call函数就是该层的计算逻辑，或计算图了，当创建好了这个层实例后，这个实例可以使用像函数调用那样的语法来执行call函数
        (不懂的可以了解一下python中的__call__魔法方法)。显然，这个层的核心应该是一段符号式的输入张量到输出张量的计算过程。
        再次强调因为输入只是个形式，所以输入变量不能被事先定义。这个跟python中的匿名函数类似，在python中没有被赋过值的变量就是未定义的。
        '''
        ### 
        ### 下划线变量啥意思：_用作被丢弃的名称。按照惯例，这样做可以让阅读你代码的人知道，这是个不会被使用的特定名称。举个例子，你可能无所谓一个循环计数的值：
        # K.int_shape() :例如：inputs = K.placeholder(shape=(2, 4, 5))
        #  K.int_shape(inputs)= (2, 4, 5) 
        # 例如：for _ in range(40)：do_something()
        # tensors= x_train= (580713, 30, 15)；t_num=30, self.f_num=15
        _, self.t_num, self.f_num,  = K.int_shape(tensors)
        ### s_num对应的是 iter_list的长度，这里是30-5+1
        self.s_num = 0
        ### c_num是因子数量f_num里选取所有2个因子的组合数量 C_n_2 ;
        # C_n_2 = (n)(n-1)/(2*1)
        self.c_num = int(self.f_num*(self.f_num-1)/2)

        #### xs是最终要输出的变量
        xs=[]

        ### 按时间窗口window迭代数值地list，### tensors 对应 x_train
        ### 例：t_num=30, self.f_num=15；range(0, 30-5+1, 1)
        iter_list = list(range(0, self.t_num - self.window + 1, self.strides))
        if self.t_num - self.window not in iter_list:
            iter_list.append(self.t_num - self.window)
        
        ### itertools是python3-cookbook里的模块：url=https://www.w3cschool.cn/youshq/hcs4mozt.html
        '''itertools模块提供了三个函数来解决这类问题：
        1，考虑顺序：itertools.permutations() ，
            它接受一个集合并产生一个元组序列，每个元组由集合中所有元素的一个可能排列组成。也就是说通过打乱集合中元素排列顺序生成一个元组，比如：
            items = ['a', 'b', 'c'] ; from itertools import permutations
            for p in permutations(items，3): # 3是生成3个元素的所有组合，2是2个元素的所有组合
                print(p) -->...
            ('a', 'b', 'c')，('a', 'c', 'b') ....会生成所有a,b,c的排列组合
        2，不分顺序：itertools.combinations() 可得到输入集合中元素的所有的组合。比如：
            for c in combinations(items, 3): --> ('a', 'b', 'c')
        '''
        ### iter_list=[0,1,2,...,26 ]
        for i_stride in iter_list:
            ### itertools:对于f_num=15个因子里，对所有2个因子组合计算相关性：self.compute_corr
            for subset in itertools.combinations(list(range(self.f_num)), 2):
                tensor1 = tensors[:,i_stride:self.window+i_stride,subset[0]]
                tensor2 = tensors[:,i_stride:self.window+i_stride,subset[1]]

                x_corr = self.compute_corr(tensor1, tensor2)
                #### xs：所有两两因子的相关系数的列表
                xs.append(x_corr)
            self.s_num += 1
        
        ### 把xs从list类变成tensor：type(output) ='tensorflow.python.framework.ops.EagerTensor'
        output = K.stack(xs,axis=1)
        ### 就是因为xs在赋值时是1维的，其实也可以赋值成二维的矩阵，那就不用reshape了。
        ### K.reshape类似于np.reshape:-1好像是标配，后边的(s_num,c_num)是输出的维度
        # 猜测-1是倒数第一个维度？
        output = K.reshape(output, [-1, self.s_num, self.c_num])
        return output

    def compute_output_shape(self, input_shape):
        ### 必备组件：计算输出形状，如果输入和输出形状一致，那么可以省略，否则最好加上。
        # 为了能让Keras内部shape的匹配检查通过，这里需要重写compute_output_shape方法去覆盖父类中的同名方法，
        # 来保证输出shape是正确的。父类Layer中的compute_output_shape方法直接返回的是input_shape这明显是不对的，
        # 所以需要我们重写这个方法。所以这个方法也是4个要实现的基本方法之一。
        return (input_shape[0], self.s_num, self.c_num)


class ts_cov(Layer):
    ### 过去 d 天 X 值构成的时序数列和 Y 值构成的时序数列的协方差。
    def __init__(self, window=5, strides=1, **kwargs):
        self.strides = strides
        self.window = window
        super(ts_cov, self).__init__(**kwargs)

    def build(self, input_shape):
        super(ts_cov, self).build(input_shape)

    def compute_cov(self, x, y):
        x_mul_y = x * y
        E_x_mul_y = K.mean(x_mul_y, axis=1)
        mean_x = K.mean(x, axis=1)
        mean_y = K.mean(y, axis=1)
        cov = E_x_mul_y - mean_x * mean_y
        return cov

    def call(self, tensors):
        ###
        # tensors= x_train= (580713, 30, 15)；t_num=30, self.f_num=15
        _, self.t_num, self.f_num,  = K.int_shape(tensors)
        self.s_num = 0
        self.c_num = int(self.f_num*(self.f_num-1)/2)
        xs=[]

        iter_list = list(range(0, self.t_num - self.window + 1, self.strides))
        if self.t_num - self.window not in iter_list:
            iter_list.append(self.t_num - self.window)
        
        ### 这个循环还是不知道啥意思
        for i_stride in iter_list:
            for subset in itertools.combinations(list(range(self.f_num)), 2):
                tensor1 = tensors[:,i_stride:self.window+i_stride,subset[0]]
                tensor2 = tensors[:,i_stride:self.window+i_stride,subset[1]]
                x_corr = self.compute_cov(tensor1, tensor2)
                xs.append(x_corr)
            self.s_num += 1
        
        output = K.stack(xs,axis=1)
        output = K.reshape(output, [-1, self.s_num, self.c_num])
        return output

    def compute_output_shape(self, input_shape):
        return (input_shape[0], self.s_num, self.c_num)
  
  

class ts_std(Layer):
    ### 过去 d 天 X 值构成的时序数列的标准差。
    def __init__(self, window=5,strides=1, **kwargs):
        self.window = window
        self.strides = strides
        super(ts_std, self).__init__(**kwargs)

    def build(self, input_shape):
        super(ts_std, self).build(input_shape)

    def call(self, tensors):
        # tensors= x_train= (580713, 30, 15)；t_num=30, self.f_num=15
        _, self.t_num, self.f_num,  = K.int_shape(tensors)  
        self.s_num=0
        xs=[]
        iter_list = list(range(0, self.t_num - self.window + 1, self.strides))
        if self.t_num - self.window not in iter_list:
            iter_list.append(self.t_num - self.window)
        for i_stride in iter_list:
            for j in range(0,self.f_num):
                x_std = calc_std(tensors[:,i_stride:self.window+i_stride,j])
                xs.append(x_std)
            self.s_num += 1
        output = K.stack(xs,axis=1)
        output = K.reshape(output, [-1, self.s_num, self.f_num])
        return output

    def compute_output_shape(self, input_shape):
        return (input_shape[0], self.s_num , self.f_num)

#############################################################
### BN(Batch Normalization)，中文为批标准化层，该层已经是目前神经网络中最常用的组件
# 之一，在 AlphaNet 的特征标准化中起着重要作用。下面我们简要介绍 BN 的原理。
# 设𝑍𝑙为神经网络第𝑙层的计算结果， 𝑚为每个 batch 中样本的数量，有：
class LayerNormalization(Layer):
    ### 对layer的值进行正态化
    ### 在 BN 层标准化前，ts_corr 层提取特征的取值范围为(-0.7, 0.4)，ts_std 层提取特征的取值范围为(0, 23000000)，差距
    ### 很大。而在 BN 层标准化后，特征的取值范围非常接近，都在区间(-1.5, 2)中
    def __init__(self, eps=1e-6, **kwargs):
        self.eps = eps
        super(LayerNormalization, self).__init__(**kwargs)

    def build(self, input_shape):
        # 为gamma层创建一个可训练的权重add_weight
        self.gamma = self.add_weight(name='gamma', shape=input_shape[-1:],
                                     initializer=Ones(), trainable=True)
        self.beta = self.add_weight(name='beta', shape=input_shape[-1:],
                                    initializer=Zeros(), trainable=True)
        super(LayerNormalization, self).build(input_shape)

    def call(self, x):
        # -1 可能是x的倒数第一个维度？
        mean = K.mean(x, axis=-1, keepdims=True)
        std = K.std(x, axis=-1, keepdims=True)
        std = K.switch(tf.math.is_nan(std), mean - mean, std)
        return self.gamma * (x - mean) / (std + self.eps) + self.beta

    def compute_output_shape(self, input_shape):
        return input_shape

    def compute_mask(self, inputs, input_mask=None):
        return input_mask
    

class ts_norm(Layer):
    def __init__(self,  **kwargs):
        super(ts_norm, self).__init__(**kwargs)
        
    def build(self, input_shape):
        super(ts_norm, self).build(input_shape)
        
    def call(self, tensors):
        ### 
        ### tf.norm(tensors, axis=1) 计算向量范数
        # 例如 a=[[1,1],[1,1]],tf.norm(a,axis=1)=tf.Tensor([1.4142135 1.4142135], shape=(2,), dtype=float32)
        # a/tf.norm(a,axis=1)= array([[0.70710677, 0.70710677],[0.70710677, 0.70710677]], dtype=float32)>
        output = tensors / tf.norm(tensors, axis=1)
        return output
    

    def compute_output_shape(self, input_shape):
        return input_shape
    
    
class ts_zscore(Layer):
    ### 计算标准分值
    def __init__(self, window=5, strides=1, **kwargs):
        self.window = window
        self.strides = strides
        super(ts_zscore, self).__init__(**kwargs)

    def build(self, input_shape):
        super(ts_zscore, self).build(input_shape)

    def call(self, tensors):
        ### 
        # tensors= x_train= (580713, 30, 15)；t_num=30, self.f_num=15
        _, self.t_num, self.f_num,  = K.int_shape(tensors)  
        self.s = 0
        tmparray=[]
        ###  
        def _df_ts_zscore(k):
            return ((tensors[:, self.window + k - 1,:]) - K.mean(tensors[:, k:self.window + k,:], axis=1)) / (K.std(tensors[:, k:self.window + k,:],axis=1)+1e-4)
        
        iter_list = list(range(0, self.t_num - self.window + 1, self.strides))

        if self.t_num - self.window not in iter_list:
            iter_list.append(self.t_num - self.window)

        for i_stride in iter_list:
            ### 引用张量tensor的标准分计算：
            tmparray.append(_df_ts_zscore(i_stride))

        self.s=len(iter_list)
        output = K.stack(tmparray,axis=1)
        output = K.reshape(output, [-1, self.s, self.f_num])
        return output

    def compute_output_shape(self, input_shape):
        return (input_shape[0], self.s, self.f_num)


class ts_prod(Layer):
    def __init__(self, window=5,strides=1, **kwargs):
        self.window = window
        self.strides = strides
        super(ts_prod, self).__init__(**kwargs)

    def build(self, input_shape):
        super(ts_prod, self).build(input_shape)

    def call(self, tensors):
        ###
        # tensors= x_train= (580713, 30, 15)；t_num=30, self.f_num=15
        _, self.t_num, self.f_num,  = K.int_shape(tensors)  
        tmparray=[]
        iter_list = list(range(0, self.t_num - self.window + 1, self.strides))
        if self.t_num - self.window not in iter_list:
            iter_list.append(self.t_num - self.window)
        ### keras.backend.prod(x, axis=None, keepdims=False) ;在某一指定轴，计算张量中的值的乘积。
        # x: 张量或变量。axis: 一个整数需要计算乘积的轴。
        # keepdims: 布尔值，是否保留原尺寸。 如果 keepdims 为 False，则张量的秩减 1。 如果 keepdims 为 True，缩小的维度保留为长度 1。
        for i_stride in iter_list:
            tmparray.append(K.prod(tensors[:, i_stride:self.window + i_stride], axis=1))
        output = K.stack(tmparray,axis=1)
        output = K.reshape(output, [-1, i_stride+1, self.f_num])
        return output

    def compute_output_shape(self, input_shape):
        return (input_shape[0], self.t_num - self.window+1 , self.f_num)

class ts_decay_linear(Layer):
    ### 线性衰减？
    def __init__(self, window=5,strides=1, **kwargs):
        self.window = window
        self.strides = strides
        super(ts_decay_linear, self).__init__(**kwargs)

    def build(self, input_shape):
        super(ts_decay_linear, self).build(input_shape)

    def call(self, tensors):
        ### 
        # <tf.Tensor: shape=(5,), dtype=float32, numpy=array([1., 2., 3., 4., 5.], dtype=float32)>
        # 相当于矩阵转置了，从1*5 变成 5*1 ：array([[1.],[2.],[3.],[4.],[5.]], dtype=float32)>
        num = K.reshape(K.constant(list(range(self.window))) + 1.0,(-1,1))

        ### keras.backend.tile(x, n)创建一个用 n 平铺 的 x 张量。
        # 参数 x : 张量或变量。 n : 整数列表。长度必须与 x 中的维数相同。返回  一个平铺的张量。
        #例子： rr=[[1,3,],[5,7]]；K.tile(rr,[1,2]) 在第二维度=列复制成2倍
        # <tf.Tensor: shape=(2, 4), dtype=int32, numpy=array([[1, 3, 1, 3],[5, 7, 5, 7]])>
        # K.tile(rr,[2,1])在第一维度=行复制成2倍
        # <tf.Tensor: shape=(4, 2), numpy=array([[1, 3],[5, 7], [1, 3],[5, 7]])>
        # tensors.shape[2] 对应的是15哥因子
        coe = K.tile(num, (1,tensors.shape[2]))
        self.s=0
        ### 
        def _sub_decay_linear(k, coe):
            data = tensors[:, k:self.window + k, :]
            sum_days = K.reshape(K.sum(coe,axis = 0),(-1,tensors.shape[2]))
            sum_days = K.tile(sum_days,(self.window,1))
            coe = coe/sum_days
            decay = K.sum(coe*data,axis = 1)
            return decay
        
        ### 
        # tensors= x_train= (580713, 30, 15)；t_num=30, self.f_num=15
        _, self.t_num, self.f_num,  = K.int_shape(tensors)  
        tmparray=[]
        iter_list = list(range(0, self.t_num - self.window + 1, self.strides))

        if self.t_num - self.window not in iter_list:
            iter_list.append(self.t_num - self.window)

        for i_stride in iter_list:
            tmparray.append(_sub_decay_linear(i_stride, coe))
        self.s=len(iter_list)
        output = K.stack(tmparray,axis=1)
        output = K.reshape(output, [-1, self.s, self.f_num])
        return output

    def compute_output_shape(self, input_shape):
        return (input_shape[0],self.s, self.f_num)

class ts_return(Layer):
    ### 计算区间收益率或者变化率
    def __init__(self, window=5,strides=1, **kwargs):
        self.window = window
        self.strides = strides
        super(ts_return, self).__init__(**kwargs)

    def build(self, input_shape):
        super(ts_return, self).build(input_shape)

    def call(self, tensors):
        # tensors= x_train= (580713, 30, 15)；t_num=30, self.f_num=15 
        _, self.t_num, self.f_num,  = K.int_shape(tensors)
        self.s = 0
        tmparray=[]
        iter_list = list(range(0, self.t_num - self.window + 1, self.strides))

        if self.t_num - self.window not in iter_list:
            iter_list.append(self.t_num - self.window)

        ### 计算和window(5个交易日) 前相比的百分比变化率。
        for i_stride in iter_list:
            # Qs:万一分母是负值咋办。相当于 [ret(T+5)-ret(T)]/ret(T)
            tmparray.append((tensors[:, self.window + i_stride - 1] - tensors[:, i_stride]) / (tensors[:, i_stride]+1e-4))
        self.s = len(iter_list)
        output = K.stack(tmparray,axis=1)
        output = K.reshape(output, [-1, self.s, self.f_num])
        return output

    def compute_output_shape(self, input_shape):
        return (input_shape[0], self.s, self.f_num)

class ts_mean(Layer):
    ### 计算平均值：例如过去window（5个交易日）个值
    def __init__(self, window=5,strides=1, **kwargs):
        self.window = window
        self.strides = strides
        super(ts_mean, self).__init__(**kwargs)

    def build(self, input_shape):
        super(ts_mean, self).build(input_shape)

    def call(self, tensors):
        # tensors= x_train= (580713, 30, 15)；t_num=30, self.f_num=15
        _, self.t_num, self.f_num,  = K.int_shape(tensors)  
        self.s=0
        tmparray=[]
        iter_list = list(range(0, self.t_num - self.window + 1, self.strides))
        if self.t_num - self.window not in iter_list:
            iter_list.append(self.t_num - self.window)
        ### 计算均值
        for i_stride in iter_list:
            tmparray.append(K.mean(tensors[:, i_stride:self.window + i_stride],axis=1))
        self.s=len(iter_list)
        output = K.stack(tmparray,axis=1)
        output = K.reshape(output, [-1, self.s, self.f_num])
        return output

    def compute_output_shape(self, input_shape):
        return (input_shape[0], self.s, self.f_num)

class ts_sum(Layer):
    ### 求和
	def __init__(self, window=5,strides=1, **kwargs):
	    self.window = window
	    self.strides = strides
	    super(ts_sum, self).__init__(**kwargs)

	def build(self, input_shape):
	    super(ts_sum, self).build(input_shape)

	def call(self, tensors):
        # tensors= x_train= (580713, 30, 15)；t_num=30, self.f_num=15
	    _, self.t_num, self.f_num,  = K.int_shape(tensors)  
	    tmparray=[]
	    iter_list = list(range(0, self.t_num - self.window + 1, self.strides))
	    if self.t_num - self.window not in iter_list:
	        iter_list.append(self.t_num - self.window)
	    for i_stride in iter_list:
	        tmparray.append(K.sum(tensors[:, i_stride:self.window + i_stride - 1],axis=1))
	    output = K.stack(tmparray,axis=1)
	    output = K.reshape(output, [-1, i_stride+1, self.f_num])
	    return output

	def compute_output_shape(self, input_shape):
	    return (input_shape[0], self.t_num - self.window+1 , self.f_num)

class ts_max(Layer):
    ### 求最大值
    def __init__(self, window=5,strides=1, **kwargs):
        self.window = window
        self.strides = strides
        super(ts_max, self).__init__(**kwargs)

    def build(self, input_shape):
        super(ts_max, self).build(input_shape)

    def call(self, tensors):
        # tensors= x_train= (580713, 30, 15)；t_num=30, self.f_num=15
        _, self.t_num, self.f_num,  = K.int_shape(tensors)  
        self.s=0
        tmparray=[]
        iter_list = list(range(0, self.t_num - self.window + 1, self.strides))
        if self.t_num - self.window not in iter_list:
            iter_list.append(self.t_num - self.window)
        for i_stride in iter_list:
            tmparray.append(K.max(tensors[:, i_stride:self.window + i_stride],axis=1))
        self.s=len(iter_list)
        output = K.stack(tmparray,axis=1)
        output = K.reshape(output, [-1, self.s, self.f_num])
        return output

    def compute_output_shape(self, input_shape):
        return (input_shape[0],self.s, self.f_num)

class ts_min(Layer):
    ### 取最小值
    def __init__(self, window=5,strides=1, **kwargs):
        self.window = window
        self.strides = strides
        super(ts_min, self).__init__(**kwargs)

    def build(self, input_shape):
        super(ts_min, self).build(input_shape)

    def call(self, tensors):
        ###
        # tensors= x_train= (580713, 30, 15)；t_num=30, self.f_num=15
        _, self.t_num, self.f_num,  = K.int_shape(tensors)  
        self.s=0
        tmparray=[]
        iter_list = list(range(0, self.t_num - self.window + 1, self.strides))
        if self.t_num - self.window not in iter_list:
            iter_list.append(self.t_num - self.window)
        for i_stride in iter_list:
            tmparray.append(K.min(tensors[:, i_stride:self.window + i_stride],axis=1))
        self.s=len(iter_list)
        output = K.stack(tmparray,axis=1)
        output = K.reshape(output, [-1, self.s, self.f_num])
        return output

    def compute_output_shape(self, input_shape):
        return (input_shape[0], self.s, self.f_num)


class ts_corr3(ts_corr):
    def  __init__(self, **kwargs):
        super(ts_corr3, self).__init__(3,3, **kwargs)
    
class ts_cov3(ts_cov):
    def  __init__(self, **kwargs):
        super(ts_cov3, self).__init__(3,3, **kwargs)
        
class ts_std3(ts_std):
    def  __init__(self, **kwargs):
        super(ts_std3, self).__init__(3,3, **kwargs)

class ts_decay_linear3(ts_decay_linear):
    def  __init__(self, **kwargs):
        super(ts_decay_linear3, self).__init__(3,3, **kwargs)

class ts_zscore3(ts_zscore):
    def  __init__(self, **kwargs):
        super(ts_zscore3, self).__init__(3,3, **kwargs)

class ts_zscore3(ts_zscore):
    def  __init__(self, **kwargs):
        super(ts_zscore3, self).__init__(3,3, **kwargs)

class ts_return3(ts_return):
    def  __init__(self, **kwargs):
        super(ts_return3, self).__init__(3,3, **kwargs)

class ts_mean3(ts_mean):
    def  __init__(self, **kwargs):
        super(ts_mean3, self).__init__(3,3, **kwargs)

class ts_sum3(ts_sum):
    def  __init__(self, **kwargs):
        super(ts_sum3, self).__init__(3,3, **kwargs)

class ts_max3(ts_max):
    def  __init__(self, **kwargs):
        super(ts_max3, self).__init__(3,3, **kwargs)

class ts_min3(ts_min):
    def  __init__(self, **kwargs):
        super(ts_min3, self).__init__(3,3, **kwargs)

class ts_corr5(ts_corr):
    def  __init__(self, **kwargs):
        super(ts_corr5, self).__init__(5,5, **kwargs)

class ts_corr10(ts_corr):
    def  __init__(self, **kwargs):
        super(ts_corr10, self).__init__(10,10, **kwargs)
    
class ts_cov5(ts_cov):
    def  __init__(self, **kwargs):
        super(ts_cov5, self).__init__(5,5, **kwargs)

class ts_cov10(ts_cov):
    def  __init__(self, **kwargs):
        super(ts_cov10, self).__init__(10,10, **kwargs)

class ts_cov15(ts_cov):
    def __init__(self, **kwargs):
        super(ts_cov15, self).__init__(15, 15, **kwargs)


class ts_std5(ts_std):
    def  __init__(self, **kwargs):
        super(ts_std5, self).__init__(5,5, **kwargs)

class ts_std10(ts_std):
    def  __init__(self, **kwargs):
        super(ts_std10, self).__init__(10,10, **kwargs)

class ts_std15(ts_std):
    def __init__(self, **kwargs):
        super(ts_std15, self).__init__(15, 15, **kwargs)

class ts_decay_linear5(ts_decay_linear):
    def  __init__(self, **kwargs):
        super(ts_decay_linear5, self).__init__(5,5, **kwargs)

class ts_decay_linear10(ts_decay_linear):
    def  __init__(self, **kwargs):
        super(ts_decay_linear10, self).__init__(10,10, **kwargs)

class ts_zscore5(ts_zscore):
    def  __init__(self, **kwargs):
        super(ts_zscore5, self).__init__(5,5, **kwargs)

class ts_zscore10(ts_zscore):
    def  __init__(self, **kwargs):
        super(ts_zscore10, self).__init__(10,10, **kwargs)
       
class ts_return5(ts_return):
    def  __init__(self, **kwargs):
        super(ts_return5, self).__init__(5,5, **kwargs)

class ts_return10(ts_return):
    def  __init__(self, **kwargs):
        super(ts_return10, self).__init__(10,10, **kwargs)

class ts_return15(ts_return):
    def  __init__(self, **kwargs):
        super(ts_return15, self).__init__(15,15, **kwargs)

class ts_mean5(ts_mean):
    def  __init__(self, **kwargs):
        super(ts_mean5, self).__init__(5,5, **kwargs)

class ts_mean10(ts_mean):
    def  __init__(self, **kwargs):
        super(ts_mean10, self).__init__(10,10, **kwargs)

class ts_sum5(ts_sum):
    def  __init__(self, **kwargs):
        super(ts_sum5, self).__init__(5,5, **kwargs)

class ts_sum10(ts_sum):
    def  __init__(self, **kwargs):
        super(ts_sum10, self).__init__(10,10, **kwargs)

class ts_max5(ts_max):
    def  __init__(self, **kwargs):
        super(ts_max5, self).__init__(5,5, **kwargs)

class ts_max10(ts_max):
    def  __init__(self, **kwargs):
        super(ts_max10, self).__init__(10,10, **kwargs)

class ts_min5(ts_min):
    def  __init__(self, **kwargs):
        super(ts_min5, self).__init__(5,5, **kwargs)

class ts_min10(ts_min):
    def  __init__(self, **kwargs):
        super(ts_min10, self).__init__(10,10, **kwargs)