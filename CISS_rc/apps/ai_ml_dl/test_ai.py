# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
目标：
测试XGboost模块

todo：
1，
 

notes: 
1，参考资料：
1.1，集成学习之sklearn中的xgboost基本用法，https://www.cnblogs.com/wanglei5205/p/8578486.html
1.2，20170911-华泰证券-华泰证券人工智能系列之六：人工智能选股之Boosting模型.pdf


'''
import pandas as pd
import numpy as np

### Initialization
path_input = "D:\\CISS_db\\factor_model\\000300.SH\\"


### Import modules
from sklearn import datasets  
# 载入数据分割函数train_test_split
from sklearn.model_selection import train_test_split                 
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score   # 准确率
import matplotlib.pyplot as plt
from xgboost import plot_importance

### 载入数据集
file_input = "df_factor_20060125_000300.SH_20060125.csv"
df_temp = pd.read_csv(path_input + file_input )
col_list_input = ["zscore_ep_ttm","zscore_close_pct_52w","zscore_amt_ave_1m_6m","zscore_ret_accumu_20d_120d"]
col_output = "ret_alpha_ind_citic_1_20d_mad"
### 代替空值，fillna
df_temp_stat = df_temp.describe()

for temp_col in col_list_input: 
    #假设所有column的值越大越好，因此选择25%分位数的值代替空值；相当于假设空值对应的是差于均值但好于最小值的情况。
    temp_value = df_temp_stat.loc["25%", temp_col  ]
    df_temp[temp_col] = df_temp[temp_col].fillna(value= temp_value)
# 
temp_value = df_temp_stat.loc["25%", col_output  ]
df_temp[ col_output] = df_temp[ col_output].fillna(value= temp_value)

np_input = df_temp.loc[:,col_list_input].values
np_output = df_temp[col_output].values

### 数据集分割 |
# 特征空间 digits.data,输出空间 digits.target| 
# 测试集占30%
test_size1 = 0.3
# 为了复现实验，设置一个随机数
random_state1 = 7
x_train,x_test,y_train,y_test = train_test_split(np_input, np_output , test_size=test_size1, random_state=random_state1)
# notes: x_train,x_test,y_train,y_test 会有少量nan值
print("x_train,x_test,y_train,y_test")

### 模型相关（载入模型--训练模型--模型预测）
# 载入模型（模型命名为model)
model = XGBClassifier()               
# 训练模型（训练集）
model.fit(x_train,y_train)        

### Plot a Single XGBoost Decision Tree
from xgboost import plot_tree
# Qs:ImportError: You must install graphviz to plot tree
# Ans：需要下载软件 和设置path：在用户变量“path”里添加 C:\Program Files (x86)\graphviz\bin
# CMD里运行 dot -version
# url=https://blog.csdn.net/c_daofeng/article/details/81077594
# 画决策树图
model.fit(x_train,y_train)
plot_tree(model)
plt.show()

asd

# 现实用于训练模型的相关参数
# print("Model \n", model )

# 模型预测（测试集），y_pred为预测结果
y_pred = model.predict(x_test)

### 性能评估
# sklearn.metrics中accuracy_score函数用来判断模型预测的准确度。
print("y_test",y_test[:5])
print("y_pred",y_pred[:5])
print("y_diff",(y_test -y_pred)[:5] )

# 测试变量是否连续,不必须
# from sklearn.utils.multiclass import type_of_target
# print(type_of_target(y_test))

### 性能度量
# accuracy_score,分类准确率分数是指所有分类正确的百分比。分类准确率这一衡量分类器的标准比较容易理解，但是它不能告诉你响应值的潜在分布，并且它也不能告诉你分类器犯错的类型。
# Qs:File "C:\ProgramData\Anaconda3\lib\site-packages\sklearn\metrics\classification.py", line 88, in _check_targets
    # raise ValueError("{0} is not supported".format(y_type))  || ValueError: continuous is not supported
#Ans:在classification.py 文件内把line87-88 注释化
accuracy = accuracy_score(y_test,y_pred)
print("accuarcy: %.2f%%" % (accuracy*100.0))

### 特征重要性
# xgboost分析了特征的重要程度，通过函数plot_importance绘制图片。
# fig,ax = plt.subplots(figsize=(10,15))
# plot_importance(model,height=0.5,max_num_features=64,ax=ax)
# plt.show()

### Evaluate Models With k-Fold Cross-Validation
from sklearn.model_selection import KFold
from sklearn.model_selection import cross_val_score
# 把数据分成k组
kfold = KFold(n_splits=5, random_state=7)
results = cross_val_score(model, x_train,y_train, cv=kfold)
print(results)
print("Accuracy: %.2f%% (%.2f%%)" % (results.mean()*100, results.std()*100))

asd







##############################################################################
### 运行xgboost demo数据；
# https://xgboost.readthedocs.io/en/latest/get_started.html
# https://github.com/dmlc/xgboost/blob/master/demo/guide-python/basic_walkthrough.py
import xgboost as xgb
path="D:\\CISS_db\\py_modules\\xgboost-1.1.1\\demo\\data\\"
file="agaricus.txt.train"
dtrain = xgb.DMatrix(path+file)
# [11:28:07] 6513x127 matrix with 143286 entries loaded from D:\CISS_db\py_modules\xgboost-1.1.1\demo\data\agaricus.txt.train
dtest = xgb.DMatrix(path+"agaricus.txt.test")
# [11:28:32] 1611x127 matrix with 35442 entries loaded from D:\CISS_db\py_modules\xgboost-1.1.1\demo\data\agaricus.txt.test
param = {'max_depth':2, 'eta':1, 'objective':'binary:logistic' }
num_round = 2
bst = xgb.train(param, dtrain, num_round)
preds = bst.predict(dtest) 

preds = bst.predict(dtest)
### output：array([0.28583017, 0.9239239 , 0.28583017, ..., 0.9239239 , 0.05169873, 0.9239239 ], dtype=float32)

asd