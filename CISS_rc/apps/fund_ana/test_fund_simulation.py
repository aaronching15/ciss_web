# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

''' 
################################################
todo： 
last 211111 | since 211111 
derived from file=test_fund_ana_stock.py；keti21_基金逆向研究.docx,path=C:\rc_HUARONG\rc_HUARONG\2020IAMAC课题 ; 
file=0基金持仓仿真.xlsx，sheet=代码管理
notes:对 test_fund_ana_stock.py 中的代码进行简化
################################################
功能：基金的逆向仿真
1，数据准备、分析和处理：test_fund_ana.py

2，股票投资组合的量化分析,test_fund_ana_stock.py,test_fund_ana2.py
    进度：在这里
3,基金股票组合的调仓行为模拟

4,投资策略特征和预测能力

5,组合迭代和绩效优化

'''
#################################################################################
### Part 0 数据准备 |  Initialization，load configuration 
# Notes: 除了基本的路径和时间，其他脚本导入尽量都放在 config_XXX 里，保存再 obj 对象 
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db；# C:\ciss_web\CISS_rc\config
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )

import pandas as pd 
import numpy as np 
import math
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

################################################################################
### Step 0, 导入基金配置文件，初始化基金对象
### 1，时间：对于更新时间t，确定对应季末时间T和上一季度末时间T-1
from config_fund import config_fund
config_fund_1 = config_fund()
from config_data import config_data
config_data_1 = config_data()

#################################################################################
### 1 数据准备、分析和处理：test_fund_ana.py



#################################################################################
### 2 股票投资组合的量化分析,test_fund_ana_stock.py,test_fund_ana2.py

df1= pd.read_excel("D:\\debug-df_all_nav.xlsx")
df_all_nav = df1.fillna(method="ffill",axis=1)     
obj_fund={}
series_nav = df_all_nav[20190801]
print("series_nav",series_nav ) 
for temp_i in df_all_nav.index  :   
    ### notes:部分组合可能由于各种各样的原因存在净值缺失
    
    # try :
    
    df_all_nav.loc[temp_i, [20190801,20190802]] = df_all_nav.loc[temp_i, [20190801,20190802]]/df_all_nav.loc[temp_i, 20190801]
    # print(temp_date ,df_all_nav[ temp_date ]  ) 
    # print("series_nav",series_nav )
    # asd
    # except :
    #     obj_fund["fund_list_error"] = obj_fund["fund_list_error"] + [temp_date ]

df_all_nav.to_excel("D:\\debug-df_all_nav2.xlsx")
#################################################################################
### 3 基金股票组合的调仓行为模拟




#################################################################################
### 4,投资策略特征和预测能力





#################################################################################
### 5,组合迭代和绩效优化




























