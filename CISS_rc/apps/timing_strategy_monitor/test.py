# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''


'''


########################################################################
### Initialization 
import os 
# 获取当前目录 os.getcwd() =: G:\zd_zxjtzq\ciss_web
import sys
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\" )
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )
sys.path.append(os.getcwd()[:2] + "\\ciss_web\\CISS_rc\\db\\" )
sys.path.append(os.getcwd()[:2] + "\\ciss_web\\CISS_rc\\db\\db_assets\\" )

import pandas as pd 
import numpy as np 
import math
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

from data_io import data_io,data_timing_abcd3d,data_factor_model
data_io_1 = data_io()
data_timing_abcd3d_1 = data_timing_abcd3d()
data_timing_abcd3d_1.print_info()
data_factor_model_1 = data_factor_model()
data_factor_model_1.print_info()

from signals import signals_ashare
signals_ashare_1 = signals_ashare()

from analysis_indicators import analysis_factor
analysis_factor_1 = analysis_factor()

from performance_eval import perf_eval_ashare_stra
perf_eval_ashare_stra_1 = perf_eval_ashare_stra()
from algo_opt import algorithm_ashare_weighting
algorithm_ashare_weighting_1 = algorithm_ashare_weighting()

########################################################################
path="D:\\"
file_name ="temp.csv"

df0=pd.read_csv(path+file_name)

obj_data_future = {} 
obj_data_future["dict"]={}
obj_data_future["dict"]["date_start"] = "20191101"
obj_data_future["dict"]["date_end"] =  "20200430"
obj_data_future["df_ashare_ana"] = df0
obj_data_future = data_io_1.get_period_pct_chg_codelist( obj_data_future)



obj_data_future["df_ashare_ana"].to_csv("D:temp2.csv")








