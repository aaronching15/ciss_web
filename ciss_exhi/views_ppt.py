# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
=============================================== 
todo:
功能：专门展示ppt各个页面的网页；利用layui-轮播功能

last  | since 20221023
refernce: 
===============================================
''' 

###########################################################################
### Initialization
from operator import index
from django.shortcuts import render
from django.http import HttpResponse 
from django.views.decorators.csrf import csrf_protect,requires_csrf_token,csrf_exempt

###########################################################################
### 导入项目地址
import sys,os

# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"\\CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\analysis_indicators\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )

import pandas as pd
import numpy as np 
import json 
sys.path.append("..") 
import datetime as dt  
time_now = dt.datetime.now()
### 获取近1w、1m、3m、6m、1year日期
time_now_str = dt.datetime.strftime(time_now   , "%Y%m%d")
time_now_str_pre1d = dt.datetime.strftime(time_now - dt.timedelta(days=1)  , "%Y%m%d")

###########################################################################
@csrf_protect
@requires_csrf_token
@csrf_exempt
def ppt(request):
    ###########################################################################
    ### 基金分析和FOF分析
    #################################################################################
    ### 定义dict对象 context
    context={"info":"none"}
    ### Get data from html templates 
    print("request.POST", type(request.POST.keys() ),request.POST.keys() )
    #################################################################################
    ### 导入常用地址信息 

    









    return render(request,"ciss_exhi/ppt.html",context)
    

























