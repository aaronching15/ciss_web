# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
功能：初始化需要的股票价格，回报等数据
数据来源： Choice-API 东方财富Choice量化数据接口
last update 180923 | since  160121
/ 
===============================================
'''
import pandas as pd
import numpy as np
import json
import time
import datetime as dt

class choice_api():
    # 类的初始化操作
    def __init__(self):
        