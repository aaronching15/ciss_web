# -*- coding:utf-8 -*-

__author__ = 'weijie'

from EmQuantAPI import *
from datetime import timedelta, datetime

def startCallback(message):
    print("[EmQuantAPI Python]", message)
    return 1
def csqCallback(quantdata):
    """
    csqCallback 是EM_CSQ订阅时提供的回调函数模板。该函数只有一个为c.EmQuantData类型的参数quantdata
    :param quantdata:c.EmQuantData
    :return:
    """
    print ("csqCallback,", str(quantdata))
    
def cstCallBack(quantdata):
    '''
    cstCallBack 是日内跳价服务提供的回调函数模板
    '''
    for i in range(0, len(quantdata.Codes)):
        length = len(quantdata.Dates)
        for it in quantdata.Data.keys():
            print(it)
            for k in range(0, length):
                for j in range(0, len(quantdata.Indicators)):
                    print(quantdata.Data[it][j * length + k], " ",end = "")
                print()


import traceback

try:
    #调用登录函数（激活后使用，不需要用户名密码）
    loginResult = c.start("ForceLogin=1")
    if(loginResult.ErrorCode != 0):
        print("login in fail")
        exit()

    # # cmc使用范例
    # data = c.cmc("300059.SZ", "OPEN,CLOSE,HIGH", (datetime.today() + timedelta(-6)).strftime("%Y-%m-%d"), datetime.today().strftime("%Y-%m-%d"), "RowIndex=2,Ispandas=0")
    # print("cmc输出结果======分隔线======")
    # if(not isinstance(data, c.EmQuantData)):
    #     print(data)
    # else:
    #     if(data.ErrorCode != 0):
    #         print("request cmc Error, ", data.ErrorMsg)
    #     else:
    #         for i in range(0, len(data.Indicators)):
    #             for j in range(0, len(data.Dates)):
    #                 print("indicator=%s, value=%s" % (data.Indicators[i], str(data.Data[i][j])))

    # csd使用范例
    # 300059.SZ,600425.SH
    # 00700.HK
    data = c.csd("APPL.O", "open,close", "2016-07-01", "2016-07-06", "RowIndex=1,period=1,adjustflag=1,curtype=1,pricetype=1,year=2016,Ispandas=0")

    print("csd输出结果======分隔线======")
    if not isinstance(data, c.EmQuantData):
        print(data)
    else:
        if(data.ErrorCode != 0):
            print("request csd Error, ", data.ErrorMsg)
        else:
            for code in data.Codes:
                for i in range(0, len(data.Indicators)):
                    for j in range(0, len(data.Dates)):
                        print(data.Data[code][i][j])

    # # css使用范例
    # data = c.css("300059.SZ, 000002.SZ, 000002.SH", "open,close", "TradeDate=20170308, Ispandas=0")
    # print("css输出结果======分隔线======")
    # if not isinstance(data, c.EmQuantData):
    #     print(data)
    # else:
    #     if(data.ErrorCode != 0):
    #         print("request css Error, ", data.ErrorMsg)
    #     else:
    #         for code in data.Codes:
    #             for i in range(0, len(data.Indicators)):
    #                 print(data.Data[code][i])

    # sector使用范例
    # 001004 全部A股板块
    # data = c.sector("001004", "2016-04-26")
    # if data.ErrorCode != 0:
    #     print("request sector Error, ", data.ErrorMsg)
    # else:
    #     print("sector输出结果======分隔线======")
    #     for code in data.Data:
    #         print(code)
 
#退出
    data = logoutResult = c.stop()
except Exception as ee:
    print("error >>>",ee)
    traceback.print_exc()
else:
    print("over")
