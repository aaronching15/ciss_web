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

    # cmc使用范例
    data = c.cmc("300059.SZ", "OPEN,CLOSE,HIGH", (datetime.today() + timedelta(-6)).strftime("%Y-%m-%d"), datetime.today().strftime("%Y-%m-%d"), "RowIndex=2,Ispandas=0")
    print("cmc输出结果======分隔线======")
    if(not isinstance(data, c.EmQuantData)):
        print(data)
    else:
        if(data.ErrorCode != 0):
            print("request cmc Error, ", data.ErrorMsg)
        else:
            for i in range(0, len(data.Indicators)):
                for j in range(0, len(data.Dates)):
                    print("indicator=%s, value=%s" % (data.Indicators[i], str(data.Data[i][j])))

    # csd使用范例
    data = c.csd("300059.SZ,600425.SH", "open,close", "2016-07-01", "2016-07-06", "RowIndex=1,period=1,adjustflag=1,curtype=1,pricetype=1,year=2016,Ispandas=0")

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

    # css使用范例
    data = c.css("300059.SZ, 000002.SZ, 000002.SH", "open,close", "TradeDate=20170308, Ispandas=0")
    print("css输出结果======分隔线======")
    if not isinstance(data, c.EmQuantData):
        print(data)
    else:
        if(data.ErrorCode != 0):
            print("request css Error, ", data.ErrorMsg)
        else:
            for code in data.Codes:
                for i in range(0, len(data.Indicators)):
                    print(data.Data[code][i])

    # sector使用范例
    # 001004 全部A股板块
    data = c.sector("001004", "2016-04-26")
    if data.ErrorCode != 0:
        print("request sector Error, ", data.ErrorMsg)
    else:
        print("sector输出结果======分隔线======")
        for code in data.Data:
            print(code)

    # tradedate使用范例
    data = c.tradedates("2016-07-01", "2016-07-12")
    if(data.ErrorCode != 0):
        print("request tradedates Error, ", data.ErrorMsg)
    else:
        print("tradedate输出结果======分隔线======")
        for item in data.Data:
            print(item)

    # getdate使用范例
    data = c.getdate("20160426", -3, "Market=CNSESH")
    if(data.ErrorCode != 0):
        print("request getdate Error, ", data.ErrorMsg)
    else:
        print("getdate输出结果======分隔线======")
        print(data.Data)

    #实时行情订阅使用范例
    data = c.csq('000850.SH', 'TIME,Now,Volume','Pushtype=1',csqCallback)
    if(data.ErrorCode != 0):
        print("request csq Error, ", data.ErrorMsg)
    else:
        print("csq输出结果======分隔线======")
        text = input("press any key to cancel csq \r\n")
        #取消订阅
        data = c.csqcancel(data.SerialID)

    #日内跳价使用范例
    print("cst输出结果======分割线======")
    data = c.cst('600000.SH,300059.SZ', 'TIME,OPEN,HIGH,LOW,NOW', '093000', '094000','',cstCallBack)
    if(data.ErrorCode != 0):
        print("request cst Error, ", data.ErrorMsg)
    else:
        input("press any key to quit cst \r\n")

    #行情快照使用范例
    data = c.csqsnapshot("000005.SZ,600602.SH,600652.SH,600653.SH,600654.SH,600601.SH,600651.SH,000004.SZ,000002.SZ,000001.SZ,000009.SZ", "PRECLOSE,OPEN,HIGH,LOW,NOW,AMOUNT")
    if(data.ErrorCode != 0):
        print("request csqsnapshot Error, ", data.ErrorMsg)
    else:
        print("csqsnapshot输出结果======分割线======")
        for key,value in data.Data.items():
            print(key, ">>> ", end="")
            for v in value:
                 print(v, " ", end="")
            print()
            
    #获取专题报表使用范例
    data = c.ctr("INDEXCOMPOSITION", "", "IndexCode=000300.SH,EndDate=2017-01-13")
    if(data.ErrorCode != 0):
        print("request ctr Error, ", data.ErrorMsg)
    else:
        print("ctr输出结果======分割线======")
        for key,value in data.Data.items():
            for v in value:
                 print(v, " ", end="")
            print()

    #选股使用范例
    data = c.cps("B_001004", "s0,OPEN,2017/2/27,1;s1,NAME", "[s0]>0", "orderby=rd([s0]),top=max([s0],100)")
    if(data.ErrorCode != 0):
        print("request cps Error, ", data.ErrorMsg)
    else:
        print("cps输出结果======分割线======")
        for it in data.Data:
           print(it)

    #宏观指标服务
    data = c.edb("EMM00087117","IsPublishDate=1")
    if(data.ErrorCode != 0):
        print("request edb Error, ", data.ErrorMsg)
    else:
        print("edbid           date          ",end="")
        for ind in data.Indicators:
            print(ind, end="   ")
        print("")
        for code in data.Codes:
            for j in range(0, len(data.Dates)):
                print(code, "    ", data.Dates[j], end="   ")
                for i in range(0, len(data.Indicators)):
                    print(data.Data[code][i][j], end="   ")
                print("")
    #宏观指标id详情查询
    data = c.edbquery("EMM00058124,EMM00087117,EMG00147350")
    if(data.ErrorCode != 0):
        print("request edbquery Error, ", data.ErrorMsg)
    else:
        print("edbid         ",end="")
        for ind in data.Indicators:
            print(ind, end="   ")
        print("")
        for code in data.Codes:
            for j in range(0, len(data.Dates)):
                print(code, "    ", end="   ")
                for i in range(0, len(data.Indicators)):
                    print(data.Data[code][i][j], end="   ")
                print("")

    #新建组合
    data = c.pcreate("quant001", "组合牛股", 100000000, "这是一个牛股的组合")
    if(data.ErrorCode != 0):
        print("request pcreate Error, ", data.ErrorMsg)
    else:
        print("create succeed")
    orderdict = {'code':['300059.SZ','600000.SH'],
                 'volume':[1000,200],
                 'price':[13.11,12.12],
                 'date':['2017-08-14','2017-08-24'],
                 'time':['14:22:18','14:22:52'],
                 'optype':[eOT_buy,eOT_buy],
                 'cost':[0,3],
                 'rate':[0,2]}
    #组合下单
    data = c.porder("quant001", orderdict, "this is a test")
    if(data.ErrorCode != 0):
        print("porder Error, ", data.ErrorMsg)
    else:
        print("order succeed")

    #组合报表查询
    data = c.preport("quant001", "record", "startdate=2017/07/12,enddate=2018/01/15")
    if(data.ErrorCode != 0):
        print("request preport Error, ", data.ErrorMsg)
    else:
        for ind in data.Indicators:
            print(ind, end="   ")
        print("")
        for k in data.Data:
            for it in data.Data[k]:
                print(it, end="   ")
            print("")

    #组合信息查询
    data = c.pquery()
    if(data.ErrorCode != 0):
        print("request pquery Error, ", data.ErrorMsg)
    else:
        print("[key]:",end="")
        for index in range(0, len(data.Indicators)):
            print("\t", data.Indicators[index],end="")
        print("")
        for k,v in data.Data.items():
            print(k,": ", end="")
            for vv in v:
                print("\t", vv, end="")
            print("")

    #删除组合
    data = c.pdelete("quant001")
    if(data.ErrorCode != 0):
        print("request pdelete Error, ", data.ErrorMsg)
    else:
        print("delete succeed")

#退出
    data = logoutResult = c.stop()
except Exception as ee:
    print("error >>>",ee)
    traceback.print_exc()
else:
    print("over")
