# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：configuration for operating data.For instance, data from Wind API
last update 181226 | since 181107
Menu :
1, return,price and volumn data 

2, fundamental data 

3, basic information supportting investment process
    For instance, list of index components given date 

todo:


Notes:


===============================================
'''
import os 
###################################################
class config_data():
    def __init__(self,config_name=''):
        self.config_name = config_name 

    def gen_config_wsd(self,type_wsd='week') :
        # generate all necessary configurations 
        if type_wsd in ['week','w','W','Week'  ]:
            # original
            # items = ["open","high","low","close","volume","amt","turn"]
            # short items
            items = ["open","close","volume","amt","turn"]
            para = "Period=W;PriceAdj=F"
        elif type_wsd in [ 'day','d','D','Day' ]:
            # original
            # items = ["open","high","low","close","volume","amt","turn"]
            # short items
            items = ["open","close","volume","amt","turn"]
            para = "PriceAdj=F"

        elif type_wsd in [ 'day_us','day_hk' ]:
            # for US or HK market
            # items = ["open","high","low","close","volume","amt","turn"]
            # short items
            items = ["open","close","volume"]
            para = "PriceAdj=F"

        config={}
        config["items"] = items
        config["para"] = para
        
        return config






































