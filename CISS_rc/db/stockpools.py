# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：
last update 181102 | since  181102
Menu :
THREE COMPONENTS:
1,class A:
    {INPUT,ALGO,OUTPUT  }
1,function a:
    {INPUT,ALGO,OUTPUT  }
    
class basics() : 
    初始化股票池相关 

Generate stock pool using input.
    stockpool is a subset of whole asset universe 
    since input dataframe might include too many columns, we only choose what we want and 
    output:
    1, stock pool table which include all basic and fundamental information we need 
    2, head dict(json) file which include: generate and update time, operator,market, country
 

Notes: 

===============================================
'''
import pandas as pd 
###################################################
class stockpools():
    def __init__(self, temp_df={},config={},sp_name0=''):
        self.sp_name = sp_name0 

###################################################
class gen_stockpools():
    def __init__(self, id_time_stamp,temp_df={},config={},sp_name0=''):
        self.sp_name = sp_name0 
        self.sp_head = self.gen_stockpool_head(id_time_stamp, config,sp_name0)        
        self.sp_df = temp_df 
         
    def gen_stockpool_head(self,id_time_stamp,config={},sp_name0='') :
        ''' 
        generate stock pool using input.
        stockpool is a subset of whole asset universe 
        since input dataframe might include too many columns, we only choose what we want and 
        output:
        1, stock pool table which include all basic and fundamental information we need 
        2, head dict(json) file which include: generate and update time, operator,market, country
        3, 
        '''  
        stockpool_head = {} 
        ### ref. previous portfolio file. 
        ## get stockpool id using  time stamp
        import sys
        sys.path.append("..")
        # from db.basics import time_admin
        # time_admin1 = time_admin()
        # time_stamp = time_admin1.get_time_stamp()
        if config == {} :
            ## Basic info
            if sp_name0 == '' :
                stockpool_head["name_sp"] = "name_sp_" + id_time_stamp
                stockpool_head["id_sp"] = "id_sp_" + id_time_stamp
                stockpool_head["sp_id_time"]= id_time_stamp
            else :
                stockpool_head["name_sp"] = "name_sp_" + sp_name0
                stockpool_head["id_sp"] = "id_sp_" +id_time_stamp+"_"+sp_name0
                stockpool_head["sp_id_time"]= id_time_stamp
                
            stockpool_head["code"] = ""
            stockpool_head["code_wind"] = ""
            stockpool_head["date_update"] = "" # derived from "Date"
            stockpool_head["date_start"] = ""
            stockpool_head["date_stop"] = ""
            stockpool_head["bsh"] = "" # buy,sell,hold previous:B/S/H
            stockpool_head["value"] = ""    # stock attribution in value     
            stockpool_head["growth"] = ""   # stock attribution in growth
            stockpool_head["pnl_last"] = 0.0  # profit or loss in last trade 
            stockpool_head["pnl_pct_last"] = 0.0  # percentage of profit or loss  in last trade 
            stockpool_head["w_optimal"] = 0.0 # derived from "w_ideal"
            stockpool_head["w_max"] = 0.0 # maximum weight should not exceed such level
            # stockpool_head["w_ideal"] = 0.0
            stockpool_head["if_Hold"] = 0
            stockpool_head["if_ST"] = 0
            stockpool_head["info"] = "" 
            stockpool_head["filename_sps_df"] = "" 


        return stockpool_head 
 

###################################################
class update_stockpools():
    def __init__(self, id_time_stamp,temp_head,temp_df,sp_name0=''):
        self.sp_name = sp_name0 
        self.sp_head = temp_head
        self.sp_df = temp_df 



