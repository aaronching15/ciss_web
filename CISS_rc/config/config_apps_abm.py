# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：configuration for abm model setting 
last update 181109 | since 181109
Menu :


Notes:


===============================================
'''
import os 
###################################################.
class config_apps_abm():

    def __init__(self,init_cash,date_start,date_end,port_name):
        # port_name=  name_id
        self.config ={}
        self.config["init_cash"] = init_cash #100000000.0
        self.config["date_start"] = date_start # "20140531" 
        self.config["date_end"] =  date_end   # "20141130"
        self.config["account_name"] = port_name # 
        self.config["trade_name"] = port_name # 
        self.config["signal_name"] = port_name # 

        # init_date="20140531", 
        # port_df,
        # init_cash=100000000.0
        # init_date="20140531",
        # account_name






