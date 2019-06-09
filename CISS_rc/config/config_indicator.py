# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：configuration for indicator setting
last update 181110 | since 181110
Menu :


Notes:


===============================================
'''
import os 
###################################################
class config_indicator():

    def __init__(self,indicator_name='name_rc001',indicator_id='id_rc001'):
        self.indicator_name = indicator_name
        self.indicator_id = indicator_id
        

###################################################
class config_indi_mom(config_indicator):
    def __init__(self,indicator_name='name_rc001',indicator_id='id_rc001'):
        self.indicator_name = indicator_name
    
    def technical_ma(self):
    	# generate parameters for technical analysis
    	technical_ma={}
    	# period of moving averagee
    	technical_ma['ma_x'] =[3,8,16,40,100]
    	# relative value of price over moving average price 
    	technical_ma['p_ma']=[0,0,0,0,0]
    	# status of moving average 
    	technical_ma['ma_up']=[1,1,1,1,1] 


    	return technical_ma









