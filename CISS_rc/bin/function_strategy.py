# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
===============================================
Function:
strategy analysi engine  
last 181110 | since 181110

功能： 

Menu :
分析：
0，策略主要类别：

    

1，输入:
   
2，配置文件 |  
 
3,supportting modules:
	db\\func_stra.py

Notes: 
refernce: rC_Stra_MAX.py 
===============================================
'''

########################################################
class functions():
    def __init__(self, func_name ):
        self.func_name = func_name

class strategy():
    def __init__(self, stra_name ):
        self.stra_name = stra_name
        self.stra_head = self.strategy_head()

    def stra_head(self) :
    	# return important information of strategy
    	strategy_head ={}
    	# expiry date of strategy: default to be 2 years since effective date
    	strategy_head["name_stra"] = 'rc001' 
    	strategy_head["id_stra"] = 'stra_rc_001' 

    	strategy_head["date_effective"] = '3000-12-31' 
    	strategy_head["date_expiry"] = '3000-12-31' 






    	return strategy_head
########################################################

































