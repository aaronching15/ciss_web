# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
===============================================
Function:
serve as optimization calculation 
last 181110 | since 181110
功能： 

Menu :
分析：
0，main function
    

1，输入:
   
2，配置文件 |   

THREE COMPONENTS:
1,class A:
    {INPUT,ALGO,OUTPUT  }
1,function a:
    {INPUT,ALGO,OUTPUT  }

Notes: 
refernce: rC_Portfolio.py 

===============================================
'''
import pandas as pd 
import sys 
sys.path.append("..")

class algorithm():
    def __init__(self, algo_name ):
        self.algo_name = algo_name



class optimizer():
    def __init__(self, opt_name ):
        self.opt_name = opt_name

        # import scipy.optimize as sco 
        # p294 


    def optimizer_weight(self,stra_estimates_group ):
    	# import all strategy estimation and return optimized weight list 
    	# todo todo 
        if len( stra_estimates_group) ==1 :
            optimizer_weight_list  = stra_estimates_group['key_1']
        else :
            # we need to make ranking list and calculation best strategy decision 
			# from all strategy suggestion.
            optimizer_weight_list  = 1

        return optimizer_weight_list