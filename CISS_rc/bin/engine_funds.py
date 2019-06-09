# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
TODO: funds是一个包括了一系列组合的组合，fund1：{n1*port1，n2*port2,...} 

last update   | since 190128

Menu : 
    1，基金资金进出行为分析：
        1.1，初始建仓模块。
        1.2，申购赎回模块。
        1.3，Qs:是直接从fund模块生成交易计划，还是通过port_manage ?|理论上组合投资管理的前提是出入金，因此出入金的行为决定了投资行为，例如组合成立期可以看做是一次性大的入金。

Function：
    1，基金的建立有2种：1，在现有模拟组合之上，根据给定的模拟申购和赎回，建立
    2，申购赎回和分红等行为应该对应建仓，按比例调仓，减仓。
        2.1，建仓期，核心目标是控制对市场流动性冲击的风险
        2.2，调仓期，核心目标是精确管理调整的比例，以及在调整过程中争取正偏离。|两种情况，一是策略的定期调整或临时调整，
            另一种是由于资金申购赎回导致的临时调整。
        2.3，idea：trade plan 中可以标注交易的动机，是策略因素还是调仓因素。|我们假设通常投资经理没有动力把策略和调仓混起来。
            但是市场高点投资经理想降低仓位时，可能会在申购资金到账时不买；市场低点资金赎回时，投资经理可以尽快卖出对应的赎回款。


notes:
    1,
    2，
derived from rC_Portfolio_17Q1.py
===============================================
'''


import sys
import pandas as pd 
sys.path.append("..")

class Engine_funds():
    #####################################################
    def __init__(self ):
        self.name = "engine of funds"

        