# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：
last update 201101 | since  181105
Menu :
分析：
1，strategy evaluation 策略评估场景：任何一个策略开发过程，必须具备的要素和过程：
    1，要素：
    2，过程：
    2.1，从市场原始数据到模拟实盘过程中所有关键{inpu,assumptions,ana,stra|algo,sig,trades,account,port,performanc}
        都应该确保准确，严谨。对于不确定或无法复现的过程，要能清晰地定性描述。第一步应该从良好的引用习惯开始，参考任泽平的研究报告。

2，策略评估：
    2.1，质量管理：可用性、可靠性、经济性、与现有体系的协调、策略特性
    2.2，质量功能展开：产品开发过程中最大限度地满足顾客需求的系统化、用户驱动式质量保证方法。
        用户需求+技术 --> 组件配置 --> 软件计划设计 --> 质量参数控制

2，配置文件 | config\config_times.py

Notes: 
refernce: rC_Portfolio_17Q1.py 
===============================================
'''

class evaluations():
    def __init__(self,stra_id='CN001',stra_founder='rc',stra_supervisor="Du"  ):
        self.stra_id = stra_id
        self.stra_founder = stra_founder
        self.stra_supervisor = stra_supervisor









