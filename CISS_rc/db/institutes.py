# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function: define type of institutes and restrictions from beneficiaries 
and investors.

last update 181031 | since 181031
Menu :
THREE COMPONENTS:
1,class A:
    {INPUT,ALGO,OUTPUT  }
1,function a:
    {INPUT,ALGO,OUTPUT  }



===============================================
'''

class institutes():
    def __init__(self,institute_name='',institute_type='mutual fund'):
    self.institute_name = institute_name
    # initial date is the beginning date of account 
    self.institute_type = institute_type





###################################################
class mutual_fund():
    def __init__(self,institute_name='',institute_type='mutual fund'):
    self.institute_name = institute_name
    # initial date is the beginning date of account 
    self.institute_type = institute_type

class pensions_insurance():
    # 养老和保险资金
    # 偿二代规则，以及保险资金运用要求下,对于沪深300成分，中小创股票有不同的风险权重
    #   对应了不同比例的资本占用。另外对于亏损股票往往对应更多的资本占用。
    # 
    def __init__(self,institute_name='',institute_type='mutual fund'):
    self.institute_name = institute_name
    # initial date is the beginning date of account 
    self.institute_type = institute_type





    