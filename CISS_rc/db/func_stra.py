# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
===============================================
Function:
Generate portfolio using symbol list as input 
        last 181109 | since 181109
功能： 
    若signals交易信号中资产配置比例数值有问题，需要进行调整

Menu :
THREE COMPONENTS:
1,class A:
    {INPUT,ALGO,OUTPUT  }
1,function a:
    {INPUT,ALGO,OUTPUT  }


分析：
0，策略算法开发流程：
    假设 --》 方法论，模型 --》 
    

1，输入:
   
2，配置文件 |  
 
3,output file 
    1,head json file of portfolio
    2,portfolio dataframe of portfolio 
    3,stockpool dataframe of portfolio

Notes: 
refernce: rC_Stra_MAX.py 
===============================================
'''
class functions():
    def __init__(self, func_name ):
        self.func_name = func_name

class strategies():
    def __init__(self, stra_name ):
        self.stra_name = stra_name

    # 策略流程应该是 输入信息管理，计算过程管理(可能涉及专属的策略模型)，输出信息。

##################################################################
class stra_prepare():
    def __init__(self, stra_name ):
        self.stra_name = stra_name
        # 

##################################################################

class stra_allocation():
    def __init__(self, stra_name ):
        self.stra_name = stra_name
        # generate allocation weights for portfolio assets 

    def stock_weights(self,ind_level,sty_v_g, sp_df) :
        # update:增加对市场组合的权重重新计算 | 190412
        # todo import config file
        # sty_v_g = 'value' or 'growth'
        # INPUT sp_df
        # OUTPUT ALLOCATION 
        # print(sp_df.loc[:,['code','ind1_code']].head()  )

        ####################################################################
        ### set column item that we want to filtering | col_name = 'w_allo_'+'growth'+'_ind3'
        
        if not ind_level == "0" :
            col_name = 'w_allo_'+sty_v_g+'_ind'+ind_level
            col_w_value = 'w_allo_value_ind'+ind_level
            col_w_growth = 'w_allo_growth_ind'+ind_level
            weight_list= sp_df.loc[:,['code','ind1_code','ind2_code','ind3_code',col_w_value,col_w_growth]]
            # we want to drop stock with portfolio weight smaller than 0.1%, which means no significant support
            # to portfolio return or risks.
            # when calc 601020, we found 600747 holds 0.1088% at 20140531,we think we want to excule this type of firm
            weight_list= weight_list[ weight_list[col_name ] >= 0.0011 ]       
        else :
            ####################################################################
            ### working on whole market 
            ind_level = "1"
            col_name = 'w_allo_'+sty_v_g+'_ind'+ind_level
            col_w_value = 'w_allo_value_ind'+ind_level
            col_w_growth = 'w_allo_growth_ind'+ind_level
            weight_list= sp_df.loc[:,['code','ind1_code','ind2_code','ind3_code',col_w_value,col_w_growth]]
            # we want to drop stock with portfolio weight smaller than 0.1%, which means no significant support
            # to portfolio return or risks.
            # when calc 601020, we found 600747 holds 0.1088% at 20140531,we think we want to excule this type of firm
            weight_list= weight_list[ weight_list[col_name ] >= 0.0005 ]  
            weight_list[col_name] = weight_list[col_name]/weight_list[col_name].sum()

            # Twice optimze :Drop small weights again 
            weight_list= weight_list[ weight_list[col_name ] >= 0.0005 ]  
            weight_list[col_name] = weight_list[col_name]/weight_list[col_name].sum()
            
            weight_list[col_w_value] = weight_list[col_w_value]/weight_list[col_w_value].sum()
            weight_list[col_w_growth] = weight_list[col_w_growth]/weight_list[col_w_growth].sum()

            # print( weight_list.info() )
            # print( weight_list.head() )
            # print( weight_list[col_w_value].sum() )
            # print( weight_list[col_w_growth].sum() )
            # asd

        ### 根据对利润的配置权重和估值，调整配置权重。 估值是个tricky的问题。
        # 读取最新股本，结合股价，计算出每个股票在t时间的P/E：如果w_allo 70:30, 估值调整后w_mv= 59.3:40.7
        # 如果从效率的角度，似乎应该全部选择每单位市值利润最大的股票 profit/(1rmb mv)。如果从均衡捕捉行业价值的角度，
        # 同时控制个股的风险，那么我们的方式就比较合适。

        ''' steps:
        1,get lastest number of shares, using close at reference date to get market value.p/e= MV/profit_q4_es
        2,get new weight of stock value using p/e and w_allo_value|growth
        
        
        '''



        return weight_list 








































