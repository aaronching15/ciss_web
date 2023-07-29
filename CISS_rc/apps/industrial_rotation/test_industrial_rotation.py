# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
notes:因为程序跑不了，改成用abcd3d
基于ABM模型的指标计算行业轮动策略的大类因子。
last 200705  | since 191205
Abstract：
截至20191211，最佳配置参数是5个行业，30只股票

Function:


MENU :

0，基础信息：代码、1、2级行业分类							
	股票代码	一级行业代码	二级行业代码				
1，基础财务数据：直接打分方式：							
	一致预测ROE(FY2)	一致预测净利润2年复合增长率	长期股权投资	一致预测营业利润2年复合增长率	总资产周转率(TTM)		
2，3个ABM核心指标及变动：9个直接打分							
	当年净利润预测	当年收入预测	当年经营性现金流预测	当年净利润增长值	当年收入增长值	当年经营性现金流增长值	当年净利润增长率
3，相对打分1：找出价值靠近价值锚、成长靠近成长锚的 
4，相对打分2：判断行业整体是否向好；行业内的集中度-越高越利于大公司，越低越利于小公司

对以上4个环节分别建模，观察不同时期每个大类的绩效，最后形成加权优化的方案。

Data:
GICS一级行业list表：[40 60 20 15 30 45 25 35 55 10 50]
'''
#################################################################################
### 指标数据：
### 1，基础财务数据：直接打分方式
cols_financial_indicator =["west_avgroe_FY2","west_netprofit_CAGR","longcapitaltoinvestment","west_avgoperatingprofit_CAGR","turnover_ttm"]

### 2，3个ABM核心指标及变动：9个直接打分
cols_abm_3indicator = ["profit_q4_es","revenue_q4_es","cf_oper_q4_es","profit_q4_es_dif","revenue_q4_es_dif","cf_oper_q4_es_dif","profit_q4_es_dif_pct","revenue_q4_es_dif_pct","cf_oper_q4_es_dif_pct"]

### 3，相对打分1：找出价值靠近价值锚、成长靠近成长锚的 | 
# "code_anchor_value",
cols_anchor_indicator_value = ["profit_anchor_value","profit_dif_anchor_value","revenue_anchor_value","cf_oper_anchor_value"]
# "code_anchor_growth",
cols_anchor_indicator_growth = ["profit_anchor_growth","profit_dif_anchor_growth","revenue_anchor_growth","cf_oper_anchor_growth"]

### 4，相对打分2：判断行业整体是否向好；行业内的集中度-越高越利于大公司，越低越利于小公司
cols_ind1_indicator = ["ind1_pct_profit_q4_es","ind1_pct_revenue_q4_es","ind1_pct_cf_oper_q4_es","w_allo_value_ind1","w_allo_growth_ind1"]

#################################################################################
### 按日期导入ABM模型数据
# date_list = ["20140531","20141130","20150531","20151130","20160531","20161130"]
# date_list = date_list + ["20170531","20171130","20180531"]
# date_list = date_list + ["20181130","20190531","20191130"]

date_list = ["20200531"]

print(   date_list ) 
#################################################################################
### Initialization 
from industrial_rotation import ind_rotation 
ind_rot = ind_rotation()
ind_rot.print_info()

################################################################################
### CHOICE 1 :对indicator list 分别计算df_score
'''
1:GICS ind level= 1 : 5 industry and 30 sotcks at max
    11 ind1 in csi800;
2,GICS ind level= 2 : 10 industry and 60 sotcks at max
    24 ind2 in csi800 and average number of stock is 33 
    according to this para set, 

'''
para_dict= {}
para_dict["ind_level"] = 2
para_dict["para_indnumber"] = 12 # 默认是5个行业
para_dict["para_stocknumber"] = 60  # 默认是100只股票
### 选择想要的indicator list
#3 types: cols_financial_indicator  | cols_abm_3indicator | cols_ind1_indicator
# list of industry indicator 
list_dict={}
list_dict["financial_indicator"] = cols_financial_indicator
list_dict["abm_3indicator"] = cols_abm_3indicator
list_dict["ind1_indicator"] = cols_ind1_indicator
for list_str in list_dict :
    list_indicator = list_dict[ list_str  ]
    para_dict["indi_list"] =  list_str
    #  "abm_3indicator" # 根据col list 名称调整  
    # 1，要增加行业级别选择的功能

    str_output = "para_" + "ind_" + str(para_dict["ind_level"] ) +"_"
    str_output = str_output + str(para_dict["para_indnumber"] )+"_"+str(para_dict["para_stocknumber"]) +"_"
    str_output = str_output +str(len(list_indicator )) + "_indicator"
    para_dict["str_output"] = str_output # 根据col list 名称调整 


    df_port_return = ind_rot.test_score_weight_return_list_indicator(list_indicator, para_dict,date_list )

    
asd


##############################################################################   
##############################################################################    
### CHOICE2:将所有指标打分加总 ,temp_col = "sum",所有的indicator分数和sum保存在 df_score_sum 里了
'''
1:GICS ind level= 1 : 5 industry and 30 sotcks at max
    11 ind1 in csi800;
2,GICS ind level= 2 : 10 industry and 60 sotcks at max
    24 ind2 in csi800 and average number of stock is 33 
    according to this para set, 

'''

### Method 2:Define ideally indicators
list_indicator= ["revenue_q4_es","revenue_q4_es_dif","west_netprofit_CAGR"]
list_indicator=list_indicator+["profit_q4_es_dif_pct","cf_oper_q4_es","w_allo_value_ind1"]
list_indicator=list_indicator+["ind1_pct_revenue_q4_es","profit_q4_es","w_allo_growth_ind1"]


### Another choice 
list_indicator= ["profit_q4_es_dif_pct","revenue_q4_es_dif","cf_oper_q4_es"]
# 反思：20190530以来收益不佳，但west_avgroe_FY2指标却比较好，代替w_allo_growth_ind1指标


#############################################
### 设置组合参数
para_dict= {}
para_dict["ind_level"] = 2  # only be 1 or 2 for current raw data 
para_dict["para_indnumber"] = 6 # 默认是5个行业,
para_dict["para_stocknumber"] = 40  # 默认是100只股票
#3 types: cols_financial_indicator  | cols_abm_3indicator | cols_ind1_indicator
# str_output = "para_5_30_top9adj_indicator"

str_output = "para_" + "ind_" + str(para_dict["ind_level"] ) +"_"
str_output = str_output + str(para_dict["para_indnumber"] )+"_"+str(para_dict["para_stocknumber"]) +"_"
str_output = str_output +str(len(list_indicator )) + "_indicator"
para_dict["str_output"] = str_output # 根据col list 名称调整 

para_dict["dir_output"] = str_output 

#############################################
### 剔除特定行业：例如金融的 "40"
# para_dict["drop_ind_list"] = ["40"]
# para_dict["dir_output"] = str_output +"_drop40"
# print( "Drop list of industry code", para_dict["drop_ind_list"] )

#############################################
### 限制单一行业权重上限
# if行业数量3、5、7，则单一行业权重上限 
import math
para_dict["w_max_indX"] = math.floor( 100/para_dict["para_indnumber"]*1.1+10 )/100
para_dict["dir_output"] = str_output +"_w-max"
print( "Maximum weight for single industry or sector ", para_dict["w_max_indX"] )

### 限制单一行业股票数量上限
para_dict["num_max_indX"] = math.floor( para_dict["para_stocknumber"]*1.5/para_dict["para_indnumber"] )
print( "Maximum number of stocks for single industry or sector ", para_dict["num_max_indX"] )
para_dict["dir_output"] = str_output +"_w-max" + "_num-max_ano3"
df_pms = ind_rot.test_score_weight_return_sum(list_indicator,para_dict,date_list )


### End
asd

##############################################################################    
### CHOICE3: 计算剔除金融行业和控制每个行业权重。  
### GICS一级行业list表：[40 60 20 15 30 45 25 35 55 10 50]











#################################################################################
### 以下是industrial_rotation.py 的原始版本

#################################################################################
### Initialization 
import os 
# 获取当前目录 os.getcwd() =: G:\zd_zxjtzq\ciss_web
import sys
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )

import pandas as pd 
import numpy as np 
import math
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

path_abm = "D:\\CISS_db\\Active_Benchmark_Model_2019\\abm_weights\\"

path_output = "D:\\CISS_db\\csi800_industrial_rotation\\"

file_return = "return_data_0531_1130.csv"
df_return = pd.read_csv( path_output+  file_return  ) 
print("df_return "  )
print( df_return.head() )

### get list of ind1 
file_date = "weights_"+"20140531" + "_1.csv"
df_abm = pd.read_csv( path_abm + file_date ) 
df_abm_des = df_abm.describe() 
list_ind1 = df_abm["ind1_code"].drop_duplicates().values

#################################################################################
### FUNCTION 标准方程：根据指标计算行业内的标准化得分、根据标准分计算指标权重
def get_score_mkt(df_abm,df_score,temp_col ) :
    '''score 计算方法：
    1，对样本内全部数据，计算均值、方差，最大值最小值，取2倍标准差，若超过则对于标准化0~1的分值+0.1或-0.1作为惩罚
    2,正太分布1~3个标准差对应的概率分布：68.27%、95.45%、99.74%
    notes:
    1,df_abm和df_score 应该有相同的index；
    '''
    df_abm_des = df_abm.describe() 
                                   
    # print( df_abm_des.loc["mean", temp_col ]  )
    # df_abm_des.to_csv("D:\\temp.csv")
    # print("Debug2=====")
    # print( df_abm_des  )

    # index of df_abm_des | Index(['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max'], dtype='object')
    ### 对给定指标temp_col,
    temp_mean = df_abm_des.loc["mean", temp_col ]
    temp_std  = df_abm_des.loc["std", temp_col ]
    temp_min  = df_abm_des.loc["min", temp_col ]
    temp_max  = df_abm_des.loc["max", temp_col ]        

    if temp_max - temp_min == 0.0 :
        # prevent cannot devided by zero error . 
        df_score.loc[df_abm.index ,  temp_col+"_mkt"] = 0.0 
        print("Debug=====", df_abm_des  )
    else :
        for temp_i in df_abm.index :
            temp_value = df_abm.loc[temp_i, temp_col]
            if temp_value <  temp_mean -3*temp_std :
                temp_score = -0.1
            elif temp_value >  temp_mean + 3*temp_std :
                temp_score = 1.1
            else : 
                ### 对于存在缺失值的位置，通常有用均值的，也有用较小值的；这里可以用miu-2*std 代替
                # 判断是否nan，pd.isnull(temp_value), pd.isna(temp_value ) ；注意这里np.nan没用
                if pd.isnull(temp_value) :
                    temp_score = max(0, ( temp_mean-2*temp_std- temp_min )/(temp_max - temp_min) ) 
                else :
                    temp_score = ( temp_value - temp_min )/(temp_max - temp_min)
                
                # print( temp_value, temp_score )
                # input1= input( "temp_score" )

            df_score.loc[temp_i,temp_col+"_mkt"] = temp_score

    return df_score 
# 根据指标计算行业内的标准化得分
def get_score_ind1(df_abm,df_score,temp_col ) :
    '''score 计算方法：
    1，对每个一级行业的列数据，计算均值、方差，最大值最小值，取2倍标准差，若超过则对于标准化0~1的分值+0.1或-0.1作为惩罚
    2,正太分布1~3个标准差对应的概率分布：68.27%、95.45%、99.74%

    notes:
    1,df_abm和df_score 应该有相同的index；
    '''
    list_ind1 = df_abm["ind1_code"].drop_duplicates().values
    # list_ind1  [40 60 20 15 30 45 25 35 55 10 50]
    print("list_ind1 ", list_ind1)
    for temp_ind1 in list_ind1 :
        df_abm_sub = df_abm[ df_abm["ind1_code"]== temp_ind1 ]
        df_abm_sub_des = df_abm_sub.describe() 
        
        # index of df_abm_des | Index(['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max'], dtype='object')
        ### 对给定指标temp_col,
        temp_mean = df_abm_sub_des.loc["mean", temp_col ]
        temp_std  = df_abm_sub_des.loc["std", temp_col ]
        temp_min  = df_abm_sub_des.loc["min", temp_col ]
        temp_max  = df_abm_sub_des.loc["max", temp_col ]        

        if temp_max - temp_min == 0.0 :
            # prevent cannot devided by zero error . 
            df_score.loc[df_abm_sub.index ,  temp_col+"_ind1"] = 0.0 
            print("Debug=====", df_abm_sub_des  )
        else :
            for temp_i in df_abm_sub.index :
                temp_value = df_abm_sub.loc[temp_i, temp_col]
                if temp_value <  temp_mean -3*temp_std :
                    temp_score = -0.1
                elif temp_value >  temp_mean + 3*temp_std :
                    temp_score = 1.1
                else : 
                    ### 对于存在缺失值的位置，通常有用均值的，也有用较小值的；这里可以用miu-2*std 代替
                    # 判断是否nan，pd.isnull(temp_value), pd.isna(temp_value ) ；注意这里np.nan没用
                    if pd.isnull(temp_value) :
                        temp_score = max(0, ( temp_mean-2*temp_std- temp_min )/(temp_max - temp_min) ) 
                    else :
                        temp_score = ( temp_value - temp_min )/(temp_max - temp_min)
                    
                    # print( temp_value, temp_score )
                    # input1= input( "temp_score" )

                df_score.loc[temp_i,temp_col+"_ind1"] = temp_score

    return df_score 
# 根据标准分计算指标权重
def get_weight(df_weight,df_score,temp_col,list_ind1 ,para_dict ) :
    '''
    目标：根据一级行业得分，计算行业内组合的配置和全市场组合的配置
    逻辑：
        1，行业轮动的角度，一段时期内，一定是33%~50%的行业有较好的超额收益，单行业内top20~30%的股票有较好的超额收益。
            股票梳理太少容易降低选股命中率，太多容易分散超额收益机会。
        1, 股票数量不超过100只，因此800个股票中选12.5%。11个大类行业，有的行业个股400多，有的3个，
            能做的是在300~800只股票中，寻找行业内的相对机会和全市场的整体机会。
        2，每个行业只少3只股票，最多
    全市场组合计算方法：
    1，计算初步权重w_mkt : 基于全市场的组合；剔除排名靠后的4个行业后计算行业配置权重。
    2, 对于每个行业,计算指标得分前20%的股票，
    3，对市场得分和行业得分进行加权，最后排序选择前150只股票，并剔除权重低于0.3%的股票

    working columns：[temp_col+"_ind1", temp_col+"_mkt" ]
    notes:
    1,df_abm和df_score 应该有相同的index；
    2,para_dict 里包括了行业数量的选取和股票数量的选取
    '''
    num_ind1 = int( para_dict["para_indnumber"] ) # = 5  
    num_stock = int(para_dict["para_stocknumber"])  # = 100  
    
    df_describe = df_score.describe()
    print("df_describe ", df_describe  )
    ### 1,全市场：按行业计算平均得分,剔除排名靠后的4个行业后计算行业配置权重
    df_score_ind1 = df_score.groupby("ind1_code").mean() 
    df_score_ind1 = df_score_ind1.sort_values(by=temp_col+"_mkt",ascending=False)
    ### top 7 and last 4 ind1 
    list_ind1_keep = df_score_ind1.index[:num_ind1].values
    list_ind1_drop = df_score_ind1.index[num_ind1:].values
    print("list_ind1_drop",list_ind1_drop)

    temp_df= df_score[ df_score["ind1_code"].isin( list_ind1_drop )  ]
    temp_index =temp_df.index
    print( "temp_index ",temp_index )
    df_score.loc[temp_index , temp_col+"_mkt"] = 0.00
    df_weight[temp_col+"_mkt"] = df_score[temp_col+"_mkt"]/df_score[temp_col+"_mkt"].sum()

    ### 2, 对于每个行业,计算指标得分前20%的股票
    list_index =[]
    for temp_ind1 in list_ind1 : 
        df_score_sub = df_score[ df_score["ind1_code"]==temp_ind1 ]
        temp_num = len( df_score_sub.index )
        temp_num2 = max(1,math.floor(temp_num*0.2 ) ) 
        ### 取行业内分数排名前20%的股票
        df_score_sub = df_score_sub.sort_values(by=temp_col+"_mkt",ascending=False)        
        list_index = list_index + list( df_score_sub.index[:temp_num2] ) 
        # print("list_index ",len(list_index)  )

    list_index = sorted(list_index) 
    df_weight.loc[list_index, temp_col+"_ind1"] =  df_score.loc[list_index, temp_col+"_ind1"]
    df_weight[ temp_col+"_ind1"] = df_weight[ temp_col+"_ind1"].fillna(value=0.0)
    print("df_weight ", df_weight )
    df_weight[ temp_col+"_ind1"] = df_weight[temp_col+"_ind1"]/df_weight[temp_col+"_ind1"].sum()
    
    ### 3，对市场得分和行业得分进行加权，最后排序选择前150只股票，并剔除权重低于0.3%的股票
    df_weight[ temp_col ] = df_weight[temp_col+"_ind1"]*0.5  +df_weight[temp_col+"_mkt"] *0.5
    
    ### Ana:合并后个股权重大于0.3%的有105个，对应9个行业，可以只选前5个行业
    list_ind1_top5 = df_weight["ind1_code"].drop_duplicates().values[:num_ind1]
    list_ind1_drop = df_weight["ind1_code"].drop_duplicates().values[num_ind1:] 

    list_index_drop = df_weight[ df_weight["ind1_code"].isin( list_ind1_drop )  ].index
    df_weight.loc[list_index_drop , temp_col  ] = 0.0 
    df_weight[ temp_col ] = df_weight[temp_col]/df_weight[temp_col].sum()
    ### 降序排列，以20140531为例，用roe指标这时选出来有216只股票
    df_weight = df_weight.sort_values(by=temp_col, ascending=False) 
    ### 选择前100名的股票
    list_index_drop = df_weight.index[num_stock:]
    df_weight.loc[list_index_drop , temp_col  ] = 0.0 
    df_weight[ temp_col ] = df_weight[temp_col]/df_weight[temp_col].sum()
    

    return df_weight 

#################################################################################
### 1，基础财务数据：直接打分方式
cols_financial_indicator =["west_avgroe_FY2","west_netprofit_CAGR","longcapitaltoinvestment","west_avgoperatingprofit_CAGR","turnover_ttm"]

#################################################################################
### 2，3个ABM核心指标及变动：9个直接打分
cols_abm_3indicator = ["profit_q4_es","revenue_q4_es","cf_oper_q4_es","profit_q4_es_dif","revenue_q4_es_dif","cf_oper_q4_es_dif","profit_q4_es_dif_pct","revenue_q4_es_dif_pct","cf_oper_q4_es_dif_pct"]

#################################################################################
### 3，相对打分1：找出价值靠近价值锚、成长靠近成长锚的 | 
# "code_anchor_value",
cols_anchor_indicator_value = ["profit_anchor_value","profit_dif_anchor_value","revenue_anchor_value","cf_oper_anchor_value"]
# "code_anchor_growth",
cols_anchor_indicator_growth = ["profit_anchor_growth","profit_dif_anchor_growth","revenue_anchor_growth","cf_oper_anchor_growth"]

#################################################################################
### 4，相对打分2：判断行业整体是否向好；行业内的集中度-越高越利于大公司，越低越利于小公司
cols_ind1_indicator = ["ind1_pct_profit_q4_es","ind1_pct_revenue_q4_es","ind1_pct_cf_oper_q4_es","w_allo_value_ind1","w_allo_growth_ind1"]



#################################################################################
### 按日期导入ABM模型数据
date_list = ["20140531","20141130","20150531","20151130","20160531","20161130"]
date_list = date_list + ["20170531","20171130","20180531"]
date_list = date_list + ["20181130","20190531","20191130"]

print(   date_list ) 

################################################################################
### CHOICE 1 :对indicator list 分别计算df_score
## temp_col = cols_financial_indicator[0]

# ### 设置组合参数
# para_dict= {}
# para_dict["para_indnumber"] = 5 # 默认是5个行业
# para_dict["para_stocknumber"] = 30  # 默认是100只股票
# #3 types: cols_financial_indicator  | cols_abm_3indicator | cols_ind1_indicator
# para_dict["indi_list"] = "ind1_indicator" # 根据col list 名称调整 
# list_indicator = cols_ind1_indicator

# para_dict["dir_output"] = "para_" +para_dict["indi_list"] +"_" + str(para_dict["para_indnumber"])+"_"+ str(para_dict["para_stocknumber"] ) 
# path_output = "D:\\CISS_db\\csi800_industrial_rotation\\" +para_dict["dir_output"] + "\\"

# if not os.path.exists( path_output ):
#     os.makedirs( path_output )

# print("Chosen indicator list : ", list_indicator )

# df_port_return = pd.DataFrame(index=date_list,columns=list_indicator )

# for temp_col in list_indicator  :
#     ### 对每个指标新建历史打分df和权重配置df , if_1st = 0 
#     if_1st = 0 

#     for temp_date in date_list :
        
#         # temp_date = "weights_"+"20140531" + "_1.csv"
#         file_date = "weights_"+temp_date + "_1.csv"

#         df_abm = pd.read_csv( path_abm + file_date ) 
#         df_abm_des = df_abm.describe() 
#         # index of df_abm_des | Index(['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max'], dtype='object')

#         print( df_abm_des )
#         #################################################################################
#         ### 0，基础信息：代码、1、2级行业分类  股票代码	一级行业代码	二级行业代码
#         cols_basic = [ "code","ind1_code","ind2_code"]
#         # 新建df_score,生成
#         df_score = df_abm.loc[:,cols_basic]
        
#         print( df_score.head() ) 
#         df_weight = df_abm.loc[:,cols_basic] 

#         df_score = get_score_mkt(df_abm ,df_score,temp_col )
#         df_score = get_score_ind1(df_abm ,df_score,temp_col )

#         df_weight = get_weight( df_weight,df_score,temp_col,list_ind1 ,para_dict  )

#         print("df_score ")
#         print( df_score.head() )
#         print("df_weight ")
#         print( df_weight.head() )

#         ### 对每个指标新建历史打分df和权重配置df
#         if if_1st == 0 :
#             temp_df = df_score
#             temp_df["date"] = temp_date
#             df_score_hist = temp_df

#             temp_df = df_weight
#             temp_df["date"] = temp_date
#             df_weight_hist = temp_df 
        
#         else :
#             temp_df= df_score
#             temp_df["date"] = temp_date
#             df_score_hist = df_score_hist.append( temp_df,ignore_index=True )

#             temp_df= df_weight
#             temp_df["date"] = temp_date
#             df_weight_hist = df_weight_hist.append( temp_df,ignore_index=True )
        
#         if_1st = if_1st +1 

       
#         df_score_hist.to_csv(path_output +"score_hist_" + temp_col +".csv")
#         df_weight_hist.to_csv(path_output +"weight_hist_"+ temp_col +".csv")     
        
#         ##############################################################################
#         ### 计算半年度收益 TODO TODO 
#         ### 1,把持仓对应的收益和历史收益相乘后相加
#         ### temp_date；str || type( df_return["date"].values[-1] ) , <class 'numpy.int64'>
#         # print("temp_date", type(temp_date), type( df_return["date"].values[-1] ) )

#         # step1，找到对应日期且对应代码的列
          
#         df_return2 = df_return[  df_return["date"] == int(temp_date) ]        

#         df_return2 = df_return2[  df_return2["code"].isin( list(temp_df["code"]) ) ]
#         # 把权重df和收益率df按代码排序
        
#         df_return2 =df_return2.sort_values(by="code").reset_index() 
#         temp_df =temp_df.sort_values(by="code").reset_index()
        
#         temp_sum = 0.0
#         for temp_i in df_return2.index :            
#             ret_attribution = float(df_return2.loc[temp_i,"return"])*temp_df.loc[temp_i, temp_col ] 
#             temp_sum =temp_sum + ret_attribution 
        
#         df_port_return.loc[temp_date,temp_col] = temp_sum 

#         print("Tail ", df_port_return.tail() )
        
#         ###
#         print(time_0 )
#         print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
#         # input1 = input("Check to continue")

#     ##############################################################################    
#     ### 对于单个指标的权重文件，剔除权重低于0.1%的股票，并按wind-PMS模板的格式保存
#     df_pms= df_weight_hist.loc[:,["code",temp_col,"date"] ]
#     df_pms = df_pms[ df_pms[temp_col]>=0.001  ]
#     df_pms[temp_col] =df_pms[temp_col]*100
#     df_pms.columns = ["证券代码","持仓权重","调整日期"]
#     df_pms["成本价格"]= ""
#     df_pms["证券类型"] = "股票"
#     df_pms.to_csv(path_output + "PMS_hist_" + temp_col +".csv",encoding="gbk",index=None)     

#     ### 2，一个个导入PMS速度太慢，更合适的方式可能是对持仓数据每八年统一拉一个收益率数据，然后匹配计算区间收益
#     # file return_data_0531_1130.csv
#     #     code	date	date2	return 
#     # 600683.SH	20140531	20141130	60.8%
#     # 600816.SH	20140531	20141130	102.3%
#     # 000848.SZ	20140531	20141130	24.8%

# ### Save return matrix to csv file 
# file_name_port_return = path_output + "0port_return_" +str(para_dict["para_indnumber"])+"_"+ para_dict["indi_list"]+".csv"

# df_port_return.to_csv( file_name_port_return,encoding="gbk",index=None)     

# asd

##############################################################################    
### CHOICE2:将所有指标打分加总 ,temp_col = "sum",所有的indicator分数和sum保存在 df_score_sum 里了
'''
todo:其实这里还有很多工作要做，还不如统一放到之前的环节里一起计算了。
### 但独立也有独立的好处，比如之后可以按照设计的不同加权方式计算
### TODO 已经完成对单一指标的权重计算，下一步是对所有指标计算最终的100只股票权重，并保存到csv文件
争取1206周六能把初步的4种不同的组合算出来。
'''
cols_basic2 = [ "code","ind1_code","ind2_code","code_date"]
temp_col = "sum"

### 导入各个指标score计算结果，计算总分并计算权重
### Method 1:Take simply given indicator list 
# # cols_financial_indicator |  cols_ind1_indicator | cols_abm_3indicator
# list_indicator= cols_abm_3indicator
# str_output = "cols_abm_3indicator"
# list_indicator = cols_ind1_indicator

### Method 2:Define ideally indicators
list_indicator= ["revenue_q4_es","revenue_q4_es_dif","west_netprofit_CAGR"]
list_indicator=list_indicator+["profit_q4_es_dif_pct","cf_oper_q4_es","w_allo_value_ind1"]
list_indicator=list_indicator+["ind1_pct_revenue_q4_es","profit_q4_es","w_allo_growth_ind1"]
list_indicator=list_indicator+["ind1_pct_revenue_q4_es","profit_q4_es","west_avgroe_FY2"]
# 反思：20190530以来收益不佳，但west_avgroe_FY2指标却比较好，代替w_allo_growth_ind1指标
str_output = "para_5_30_top9adj_indicator"

#############################################
### 设置组合参数
para_dict= {}
para_dict["para_indnumber"] = 5 # 默认是5个行业
para_dict["para_stocknumber"] = 30  # 默认是100只股票
#3 types: cols_financial_indicator  | cols_abm_3indicator | cols_ind1_indicator
para_dict["indi_list"] = str_output # 根据col list 名称调整 

para_dict["dir_output"] = "para_" +para_dict["indi_list"] +"_" + str(para_dict["para_indnumber"])+"_"+ str(para_dict["para_stocknumber"] ) 
path_output = "D:\\CISS_db\\csi800_industrial_rotation\\" +para_dict["dir_output"] + "\\"

if not os.path.exists( path_output ):
    os.makedirs( path_output )

print("Chosen indicator list : ", list_indicator )

df_port_return_sum = pd.DataFrame(index=date_list,columns=["sum"] )
count_indicator = 0

###################################################
### TODO 分不同时期,分别导入score计算权重,需要分时期计算权重文件，并且temp_col="sum"
# for temp_col in cols_financial_indicator :
for temp_col in list_indicator  :
    ### Import score file as df 
    ### notes：这里可能会报错，需要把各个文件夹里的score_ 文件复制到当前文件夹内。
    print("notes：这里可能会报错，需要把各个文件夹里的score_ 文件复制到当前文件夹内。")
    print( path_output )
    df_score  =pd.read_csv( path_output +"score_hist_" + temp_col +".csv")
    df_score  =df_score.drop("Unnamed: 0",axis=1 )
    df_score["code_date"] = df_score["code"].str.cat( df_score["date"].astype("str"),sep="_" )

    if count_indicator == 0 :        
        df_score_sum = df_score 
        # case "code_date" :603980.SH20180531,603996.SH20180531
        ### add indicator score to score_sum
        df_score_sum["sum_mkt"] = df_score[ temp_col+"_mkt" ] 
        df_score_sum["sum_ind1"] = df_score[ temp_col+"_ind1" ] 
    else :
        ### notes:df_score_sum and df_score might have different order in code*date
        ### append df_score to df_score_sum
        # df_score中指标有2列，分别是indi_mkt,indi_ind1
        ### 对两df分别按"code","date" 排序，
        # drop columns name="index"
        df_score_sum = df_score_sum.sort_values(by="code_date").reset_index(drop=True)  
        df_score = df_score.sort_values(by="code_date").reset_index(drop=True)
        
        ### add indicator score to score_sum
        df_score_sum["sum_mkt"] = df_score_sum["sum_mkt"] +df_score[ temp_col+"_mkt" ] 
        df_score_sum["sum_ind1"]= df_score_sum["sum_ind1"]+df_score[ temp_col+"_ind1" ] 
        df_score_sum[temp_col+"_mkt"] = df_score[ temp_col+"_mkt" ] 
        df_score_sum[temp_col+"_ind1"] = df_score[ temp_col+"_ind1" ] 
    
    df_score_sum.to_csv( path_output +"score_sum_"+str_output  +"_.csv")
    count_indicator =count_indicator +1
    
### 对于每一期的总分，计算当期权重，并append到总df_weight_sum 理
count_date = 0
temp_col= "sum"
for temp_date in date_list :
    
    ### calculate temp weight         
    temp_score = df_score_sum[ df_score_sum["date"]== int(temp_date) ]
    
    temp_weight = temp_score.loc[:,cols_basic2 ]
    temp_weight = get_weight( temp_weight,temp_score,temp_col,list_ind1 ,para_dict  )
    if count_date == 0 :
        df_weight_sum = temp_weight
    else :
        df_weight_sum = df_weight_sum.append( temp_weight)
        
    ### append temp_weight_sum to df_weight_sum
    
    df_weight_sum.to_csv( path_output +"weight_sum_" +str_output +"_.csv")

    #############################################################################
    ### 计算半年度收益 TODO TODO 
    ### 1,把持仓对应的收益和历史收益相乘后相加
    # step1，找到对应日期且对应代码的列
    
    df_return2 = df_return[  df_return["date"] == int(temp_date) ]
    df_return2 = df_return2[  df_return2["code"].isin( list( temp_weight["code"]) ) ]
    # 把权重df和收益率df按代码排序
    
    df_return2 =df_return2.sort_values(by="code").reset_index(drop=True) 
    temp_weight =temp_weight.sort_values(by="code").reset_index(drop=True) 

    temp_sum = 0.0
    for temp_i in df_return2.index :            
        ret_attribution = float(df_return2.loc[temp_i,"return"])*temp_weight.loc[temp_i, temp_col ] 
        temp_sum =temp_sum + ret_attribution 

    print("Debug=============")
    print( "df_return" )
    print( df_return.head()  )
    print("temp_weight   ")
    print( temp_weight.head() )


    df_port_return_sum.loc[temp_date,temp_col] = temp_sum 

    file_name_port_return = path_output + "0port_return_sum_"+ str_output  +".csv"

    df_port_return_sum.to_csv(path_output + "0port_return_sum_"+ str_output  +".csv",encoding="gbk") 

    print("Tail ", df_port_return_sum.tail() )

    # input1= input("Check to continue")
    count_date =count_date +1
    
    #############################################################################    
    ### 对于单个指标的权重文件，剔除权重低于0.1%的股票，并按wind-PMS模板的格式保存
    # notes:这部分没验证过
    df_pms= df_weight_sum.loc[:,["code",temp_col,"date"] ]
    df_pms = df_pms[ df_pms[temp_col]>=0.001  ]
    df_pms[temp_col] =df_pms[temp_col]*100
    df_pms.columns = ["证券代码","持仓权重","调整日期"]
    df_pms["成本价格"]= ""
    df_pms["证券类型"] = "股票"
    df_pms.to_csv(path_output + "PMS_hist_" + temp_col +".csv",encoding="gbk",index=None)     

    ### 2，一个个导入PMS速度太慢，更合适的方式可能是对持仓数据每八年统一拉一个收益率数据，然后匹配计算区间收益
    # file return_data_0531_1130.csv
    #     code	date	date2	return 
    # 600683.SH	20140531	20141130	60.8%
    # 600816.SH	20140531	20141130	102.3%
    # 000848.SZ	20140531	20141130	24.8%

#################################################################################
### 分析：
'''
1,采用传统的5个指标，可以看到在2017年之前指标总体跑赢沪深300，但2017年之后持续下降；

2，一个个导入PMS速度太慢，更合适的方式可能是对持仓数据每八年统一拉一个收益率数据，然后匹配计算区间收益


'''










#################################################################################
### Transform to CN 
cols_basic_CN = [ "股票代码","一级行业代码","二级行业代码"]

cols_financial_indicator_CN =["一致预测ROE(FY2)","一致预测净利润2年复合增长率","长期股权投资","一致预测营业利润2年复合增长率","总资产周转率(TTM)"]

cols_abm_3indicator_CN = ["当年净利润预测","当年收入预测","当年经营性现金流预测","当年净利润增长值","当年收入增长值","当年经营性现金流增长值","当年净利润增长率","当年收入增长率","当年经营性现金流增长率"]

cols_anchor_indicator_value_CN = ["价值锚个股代码","价值锚个股利润","价值锚个股利润变动","价值锚个股收入","价值锚个股经营性现金流"]

cols_anchor_indicator_growth_CN = ["成长锚个股代码","成长锚净利润","成长锚净利润变动","成长锚收入","成长锚经营性现金流"]

cols_ind1_indicator_CN = ["一级行业净利润增长率","一级行业收入增长率","一级行业经营性现金流增长率","一级行业内价值锚权重","一级行业内成长锚权重"]
















#################################################################################
### 