# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"
'''

function ：
1，计算逐年指数成分股，根据历史调入调出记录。
2，根据输入信息和指标，计算指数成分
last 200320| since 20200313
'''
##################################################################################
### 2，根据输入信息和指标，计算指数成分
### 根据输入csv，计算打分和权重
'''
每半年，在中证800成分股内选择海外业务收入前50%的股票，
input:
0,date,20190531
0,code,
1，r_foreign,海外业务收入
2,roe_pre1,上一年roe
3,roe_ttm,当年roe，根据最近1个季度roe
4,roe_pre2,上二年roe
5,roe_pre3,上三年roe
6,roic_pre1,上一年roic
4,roic_ttm,当年roic，根据最近1个季度roic
6,roic_pre2,上二年roic

middle
1，roe_g:roe增长
2，roe_std:roe波动率
3, roic_g:roic增长

对6个指标进行标准化打分

算法文件：cssw_ydyl_20200316_cgs.xlsx\\sheet=3CSI800筛选roe&roic
'''
import pandas as pd 
import numpy as np

path_0 = "C:\\rc_reports_cs\\指数研究\\长盛中证申万一带一路\\"
path_out = path_0 + "py_cal_output\\"
path_in = path_0 + "py_cal_input\\"

file_in = "ydyl_data_raw.csv"

df_raw = pd.read_csv( path_in + file_in  )
print("df_raw", df_raw.head() )

date_list = list( df_raw["date"].drop_duplicates()) 
print("date_list ", date_list )

indi_list=["roe_ttm","roe_g","roe_std","roic_ttm","roic_g" ,"roic_std" ]

count_date = 0 
for temp_date in date_list :
    df_raw_sub = df_raw [ df_raw["date"] == temp_date ]
    
    #################################################################################
    ### 计算 1，roe_g:roe增长；2，roe_std:roe波动率；3, roic_g:roic增长
    df_raw_sub.loc[:,"roe_g"] = df_raw_sub.loc[:,"roe_ttm"] - df_raw_sub.loc[:,"roe_pre1"]
    df_raw_sub.loc[:,"roic_g"] = df_raw_sub.loc[:,"roic_ttm"] - df_raw_sub.loc[:,"roic_pre1"]
    ### 前3年波动率
    for temp_i in df_raw_sub.index :
        x1 = df_raw_sub.loc[temp_i,"roe_pre1"]
        x2 = df_raw_sub.loc[temp_i,"roe_pre2"]
        x3 = df_raw_sub.loc[temp_i,"roe_pre3"]
        df_raw_sub.loc[temp_i,"roe_std"] = 1/np.std([x1,x2,x3])

        y1 = df_raw_sub.loc[temp_i,"roe_pre1"]
        y2 = df_raw_sub.loc[temp_i,"roe_pre2"]
        y3 = df_raw_sub.loc[temp_i,"roe_ttm"]
        df_raw_sub.loc[temp_i,"roic_std"] = 1/np.std([y1,y2,y3]) 
    #################################################################################
    ### 对6个指标计算标准分值
    df_raw_sub["score_sum"] = 0 

    for temp_indi in indi_list :
        temp_median = df_raw_sub[temp_indi].median()
        temp_std = df_raw_sub[temp_indi].median()
        temp_max = min(temp_median+3*temp_std, df_raw_sub[temp_indi].max()  )
        temp_min = max(0, max(temp_median-3*temp_std, df_raw_sub[temp_indi].min()) )
        # print("max min",temp_max,temp_min)
        col_name = "score_" + temp_indi
        for temp_j in df_raw_sub.index :
            temp_score =(df_raw_sub.loc[temp_j, temp_indi] - temp_min)/(temp_max-temp_min)
            df_raw_sub.loc[temp_j,col_name] = min(max(temp_score,-0.1),1.1)
        
        #################################################################################
        ### 对6个指标计算总分
        df_raw_sub["score_sum"] = df_raw_sub["score_sum"] + df_raw_sub[ col_name ]
    
    #################################################################################
    ### 6个指标总分取前200名，计算权重,越往tail越大
    df_raw_sub =df_raw_sub.sort_values(by="score_sum",ascending="True") 

    df_score200 = df_raw_sub.tail(200)
    print("score200 Head \n",df_score200.head().T )
    print("Tail \n",df_score200.tail().T )
    #################################################################################
    ### 计算权重
    df_score200["weight"] = df_score200["score_sum"]*0.95/df_score200["score_sum"].sum()
    
    #################################################################################
    ### 计算PMS模板
    '''代码	权重	价格	日期	类型
    603160.SH	0.84%		2019/5/31	股票'''
    df_score200["代码"] = df_score200["code"]
    df_score200["权重"] = df_score200["weight"]*100

    df_score200["日期"] = df_score200["date"]
    
    df_PMS_sub = df_score200.loc[:,["代码","权重","日期"]  ]
    df_PMS_sub["类型"] = "股票"
    df_PMS_sub["价格"] = ""
    
    if count_date == 0 :
        df_PMS = df_PMS_sub
        df_score200_sum = df_score200
        df_full = df_raw_sub
    else :
        df_PMS = df_PMS.append( df_PMS_sub )
        df_score200_sum = df_score200_sum.append( df_score200 )
        df_full = df_full.append(df_raw_sub)

    #################################################################################
    ### 统计分析：基于GICS一级行业和中信一级行业
    '''1，逐个日期，计算一级行业分类入选股票数量，
    
    '''


    ### save to csv 
    df_PMS.to_csv(path_out + "df_PMS_0321.csv",index=False,encoding="gbk")  
    df_score200_sum.to_csv(path_out +"df_score200_sum_0321.csv" ,encoding="gbk")  
    df_full.to_csv(path_out +"df_full_0321.csv" ,encoding="gbk")    
    
    count_date += 1   
    
    
asd




    





















asd


 

##################################################################################
### 计算code_list行业分类


asd

##################################################################################
### 1，计算逐年指数成分股，根据历史调入调出记录。
### function 计算逐年指数成分股，根据历史调入调出记录。
file_path= "C:\\zd_zxjtzq\\rc_reports_cs\\指数研究\\美股指数研究\\"
str_name = "nasdaq100" 
# str_name = "sp500"
file_name="temp_"+ str_name +".csv" # NASDAQ100 or sp500

import pandas as pd 

temp_df = pd.read_csv(file_path+file_name,encoding='gbk')
date_list = temp_df["date"].drop_duplicates().values

### 初始化输出表格
df_ind_output = pd.DataFrame( columns=date_list )
df_top20_name_output = pd.DataFrame( columns=date_list )
df_top20_weight_output = pd.DataFrame( columns=date_list )

count = 0 
for temp_year in date_list:
    df_sub = temp_df[ temp_df["date"] == temp_year ]
    
    df_sub_ind = df_sub.groupby("gics_code").sum() 
    
    ### sum of ind level2 and top 20 stocks
    if count == 0  :
        df_sub_ind = df_sub_ind.reset_index()
        df_sub_ind["date"] = temp_year
        df_sub_ind_all = df_sub_ind
        
        df_sub_top20 =  df_sub.sort_values(by="weight",ascending=False).head(20)
        df_sub_top20 = df_sub_top20.reset_index()
        df_sub_top20_all =df_sub_top20 

        ### 需要按每个年度标注top20股票和行业权重 
        for temp_i in  df_sub_ind.index :
            gics_code = df_sub_ind.loc[temp_i, "gics_code"] 
            temp_weight = df_sub_ind.loc[temp_i, "weight"] 
            df_ind_output.loc[gics_code,temp_year] = temp_weight

        for temp_j in df_sub_top20.index : 
            temp_name = df_sub_top20.loc[temp_j, "name"] 
            temp_weight = df_sub_top20.loc[temp_j,  "weight"] 
            df_top20_name_output.loc[temp_j, temp_year] = temp_name
            df_top20_weight_output.loc[temp_j, temp_year] = temp_weight

        count = 1

    else :
        df_sub_ind = df_sub_ind.reset_index()
        df_sub_ind["date"] = temp_year
        df_sub_ind_all = df_sub_ind_all.append(df_sub_ind ,ignore_index=True )
        
        df_sub_top20 =  df_sub.sort_values(by="weight",ascending=False).head(20)
        df_sub_top20 = df_sub_top20.reset_index()
        df_sub_top20_all =df_sub_top20_all.append(df_sub_top20 ,ignore_index=True )

        ### 需要按每个年度标注top20股票和行业权重 
        for temp_i in  df_sub_ind.index :
            gics_code = df_sub_ind.loc[temp_i, "gics_code"] 
            temp_weight = df_sub_ind.loc[temp_i, "weight"] 
            df_ind_output.loc[gics_code,temp_year] = temp_weight

        for temp_j in df_sub_top20.index : 
            temp_name = df_sub_top20.loc[temp_j, "name"] 
            temp_weight = df_sub_top20.loc[temp_j,  "weight"] 
            df_top20_name_output.loc[temp_j, temp_year] = temp_name
            df_top20_weight_output.loc[temp_j, temp_year] = temp_weight

    #### debug
    print("df_sub_top20_all ",df_sub_top20_all )
    print(" df_sub_ind_all", df_sub_ind_all )
    


    df_sub_top20.to_csv( file_path+str_name+"\\" +"output_" + str_name +"_top20_"+str(temp_year)+".csv",encoding="gbk" )
    df_sub_ind.to_csv( file_path +str_name+"\\" +"output_" + str_name +"_ind_"+str(temp_year)+".csv",encoding="gbk" )

    df_sub_top20_all.to_csv( file_path +str_name+"\\" +"output_" + str_name +"_top20.csv",encoding="gbk" )
    df_sub_ind_all.to_csv( file_path +str_name+"\\" +"output_" + str_name +"_ind.csv",encoding="gbk" )

    ### 需要按每个年度标注top20股票和行业权重 
    df_ind_output.to_csv( file_path +str_name+"\\" +"output_" + str_name +"_df_ind_output.csv",encoding="gbk" )
    df_top20_name_output.to_csv(   file_path +str_name+"\\" +"output_" + str_name +"_df_top20_name_output.csv",encoding="gbk" )
    df_top20_weight_output.to_csv( file_path +str_name+"\\" +"output_" + str_name +"_df_top20_weight_output.csv",encoding="gbk" )







