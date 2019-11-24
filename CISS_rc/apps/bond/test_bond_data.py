# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：对中债指数数据进行分类匹配
last update | since 190118
Menu : 

total_return 财富
net_price  净价
full_price 全价
 
Notes:

===============================================
'''
import json
import pandas as pd 
import sys
sys.path.append("..") 

######################################################################
### 
path0= "D:\\CISS_db\\bond_pct\\"
file_name= "bond_index_190118.csv"

temp_df = pd.read_csv( path0+file_name ,encoding="gbk")

### for every  
df_full_price = temp_df[ temp_df["name"].str.contains("全价") ]
col_durations =["total_value","yrs_0_1","yrs_1_3","yrs_3_5","yrs_5_7","yrs_7_10","yrs_10_more"]

df_out = pd.DataFrame(columns=["class1","class2","class_yrs","name" ],index=[0])
i = 0 
for temp_i in df_full_price.index :

    # get Chinese name of full price from name of net price 
    temp_name = df_full_price.loc[temp_i,"name"].replace("全价","净价")
    # find net price name in raw df 
    find_df = temp_df[ temp_df["name"]==temp_name ]

    temp_name2 = df_full_price.loc[temp_i,"name"].replace("全价","财富")
    find_df2 = temp_df[ temp_df["name"]==temp_name2 ]    

    for temp_col in col_durations :
        # find all symbols for different durations
        df_out.loc[i,"class1"] = df_full_price.loc[temp_i,"class1"]
        df_out.loc[i,"class2"] = df_full_price.loc[temp_i,"class2"]
        df_out.loc[i,"class_yrs"] = temp_col
        df_out.loc[i,"name"] = df_full_price.loc[temp_i,"name"] 
        df_out.loc[i,"symbol_full_price"] = df_full_price.loc[temp_i, temp_col ]

        # find correlated net_price ,"净价"
        df_out.loc[i,"symbol_net_price"] = find_df.loc[find_df.index[0], temp_col ]

        df_out.loc[i,"symbol_total_return"] = find_df2.loc[find_df2.index[0], temp_col ]

        i+=1 

df_out = df_out.dropna( axis=0 )
df_out.to_csv("D:\\df_out_190118.csv",encoding="gbk")





















