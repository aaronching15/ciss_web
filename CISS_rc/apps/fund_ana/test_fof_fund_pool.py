# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

''' 
################################################
todo： 
1,基金池自动分析，生成对应基金池表格——【顺便把业绩监控做出来；例如分析过去1周or月基金排名上升最快地基金及持仓股票】
2,指数基金指标计算：indi_list = ["fund_trackerror_threshold","risk_navoverbenchannualreturn","risk_avgtrackdeviation_trackindex","risk_avgtrackdeviation_benchmark","risk_trackerror","risk_annuinforatio","risk_timeranking","risk_stockranking","risk_inforatioranking"]
3，


last 211104 | since 211101
derived from 基金池rc-主动股票-21Q3.xlsx ;path=C:\rc_HUARONG\rc_HUARONG\0基金池和模拟组合
derived from wind-公募基金-主动股票型-YTD-211101.xlsx ;path=C:\rc_HUARONG\rc_HUARONG\Data_数据\0基金数据-Wind
数据：wind终端——基金索引


################################################
功能：一、基金季度股票持仓变动
1，机构维度：易方达、广发，持股变动；2，基金经理维度：新能源类型、小市值、成长、价值等基金经理。
数据：file_21Q3=重仓持股(明细)-主动股票-2021Q3.xlsx ;file_21H1=全部持股(明细)-主动股票-2021H1.xlsx ;path=C:\rc_HUARONG\rc_HUARONG\Data_数据\0基金数据-Wind
二、基金池自动分析，变成各个表格——【顺便把业绩监控做出来；例如分析过去1周or月基金排名上升最快地基金及持仓股票】
Py-file = test_fof_fund_pool.py
将同名sheet从xlsx文件1存入xlsx文件
vlookup函数无法自动去获取跨文件引用地址，手动输入格式如下：[文件名（带扩展名)]工作表名!单元区域。而这个例子中我们可以写成：[data.xlsx]sheet1!A2:B8
数据：file_21Q3=重仓持股(明细)-主动股票-2021Q3.xlsx ;file_21H1=全部持股(明细)-主动股票-2021H1.xlsx ;path=C:\rc_HUARONG\rc_HUARONG\Data_数据\0基金数据-Wind
    
################################################
数据分析步骤： 
 
    
notes:
1, "换手率（%）" 在季报期可能没有数值； "重仓ROE（%）"wind说暂时不提供
2， 
################################################
'''

#################################################################################
### Initialization，load configuration 
# Notes: 除了基本的路径和时间，其他脚本导入尽量都放在 config_XXX 里，保存再 obj 对象 
import sys,os
from attr import asdict
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db；# C:\ciss_web\CISS_rc\config
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )

import pandas as pd 
import numpy as np 
import math
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
temp_date = dt.datetime.now().strftime('%Y%m%d')

##########################################################################################
### Part 1 管理PMS组合列表；划分不同资产
file= "pms_manage.xlsx"
path0 = "C:\\rc_HUARONG\\rc_HUARONG\\"
path = "C:\\rc_HUARONG\\rc_HUARONG\\data_pms\\"

path_wpf = path + "wpf\\" 
path_data = path + "wind_terminal\\" 
path_data_fund = path_data + "fund\\" 
path_adj = path +  "data_adj\\"

########################################
### 参数设置 | notes:"超额收益"的计算都是基于沪深300和一年期定存，指数基金的基准需要单独计算
### dict_w 是各项基金分析各项指标的权重
fundpool_type = input("输入基金类型：1-主动股票、2-股票指数、3-债券、4-货币、5-股票FOF、6-债券FOF： ") 
if fundpool_type == "1" :
    ### 主动股票1
    dict_w ={"收益率": 0.3,"最大回撤" :0.25,"投资风格":0.00, "超额收益" :0.25,"投研团队实力": 0.15,"交易便利": 0.05} 
    file_output =  "基金池rc_主动股票_" + temp_date + ".xlsx"
    file_name = "wind-公募基金-主动股票-"+ temp_date + ".xlsx"
elif fundpool_type == "2" :
    ### 股票指数2
    dict_w ={"收益率": 0.05,"最大回撤" :0.05,"投资风格":0.00, "超额收益" :0.30,"投研团队实力": 0.25,"交易便利": 0.35} 
    file_output =  "基金池rc_股票指数_" + temp_date + ".xlsx"
    file_name = "wind-公募基金-股票指数-"+ temp_date + ".xlsx"

elif fundpool_type == "3" :
    ### 债券3
    dict_w ={"收益率": 0.3,"最大回撤" :0.25,"投资风格":0.00, "超额收益" :0.25,"投研团队实力": 0.1,"交易便利": 0.1} 
    file_output =  "基金池rc_债券_" + temp_date + ".xlsx"
    file_name = "wind-公募基金-债券-"+ temp_date + ".xlsx" 
    
elif fundpool_type == "4" :
    ### 货币3 |notes:货币基金没有最大回撤
    dict_w ={"收益率": 0.3,"最大回撤" :0.00,"投资风格":0.00, "超额收益" :0.4,"投研团队实力": 0.1,"交易便利": 0.2} 
    file_output =  "基金池rc_货币_" + temp_date + ".xlsx"
    file_name = "wind-公募基金-货币-"+ temp_date + ".xlsx" 
    # wind-公募基金-货币-211106.xlsx

elif fundpool_type == "5" :
    ### FOF 5；股票FOF：侧重于收益
    dict_w ={"收益率": 0.3,"最大回撤" :0.25,"投资风格":0.00, "超额收益" :0.25,"投研团队实力": 0.1,"交易便利": 0.1} 
    file_output =  "基金池rc_FOF_" + temp_date + ".xlsx"
    file_name = "wind-公募基金-FOF-"+ temp_date + ".xlsx" 
elif fundpool_type == "6" :
    ### FOF 6-债券FOF：侧重于回撤
    dict_w ={"收益率": 0.2,"最大回撤" :0.4,"投资风格":0.00, "超额收益" :0.2,"投研团队实力": 0.1,"交易便利": 0.1} 
    file_output =  "基金池rc_FOF_" + temp_date + ".xlsx"
    file_name = "wind-公募基金-FOF-"+ temp_date + ".xlsx" 


################################################################################
### Part 0, 导入基金配置文件，初始化基金对象
### 1，时间：对于更新时间t，确定对应季末时间T和上一季度末时间T-1
from config_fund import config_fund
config_fund_1 = config_fund()
from config_data import config_data
config_data_1 = config_data()

################################################################################
### Part 1，数据整理
################################################################################
### Step 0，数据获取——基金,个股：... 
### 数据和分析指标的计算、IO
'''# 基金池筛选模板
基金池rc-主动股票-21Q3.xlsx ;path=C:\rc_HUARONG\rc_HUARONG\0基金池和模拟组合
wind-公募基金-主动股票型-YTD-211101.xlsx ;path=C:\rc_HUARONG\rc_HUARONG\Data_数据\0基金数据-Wind
'''
# Wind终端基金导出数据
obj_fund_data = {}
obj_fund_data["dcit"] = {}
### 导出数据
# path_output = "C:\\rc_HUARONG\\rc_HUARONG\\0基金池和模拟组合\\"
path_output = path_adj


file_output_in = input("Output filename such as: 基金池rc_主动股票_220104.xlsx:\n ")
if len(file_output_in ) >20 :
    file_output =  file_output_in
    # file_output =  "基金池rc_主动股票_210726_211104.xlsx"
else :
    asd

# sheet_name=None，读入所有sheet工作表;1,读入第一个表；不填则默认读取第一个表
# path_file = "C:\\rc_HUARONG\\rc_HUARONG\\Data_数据\\0基金数据-Wind\\" 

# file_name= "wind-公募基金-主动股票型-YTD-211101.xlsx"
file_name_in = input("Input filename such as:公募基金-股票FOF-20220128.xlsx or wind-公募基金-主动股票-YTD-220104.xlsx:\n" )
if len(file_name_in ) >20  :
    file_name = file_name_in
    # file_name = "wind-公募基金-普通股票型-+混合-YTD-210726.xlsx"
else :
    asd 

dict_fund_data = pd.read_excel( path_data_fund + file_name,sheet_name=None)
# dict_fund_data.keys() = ['概况', '业绩表现', '风险收益', '资产配置', '投资风格', '获奖情况', '交易信息']
# notes: 部分表格的第一列是不完全的，部分基金新成立没有数据，需要剔除非 float 的str值"--"
# df_temp["规模(亿)"].replace("--",-1 ) 会返回修改后的值，但是不会改变df_temp
# 会报错： df_temp = df_temp[ df_temp["规模(亿)"]>0.6 ] ； '>' not supported between instances of 'str' and 'float'


################################################################################
### 部分列改名, todo
df_temp = dict_fund_data["概况"]

##############################################################################
### 剔除小规模基金 || 数值改造：异常值替代"--"、string，过小值等

df_drop = df_temp [ df_temp["规模(亿)"] == "--" ]
df_temp = df_temp.drop( df_drop.index , axis = 0   )
df_temp["规模(亿)"] = df_temp["规模(亿)"].astype(float) 
# 规模大于 3500万
df_temp = df_temp[ df_temp["规模(亿)"] > 0.35 ]  

df_drop = df_temp [ df_temp["收益率（%）"] == "--" ]
df_temp = df_temp.drop( df_drop.index , axis = 0   )
df_temp["收益率（%）"] = df_temp["收益率（%）"].astype(float) 
df_temp["成立年限"] = df_temp["成立年限"].astype(float) 
df_temp["年化收益率(%)(年初至今)"] = df_temp["年化收益率(%)(年初至今)"].astype(float)  
# 
df_fund = df_temp  

################################################################################
### 判断是否需要用wind-API抓取指数指标

########################################
### 定义fundction，用wind-API抓取指数指标
def get_index_track_value():
    import WindPy as wp 
    wp.w.start()
    ### 需要先获取基金对应的指数基准，再取同样基准品种抓取跟踪误差
    ### notes:不能直接获取所有基金的跟踪误差指标，那样所有基金会使用同一个默认的“000001.SH”指数作为基准，
    col_list = list( df_fund["基金代码"].values )
    obj_w1 = wp.w.wss( col_list[:3] , "fund_trackindexcode")
    col_list_bench = obj_w1.Data[0]
    # 构建一一对应的dict
    dict_code = dict( zip(col_list, col_list_bench   )  )
    
    ### Parameters
    str_start = input("输入区间开始日期，such as 20210101：")
    str_end = input("输入区间结束日期，such as 20211103：")
    indi_list = ["fund_trackerror_threshold","risk_navoverbenchannualreturn","risk_avgtrackdeviation_trackindex","risk_avgtrackdeviation_benchmark","risk_trackerror","risk_annuinforatio","risk_timeranking","risk_stockranking","risk_inforatioranking"]
    for temp_indi in indi_list :
        df_fund[temp_indi ] = "nan"
    
    str_indi_list = "fund_trackerror_threshold,risk_navoverbenchannualreturn,risk_avgtrackdeviation_trackindex,risk_avgtrackdeviation_benchmark,risk_trackerror,risk_annuinforatio,risk_timeranking,risk_stockranking,risk_inforatioranking"
    str1 = "startDate="+str_start +";" +"endDate="+str_end +";"+"returnType=1;period=1;index="
    ### 
    for temp_i in df_fund.index : 
        temp_code = df_fund.loc[temp_i, "基金代码" ] 
        # "startDate=20210101;endDate=20211103;returnType=1;period=1;index=000001.SH;riskFreeRate=1;fundType=1")
        str_all = str1 + dict_code[ temp_code ] +  ";riskFreeRate=1;fundType=1"
        print("str_all:", str_all ) 
        obj_w = wp.w.wss( temp_code ,str_indi_list , str_all)

        ### save value to df 
        for temp_j in range(0,len( indi_list ) ) :

            df_fund.loc[temp_i, indi_list[temp_j ] ] = obj_w.Data[ temp_j  ]

    return df_fund

'''
w.wss("160420.OF,165511.OF", "fund_trackindexcode,fund_trackerror_threshold,risk_navoverbenchannualreturn,risk_avgtrackdeviation_trackindex,risk_avgtrackdeviation_benchmark,risk_trackerror,risk_annuinforatio,risk_timeranking,risk_stockranking,risk_inforatioranking","startDate=20210101;endDate=20211103;returnType=1;period=1;index=000001.SH;riskFreeRate=1;fundType=1")
'''
# if fundpool_type == "2" :
#     index_track_value = input("是否用wind下载并保存指数跟踪误差等数据：1 ") 
#     if index_track_value == "1" :
#         index_track_value = get_index_track_value()



################################################################################
### Step 1，统计基金公司、基金经理规模|股票型基金  
### 计算规模加权收益率
df_temp["aum_ret"] = df_temp["规模(亿)" ] * df_temp["收益率（%）" ] 
df_comp0 = df_temp.groupby("管理人" )["规模(亿)","aum_ret" ].sum()
### 取平均值
df_comp = df_temp.groupby("管理人" )["收益率（%）","成立年限" ,"年化收益率(%)(年初至今)" ].mean()
# pd.merge(df1,df2)
df_comp["规模(亿)"] = df_comp0["规模(亿)"]
df_comp["规模加权收益率"] = df_comp0["aum_ret"] / df_comp0["规模(亿)"]

###########################################
### 基金经理汇总 
df_manager0 = df_temp.groupby("基金经理" )["规模(亿)","aum_ret" ].sum()
### 取平均值
df_manager = df_temp.groupby("基金经理" )["收益率（%）","成立年限" ,"年化收益率(%)(年初至今)" ].mean()
# pd.merge(df1,df2)
df_manager["规模(亿)"] = df_manager0["规模(亿)"]
df_manager["规模加权收益率"] = df_manager0["aum_ret"]/ df_manager0["规模(亿)"]


###########################################
### 对于每一只基金，将基金公司数据赋值 
# notes: rank() 数值最小的是1，最大的是2876
num_fund = len( df_fund.index  )
df_fund["基金公司规模"] = df_fund["管理人"].apply( lambda x : df_comp.loc[x ,"规模(亿)"  ]  )
df_fund["pct_基金公司规模"] =  df_fund[ "基金公司规模" ].rank()  / num_fund

## 业绩的几种计算方式：规模加权收益率、平均收益率、前25%分位收益率
df_fund["基金公司业绩"] = df_fund["管理人"].apply( lambda x : df_comp.loc[x ,"规模加权收益率"  ]  )
df_fund["pct_基金公司业绩"] =  df_fund[ "基金公司业绩" ].rank()  / num_fund

# df_fund["基金公司业绩"] = df_fund["管理人"].apply( lambda x : df_comp.loc[x ,"收益率（%）"  ]  )
# df_fund["pct_基金公司业绩"] = df_fund[ "基金公司业绩" ].rank()  / num_fund

#######################################
### 剔除C类基金重复项, 因为C类的规模需要加到基金公司规模中
df_fund["end_char"] = df_fund["基金名称"].str[-1]
df_fund["begin_char"] = df_fund["基金名称"].str[:-1]
df_fund_C = df_fund[ df_fund["end_char"] == "C" ]  
df_fund = df_fund.drop( df_fund_C.index , axis=0 )

### 将C类和A类对应起来
for temp_i in df_fund_C.index: 
    temp_name = df_fund_C.loc[temp_i, "begin_char" ]
    ### try to find 
    df_s = df_fund[ df_fund["begin_char"] == temp_name  ]
    if len( df_s.index ) >0  : 
        df_fund_C.loc[temp_i, "code_others" ] = ",".join( df_s["基金代码"].values  )
        df_fund.loc[ df_s.index , "code_C" ] = df_fund_C.loc[temp_i, "基金代码" ]


################################################################################
### Step 2，ranking - 收益率   近3月	40%；近6月	40%；近1年	20%
# pd.merge(df1, df2, left_on='column_name', right_on='column_name') # 文件合并，left_on=左侧DataFrame中的列；right_on=右侧DataFrame中的列

# notes:"近3月"对应收益率,"Unnamed: 7"对应近3个月排名；"近6月"对应收益率,"Unnamed: 9"对应近6个月排名
df_perf = dict_fund_data["业绩表现"].loc[:,["基金代码","近3月","近6月","近1年" ] ]
# notes: left_on="基金代码", right_on="基金代码" 会导致合并后的df有2个columns:"基金代码_x",""基金代码"_y"
df_fund = pd.merge( df_fund, df_perf  , on="基金代码" )

################################################################################
### Step 3，ranking - 风险 和 超额收益 | 都在一张表里
# 风险: 历史最大回撤	100%； 波动率（%）	30%； 下行风险（%）	70%
# 最大回撤已经在 df_fund 里了
# 超额收益 ：Alpha（%）	30%；Sharpe	20%；Sortino	30%；Calmar	20%
df_risk = dict_fund_data["风险收益"] 
df_fund = pd.merge( df_fund, df_risk  , on="基金代码" )

################################################################################
### Step 4，ranking - 投资风格	rank-roe	50%；	rank-换手率	50%
# df_style = dict_fund_data["投资风格"].loc[:,["基金代码","重仓ROE（%）","换手率（%）" ] ] 
df_style = dict_fund_data["投资风格"].loc[:,["基金代码","换手率（%）" ] ] 
df_fund = pd.merge( df_fund, df_style  , on="基金代码" )
# notes: "换手率（%）" 在季报期可能没有数值； "重仓ROE（%）"wind说暂时不提供
df_fund["换手率（%）"]=df_fund["换手率（%）"].replace("--",-1)

################################################################################
### Step 5 ranking 投研团队实力	获奖次数 10%；	基金公司收益率排名	50%,pct_基金公司规模	30%；	成立年限	10%
df_award = dict_fund_data["获奖情况"].loc[:,["基金代码","获奖次数" ] ]
df_fund = pd.merge( df_fund, df_award , on="基金代码" )

################################################################################
### Step 6 ranking 交易便利	： 基金规模	50%；	管理费率	50%
df_trade = dict_fund_data["交易信息"].loc[:,["基金代码","管理费率(%)","托管费率(%)" ] ]
df_trade["fee"] = df_trade["管理费率(%)"] + df_trade["托管费率(%)"] 
df_fund = pd.merge( df_fund, df_trade , on="基金代码" )


################################################################################
### Part 2，数据分析
################################################################################
### Step 0，对每个指标进行排序、标准化打分、加权合并
### 数值改造：异常值替代"--"、string，过小值等；对于部分列，需要把非数值部分改为数值
# 一定要有的数据： 近3月 近6月   近1年；Sharpe Sortino；  | notes:Sharpe Sortino基本一定会有；重仓ROE（%）  换手率（%）暂时没有
# 数值可以为空的：Alpha（%）   Calmar     波动率（%）   下行风险（%） | notes:Alpha（%）   Calmar,基金业绩不到1年没有
# notes: "换手率（%）" 在季报期可能没有数值； "重仓ROE（%）"wind说暂时不提供
for temp_str in ["近3月","近6月","近1年","Sharpe","Sortino","成立年限","规模(亿)" ] :
    df_drop = df_fund [ df_fund[ temp_str ] == "--" ]
    df_fund = df_fund.drop( df_drop.index , axis = 0   )    
    df_fund[ temp_str ] = df_fund[ temp_str ].astype(float)  
# 211103：近1年2282只基金业绩大于1年， 594只基金小于1年。

for temp_str in ["Alpha（%）","Calmar","获奖次数"] :
    # 数值越大越好， 缺失值用最小值代替 
    try : 
        v_min = df_fund[ temp_str ].min()  
    except : 
        v_min = -1 
    # notes: to_replace 是被替代的数据
    df_fund[ temp_str ]  =df_fund[ temp_str ].replace( to_replace="--"  , value=v_min )
    

for temp_str in [ "波动率（%）","下行风险（%）","fee"] :
    # 数值越小越好， 缺失值用最大值代替；因为数值没有负号
    df_fund[ temp_str ]  =df_fund[ temp_str ].replace( to_replace= "--", value=df_fund[ temp_str ].max() ) 

###########################################
### 替代极端值，
def df_replace_pct(df_fund,col_list, para_head,para_tail ):
    ### 替换1%极端值
    # df_fund.quantile(0.99,axis=0) 类型是series；转df格式后 0.99 是columns
    # 参数para_head对应前1%百分位数值，0.01对应后99%数值，默认是数值越大越好
    df_head = pd.DataFrame( df_fund.quantile( para_head ,axis=0) ) 
    # print(  df_head.loc["近6月" , 0.99] )
    df_tail = pd.DataFrame( df_fund.quantile( para_tail ,axis=0) )

    ### 函数 替换百分位
    # def replace_pct( list_value , v_head, v_tail ): 
    #     return list_value
    
    for temp_col in col_list : 
        # print("Debug temp_col\n" ,temp_col ,  df_head  , df_tail )
        # Apply函数：df.apply()时，axis = 1，一行数据作为Series，用于计算比如求和sum； 对一列操作axis=0 
        v_head =  df_head.loc[temp_col , para_head]
        v_tail =  df_tail.loc[temp_col , para_tail]
        df_fund[ temp_col ] = df_fund[ temp_col ].apply( lambda x : v_head if x>= v_head else (v_tail if x <= v_tail else x )  )


    return df_fund 

### col_list_upward:col越大越好， col_list_downward 越小越好
col_list_upward = ["近3月","近6月","近1年","Sharpe","Sortino","Alpha（%）","Calmar","获奖次数","成立年限","最大回撤"] 
if fundpool_type == "4" :
    ### 货币3 |notes:货币基金没有最大回撤
    col_list_upward = ["近3月","近6月","近1年","Sharpe","Sortino","Alpha（%）","Calmar","获奖次数","成立年限" ] 

# col_list_downward =["换手率（%）","波动率（%）","下行风险（%）","fee","规模(亿)"]
col_list_downward =["波动率（%）","下行风险（%）","fee","规模(亿)"]
col_list = col_list_upward + col_list_downward
para_head = 0.99
para_tail = 0.01
# df_fund.to_excel(  "D:\\test.xlsx" ,sheet_name="基础池计算",encoding="gbk"  )    
df_fund = df_replace_pct(df_fund,col_list, para_head,para_tail) 

###########################################
### 去排名；标准化打分
num_fund = len( df_fund.index )
for temp_col in col_list_upward :
    # notes: rank() 数值最小的是1，最大的是2876；对应最小的值是最大的排序数字，因此不需要处理 
    df_fund["pct_"+ temp_col ] =  df_fund[temp_col ].rank()   / num_fund 

for temp_col in col_list_downward  :
    # notes: rank() 数值最大的是1，最小的是2876；col_list_downward需要反过来处理一下
    df_fund["pct_"+ temp_col ] =  ( num_fund + 1 - df_fund[temp_col ].rank() ) / num_fund

################################################################################
### Part 3，加权打分
###########################################
### 加权合并 
# notes： 之前已经计算过了的 ：	pct_基金公司业绩 ,50%,pct_基金公司规模	30%
col_list_upward = ["近3月","近6月","近1年","Sharpe","Sortino","Alpha（%）","Calmar","获奖次数","成立年限","基金公司业绩","基金公司规模"] 
# col_list_downward =["换手率（%）","波动率（%）","下行风险（%）","fee","规模(亿)"]
col_list_downward =["波动率（%）","下行风险（%）","fee","规模(亿)"]

'''收益率	近3月	40%	近6月	40%	近1年	20%
最大回撤	历史最大回撤 60%	波动率（%）	10%	下行风险（%）	30%
超额收益	Alpha（%）	30%	Sharpe	20%	Sortino	30%	Calmar	20%
投资风格	rank-roe	50%	rank-换手率	50%
投研团队实力	获奖次数	10%	基金公司收益率排名	50%	基金公司规模排名	30%	成立年限	10%
交易便利	基金规模	50%	管理费率	50%
'''
### 
df_fund["score_"+ "收益率" ] = df_fund["pct_" +"近3月" ]*0.4 +  df_fund["pct_" +"近6月" ]*0.4 +  df_fund["pct_" +"近1年" ]*0.2

if fundpool_type == "4" :
    ### 货币3 |notes:货币基金没有最大回撤
    df_fund["score_"+ "最大回撤" ] =   df_fund["pct_" + "波动率（%）" ]*0.3 +  df_fund["pct_" +"下行风险（%）" ]*0.7
else :
    df_fund["score_"+ "最大回撤" ] = df_fund["pct_" + "最大回撤" ]*0.6 +  df_fund["pct_" + "波动率（%）" ]*0.1 +  df_fund["pct_" +"下行风险（%）" ]*0.3

df_fund["score_"+ "超额收益" ] = df_fund["pct_" +"Sharpe" ]*0.2 +  df_fund["pct_" +"Sortino"]*0.3 +  df_fund["pct_" +"Alpha（%）" ]*0.3 +  df_fund["pct_" +"Calmar"  ]*0.2
# notes: roe 暂时没有数据；"换手率（%）"季度没有数据
# df_fund["score_"+ "投资风格" ] = df_fund["pct_" +"换手率（%）" ]
df_fund["score_"+ "投资风格" ] = 0.0

df_fund["score_"+ "投研团队实力" ] = df_fund["pct_" + "获奖次数"]*0.1 +  df_fund["pct_" +"成立年限" ]*0.1 +  df_fund["pct_" +"基金公司业绩"]*0.5+  df_fund["pct_" + "基金公司规模" ]*0.3
df_fund["score_"+ "交易便利" ] = df_fund["pct_" + "规模(亿)"]*0.5 +  df_fund["pct_" +"fee" ]*0.5

########################################
### dict_w 是各项基金分析各项指标的权重
# dict_w ={} 
# dict_w["收益率"] = 0.3
# dict_w["最大回撤"  ] = 0.25
# # dict_w["投资风格" ] = 0.05
# dict_w[ "超额收益" ] = 0.25
# dict_w["投研团队实力"  ] = 0.15
# dict_w["交易便利"] = 0.05

df_fund["score" ] = 0.0 
# for temp_col in ["收益率","最大回撤","投资风格",  "超额收益",  "投研团队实力" ,"交易便利"]:
for temp_col in ["收益率","最大回撤",   "超额收益",  "投研团队实力" ,"交易便利"]:
    df_fund["score" ] =df_fund["score" ] + df_fund["score_"+ temp_col ] * dict_w[temp_col]

df_fund["pct_score" ] =  df_fund[ "score" ].rank()   / num_fund 

df_fund=df_fund.sort_values(by="score",ascending=False )
print("前十名基金 \n ", df_fund.head(10).T )


################################################################################
### Part 4，EXHI

### step 0，基础池和核心池数量
if fundpool_type in ["1","2","3" ] :
    # 货币和FOF数量少
    len_025 = int(  max(100, round( num_fund*0.25 ,0 ) ) )
    print("基础池基金数量：",len_025 )
    df_fundpool = df_fund.head( len_025  )

    len_005 = int( max(100 , round( num_fund*0.05 ,0 ) ) )
    print("核心池基金数量：",len_005 )
if fundpool_type in ["4","5","6"  ] :
    # 货币和FOF数量少
    len_025 = int(  max(100, round( num_fund*0.25 ,0 ) ) )
    print("基础池基金数量：",len_025 )
    df_fundpool = df_fund.head( len_025  )

    len_005 = int( min(100 , round( num_fund*0.05 ,0 ) ) )
    print("核心池基金数量：",len_005 )

#########################################
### step 1，保留部分columns，并更名
# "基金代码","基金名称_x","类型","收益率（%）","同类排名","年化收益率(%)(年初至今)","七日年化(%)","申购限额(万)","最大回撤",
# "规模(亿)","成立年限","基金经理","管理人","综合评级","基金公司规模","pct_基金公司规模","基金公司业绩","pct_基金公司业绩",
# "近3月","近6月","近1年","基金名称_y","Alpha（%）","Sharpe","Sortino","Calmar","波动率（%）","下行风险（%）","重仓ROE（%）",
# "换手率（%）","获奖次数","管理费率(%)","托管费率(%)","fee",
# "pct_近3月","pct_近6月","pct_近1年","pct_Sharpe","pct_Sortino","pct_Alpha（%）","pct_Calmar","pct_获奖次数","pct_成立年限",
# "pct_最大回撤","pct_换手率（%）","pct_波动率（%）","pct_下行风险（%）","pct_fee","pct_规模(亿)",
# "score_收益率","score_最大回撤","score_超额收益","score_投资风格","score_投研团队实力","score_交易便利","score","pct_score"
col_list_keep = [ "基金代码","基金名称_x","类型","近3月","近6月","近1年", "score_收益率","score_最大回撤","score_超额收益","score_投资风格","score_投研团队实力","score_交易便利","score","pct_score","code_C" ]
col_list_replace = [ "基金代码","基金名称","类型","近3月收益率","近6月收益率","近1年收益率", "收益率得分","最大回撤得分","超额收益得分","投资风格得分","投研团队实力得分","交易便利得分","总分","总分百分位","C类基金代码" ]

### 仅保留部分列
df_fundpool = df_fundpool.loc[:, col_list_keep ]
# notes: zip(list1,list2)本身不是dict需要转换 。list1会变成keys，list2变成values
dict_column = dict( zip(col_list_keep, col_list_replace)  )
### 部分列改名
df_fundpool = df_fundpool.rename( columns=dict_column )

### step 2，小数点
# todo： 百分位乘以100 
for temp_col in ["收益率得分","最大回撤得分","超额收益得分","投资风格得分","投研团队实力得分","交易便利得分","总分","总分百分位"  ] :
    df_fundpool[temp_col] = df_fundpool[temp_col] *100

### 保留1位小数的
col_list_1 = [ "收益率得分","最大回撤得分","超额收益得分","投资风格得分","投研团队实力得分","交易便利得分","总分","总分百分位"  ]
df_fundpool.loc[:, col_list_1 ] = df_fundpool.loc[:, col_list_1 ].round(1)

### 保留2位小数的
col_list_2 = [ "近3月收益率","近6月收益率","近1年收益率"]
df_fundpool.loc[:, col_list_2 ] = df_fundpool.loc[:, col_list_2 ].round(2)

### 基金公司 df_comp | 
df_comp = df_comp.round(2)
col_list_keep = ["规模加权收益率", "收益率（%）","成立年限","年化收益率(%)(年初至今)","规模(亿)" ]
col_list_replace = [ "规模加权收益率","平均收益率","平均成立年限","平均年化收益率(%)(年初至今)","总规模(亿)" ]
# notes: zip(list1,list2)本身不是dict需要转换 。list1会变成keys，list2变成values
dict_column2 = dict( zip(col_list_keep, col_list_replace)  )
### 部分列改名
df_comp = df_comp.rename( columns=dict_column2 )

#########################################
### 保存多个df到excel-sheet文件
writer_excel = pd.ExcelWriter( path_output + file_output )

### 基金产品
df_fundpool.head( len_005  ).to_excel( writer_excel ,sheet_name="核心池",encoding="gbk"  ) 
df_fundpool.to_excel( writer_excel ,sheet_name="基础池",encoding="gbk"  ) 

### 基金公司
df_comp.to_excel( writer_excel ,sheet_name="基金公司",encoding="gbk"  )#  

### 基金经理 | 乘以股票比例？
df_manager.to_excel( writer_excel ,sheet_name="基金经理",encoding="gbk"  )#  

### 统计，原始数据
df_fundpool.head( len_005  ).describe().to_excel( writer_excel ,sheet_name="核心池统计",encoding="gbk"  ) 
df_fundpool.describe().to_excel( writer_excel ,sheet_name="基础池统计",encoding="gbk"  ) 
df_comp.describe().to_excel( writer_excel ,sheet_name="基金公司统计",encoding="gbk"  )
df_manager.describe().to_excel( writer_excel ,sheet_name="基金经理统计",encoding="gbk"  )
### 原始数据和C类基金
df_fund.to_excel( writer_excel ,sheet_name="raw_data",encoding="gbk"  ) 
df_fund_C.to_excel( writer_excel ,sheet_name="C类基金",encoding="gbk"  ) 

writer_excel.save()
writer_excel.close()


# 基金初步筛选：近3年收益率排名前1000，剔除C类 | 年初至今收益率前100名；成立3年以上且年化收益率大于19.5%

ASD 
