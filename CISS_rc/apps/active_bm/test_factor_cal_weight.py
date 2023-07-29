# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
用50只股票实现对主要市场指数的抽样复制
last 200410  | since 200327

Input
1，code_index：

Output：
1，

经典多因子模型计算过程:
导入历史月份数据
判断是从第一期开始算还是已经计算部分日期之后继续计算
循环：
1,给定交易日,给定选样空间，例如导入沪深300指数成分
2,因子数据构建
2.1,获取所属行业分类|中信一级
2.2,获取流通市值,总市值，部分基本面指标| 包括日衍生行情所有指标数据
2.2,获取价格和成交量指标 |包括A股日行情所有指标数据
2.3,计算流通市值和市值标准分zscore
2.4,因子做对称正交处理
2.5,计算各因子12个月ICIR，作为各因子权重
2.5.1,对每只个股，计算IC单期的IC值；信息系数（Information Coefficient，简称 IC
2.5.2, 对每只个股，计算多期IC计算ICIR值，ic_ir= ic_miu/ic_std; Grinold的算法是IR=ic*sqrt(N)
2.5.3,根据IC_IR的值，计算股票i在因子k{1，2,...,K}上的因子权重

'''
#################################################################################
### Initialization 
import os 
# 获取当前目录 os.getcwd() =: G:\zd_zxjtzq\ciss_web
import sys
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\" )
sys.path.append(os.getcwd()[:2] + "\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )
sys.path.append(os.getcwd()[:2] + "\\ciss_web\\CISS_rc\\db\\" )
sys.path.append(os.getcwd()[:2] + "\\ciss_web\\CISS_rc\\db\\db_assets\\" )

import pandas as pd 
import numpy as np 
import math
import datetime as dt 
time_0 = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

### 
from analysis_indicators import indicator_ashares,analysis_factor
indicator_ashares_1 = indicator_ashares()
analysis_factor_1 = analysis_factor()

from transform_wind_wds import transform_wds
transform_wds1 = transform_wds()

#################################################################################
### Paramater 参数设置
'''表格AIndexHS300FreeWeight内最早有权重的日期为
"000300.SH"-20050408,"000906.SH"-20070115,"000852.SH"-20141031,399001.SZ,"ALL"
指数或全市场选股：code_index = "000300.SH","000906.SH","000906.SH","ALL",399001.SZ-20090930,399006.SZ-20100630

'''
code_index = "000300.SH"

#################################################################################
### 经典多因子模型计算过程:

path_factor_model = "D:\\CISS_db\\factor_model\\"
path_factor_model_sub = path_factor_model+ code_index +"\\"
### 对以下步骤计算第1~12期，为下一步ICIR计算做准备
# 200505,200506,...,200604 开始有之前12期的数据
'''
分析：先对每个股票收益率计算各个因子的暴露，再对组合内按股票权重加权。
例如：对于股票s和逐个因子过去12期的值，如流通市值因子 szcore_S_DQ_MV,
计算IC_i_k = corr(因子k过去12期值，相对行业的超额收益率过去12期值 )
IC_IR= 
'''
### 导入历史月份数据
# 用date_list_month_050501.csv代替 "date_list_m_050501_200404.csv" ; D:\db_wind\data_adj
# file_name_date_list = "date_list_m_050501_200404.csv"
# df_date_month = pd.read_csv(path_factor_model + file_name_date_list )
file_name_date_list = "date_list_month_050501.csv"
df_date_month = pd.read_csv( transform_wds1.path_adj + file_name_date_list )

date_list_month = df_date_month["date"].values
# 日期升序排列
date_list_month.sort()
# print("date_list_month",date_list_month ) 
########################################################################
### if_generate= 0 or 1,判断是从第一期开始算还是已经计算部分日期之后继续计算
if_generate = 1 # 1 means update followin periods

date_last_month = date_list_month[-1] # 上一次更新的日期，注意，必须是月末
input1= input("Check date_last_month to proceed "+str(date_last_month) )

#Qs:line 353 开始的count_month会导致第三个月开始才有IC值，第6个月开始才有IC_IR;
#Ana:一个解决方案可能是先导入历史df_factor和df_IC_adj_20191031_000300.SH_20191031.csv等数据
if if_generate == 0 :
    ### 新建对应DataFrame和Object
    # date_list_month = ["20050501","20050531","20050630","20050731","20050831","20050930","20051031","20051130","20051231"]
    # factor在df_factor_ortho里，ret_excess在df_factor 
    ### 保存当期每个股票的因子值和：df_date_factor_return
    # df_date_factor_return = pd.DataFrame( index=date_list_month ) 这个会导致重复的index，之后的赋值会在新的行
    df_date_factor_return = pd.DataFrame()
    count_month = 0
else :
    # date_list_month_pre 是已经计算过的月份
    date_list_month_pre = [date for date in date_list_month if date<= date_last_month ]
    # date_list_month_pre 是根据最新日期还未计算的月份
    date_list_month = [date for date in date_list_month if date> date_last_month ]
    count_month = len(date_list_month_pre )
    
    ### Import df_factor_20060125_000300.SH or df_factor_20060125_000300.SH_20060125
    file_name_output= "df_factor_" +str( date_last_month) +"_"+ code_index +  ".csv"
    df_factor = pd.read_csv( path_factor_model_sub + file_name_output,index_col=0 )

    ### Import df_date_factor_return
    file_name_output= "df_date_factor_return_" +str( date_last_month) +"_"+ code_index +".csv"

    df_date_factor_return  = pd.read_csv( path_factor_model_sub + file_name_output,index_col=0 )
    
    print("df_date_factor_return",len(df_date_factor_return) )
    # 找到index是重复值的;发现index作为日期重复了2次，第二次的index对应的数值是对的。
    # Ana:index的前200505-202004行都是空的，之后的赋值都在第二次出现，查了一下发现groupby似乎可以选择.last()功能
    df_date_factor_return = df_date_factor_return.groupby(df_date_factor_return.index ).last()
    
    ### Import df_IC_adj_20060125_000300.SH_20060125 
    file_name_output= "df_IC_adj_" +str( date_last_month) +"_"+ code_index+ ".csv"
    df_ic_ir = pd.read_csv( path_factor_model_sub + file_name_output,index_col=0 ) 
    # all type(  df_ic_ir.loc[temp_i,"date"] ) == np.int64

    ### Import factor weight 


### Loop 
for temp_date in date_list_month : 
    temp_date = str( int(temp_date) )
    #################################################################################
    ### 1,给定交易日,给定选样空间，例如导入沪深300指数成分
    obj_in_index={} 
    obj_in_index["date_start"] = temp_date
    obj_in_index["code_index"] = code_index
    obj_in_index["table_name"] = "AIndexHS300FreeWeight"

    obj_out_index = indicator_ashares_1.ashares_index_constituents(obj_in_index) 
    # obj_in_index["df_ashares_index_consti"] 
    temp_code = obj_out_index["df_ashares_index_consti"]["S_CON_WINDCODE"].values[0]

    print("temp_code ", temp_code  )
    #################################################################################
    ### 2,因子数据构建
    code_list = obj_out_index["df_ashares_index_consti"]["S_CON_WINDCODE"].values
    df_factor = pd.DataFrame( code_list,columns=["wind_code"] )
    df_factor["code_index"] =  obj_in_index["code_index"]
    df_factor["date"] =  obj_in_index["date_start"]

    ########################################
    ### 2.1,获取所属行业分类|中信一级 
    date_end = obj_in_index["date_start"]
    if_all_codes=0 #  == "1"means import all code_list 
    object_ind = transform_wds1.get_ind_date(code_list,date_end,if_all_codes )
    # print("行业分类:", object_ind["df_s_ind_out"] )

    # notes: 将中信一级行业分类和流通市值、总市值放进同一个df
    df_s_ind_out = object_ind["df_s_ind_out"]
    col_list_ind_sub= ["citics_ind_code_s_1","sw_ind_code_s_1" ,"wind_ind_code_s_1" ]
    for temp_i in df_factor.index :
        temp_code = df_factor.loc[temp_i, "wind_code"]
        df_ind_sub = df_s_ind_out[ df_s_ind_out["wind_code"]== temp_code ]
        ### notes：600087.SH于2014年退市，对应行业分类只有sw，并且纳入日期20080602，却是2005年的指数成分
        # 对于这种情况，将其归类于 其他 行业分类
        if len( df_ind_sub.index )>0 :
            temp_j = df_ind_sub.index[0] 
            for temp_col in col_list_ind_sub :
                df_factor.loc[temp_i, temp_col] = df_s_ind_out.loc[temp_j, temp_col ]
        
    ########################################
    ### 2.2,获取流通市值,总市值，部分基本面指标| 包括日衍生行情所有指标数据

    obj_in_stock={} 
    obj_in_stock["date_start"] = obj_in_index["date_start"]
    obj_in_stock["table_name"] = "AShareEODDerivativeIndicator" 
    obj_in_stock["df_factor"] = df_factor

    obj_stock = indicator_ashares_1.ashares_stock_funda(obj_in_stock ) 
    # 包括日衍生行情所有指标数据
    # df_factor = obj_stock["df_factor"]
    # 主要需要：当日总市值|万 S_VAL_MV ;当日流通市值|万 S_DQ_MV 
    # 换手率指标也在里边

    ########################################
    ### 2.3,获取价格和成交量指标 |包括A股日行情所有指标数据
    ''' 
    根据"date_start"，往前取20~120天，计算主要的价量指标；
    Input:
    1,obj_in_stock:object类型，至少包括 "date_start","table_name"
    2,[]
    近1个月日均成交额比近6个月日均成交额均值 amt_ave_1m_6m = amt_ave_1m/amt_ave_6m
    近1个月日均换手率比近6个月日均换手率均值 turnover_ave_1m_6m= turnover_ave_1m/turnover_ave_6m
    近1个月日均波动率比近6个月日均波动率标准差 volatility_std_1m_6m= volatility_std_1m/volatility_std_6m
    20天移动平均价格比120天移动平均价格 ma_20d_120d = ma_20d/ma_120d
    20天平均涨跌幅比120天平均涨跌幅 ret_averet_ave_20d_120d = ret_averet_ave_20d/ret_averet_ave_120d
    20天累计涨跌幅比120天累计涨跌幅 ret_accumu_20d/ret_accumu_120d
    20天内最大回撤比120天内最大回撤 ret_mdd_20d/ret_mdd_120d
    收盘价在过去52周内百分比 close_pct_52w 
    20天和120天个股相对于中信一级的行业市值加权超额收益率 ret_alpha_ind_citic_1_20d,ret_alpha_ind_citic_1_120d
    20天和120天个股相对于基准指数的超额收益率 ret_alpha_index_bm_20d,ret_alpha_index_bm_120d
    20天和120天个股相对于全样本空间市值加权的超额收益率 ret_alpha_stockpool_mv_20d,ret_alpha_stockpool_mv_120d
    todo,20天和120天个股相对于全市场成交额加权的超额收益率 ret_alpha_market_amt_20d,ret_alpha_market_amt_120d


    col_list = ["close_52w_last","close_52w_low", "close_52w_high" ,"close_pct_52w"]
    col_list = col_list +["amt_ave_1m","amt_ave_6m","amt_ave_1m_6m","turnover_ave_1m","turnover_ave_6m","turnover_ave_1m_6m" ]
    col_list = col_list +["volatility_std_1m","volatility_std_6m","volatility_std_1m_6m"]
    col_list = col_list +["ret_averet_ave_20d","ret_averet_ave_120d","ret_averet_ave_20d_120d"]
    col_list = col_list +["ma_20d","ma_120d","ma_20d_120d","ret_accumu_20d","ret_accumu_120d","ret_accumu_20d_120d" ]
    col_list = col_list +["ret_mdd_20d","ret_mdd_120d","ret_mdd_20d_120d"]
    col_list = col_list +["ret_alpha_ind_citic_1_20d","ret_alpha_ind_citic_1_120d"]
    col_list = col_list +["ret_alpha_stockpool_mv_20d","ret_alpha_stockpool_mv_120d"]
    col_list = col_list +["ret_alpha_index_bm_20d","ret_alpha_index_bm_120d"]
    '''
    obj_stock["table_name"] = "AShareEODPrices"
    obj_stock = indicator_ashares_1.ashares_stock_price_vol_sub(obj_stock  )

    file_name_output= "df_factor_" +obj_in_index["date_start"] +"_"+ obj_in_index["code_index"] +".csv"
    obj_stock["df_factor"].to_csv(path_factor_model_sub +file_name_output) 

    ########################################
    ### 2.3,计算流通市值和市值标准分zscore
    '''市现率(PCF,经营现金流TTM) S_VAL_PCF_OCFTTM;市盈率(PE,TTM),S_VAL_PE_TTM
    换手率,S_DQ_TURN;52周最高价(复权),S_PQ_ADJHIGH_52W;52周最低价(复权),S_PQ_ADJLOW_52W
    归属母公司净利润(TTM),NET_PROFIT_PARENT_COMP_TTM;当日净资产,NET_ASSETS_TODAY
    To calculate:
    1,当前价格在52周内百分位 price_pct_52w = (p- low)/(high - low )
    2,过去20个交易日换手率/120天换手率: ...需要回溯多个交易日表格
    3,净资产收益率 roe_ttm= NET_PROFIT_PARENT_COMP_TTM/NET_ASSETS_TODAY

    # notes:"S_PQ_ADJHIGH_52W"等指标需要依靠ashares_stock_price_vol_sub步骤，在df_factor已经导入了的。
    '''
    df_factor = obj_stock["df_factor"]
    df_factor["roe_ttm"] = df_factor["NET_PROFIT_PARENT_COMP_TTM"]/df_factor["NET_ASSETS_TODAY"]
    df_factor["ep_ttm"] = 1/ df_factor["S_VAL_PE_TTM"]
    col_list_to_zscore =["ep_ttm","S_VAL_PCF_OCFTTM","roe_ttm"  ] 

    ### 方法一：将全部价量指标都当作因子；这会导致因子正交无法进行
    col_list_price_vol = obj_stock["col_list_price_vol"]
    # notes:加上col_list_price_vol全部指标会有34个因子时无法计算，可能是因为收益率的因子太多了。
    ### 方法二：设定因子具体值
    # # 需要减少
    col_list_to_zscore = col_list_to_zscore+[ "close_pct_52w","amt_ave_1m_6m","turnover_ave_1m_6m"]
    col_list_to_zscore = col_list_to_zscore+["volatility_std_1m_6m","ret_averet_ave_20d_120d" ]
    col_list_to_zscore = col_list_to_zscore+["ma_20d_120d","ret_accumu_20d","ret_accumu_20d_120d"]
    col_list_to_zscore = col_list_to_zscore+["ret_mdd_20d","ret_mdd_20d_120d"]
    col_list_to_zscore = col_list_to_zscore+["ret_alpha_ind_citic_1_20d","ret_alpha_ind_citic_1_120d"   ]

    # col_list_to_zscore 包括了需要计算标准分值zscore的指标，
    obj_factor = analysis_factor_1.indicator_data_adjust_zscore(df_factor,col_list_to_zscore )
    # obj_factor["df_factor"] ;obj_factor["col_list_zscore"] 包括所有zscore列名的column list  

    df_factor = obj_factor["df_factor"] 
    file_name_output= "df_factor_" +obj_in_index["date_start"] +"_"+ obj_in_index["code_index"] +".csv"
    df_factor.to_csv(path_factor_model_sub +file_name_output  )
    
    ########################################
    ### 2.4,因子做对称正交处理； 
    '''
    因子正交 factor SymmetricOrthogonalization的步骤：
    file=天风证券-专题报告：因子正交全攻略——理论、框架与实践.pdf;
    path=.\TOUYAN\天风证券金工合集\多因子选股系列报告\
    step1，求t时间矩阵df1[股票数量N，因子数量K]的协方差矩阵Sigma= df1.cov(),重叠矩阵M=np.matrix( (N-1)*Sigma );
    step2,求解S*S_t= inv(M) :矩阵逆的2种方法：1，M_inv=np.matrix(M).I ;2,np.linalg.inv(M) 3,伪逆-不可逆的情况，np.linalg.pinv(M) ;
    step3：M_inv=U*D_inv*U', (A,B)=np.linalg.eig(M_inv), M_inv*A=B*A，B是特征向量矩阵，A是特征值vector；
    D = np.dot( np.dot(np.linalg.inv(B),M_inv ),B ) ;矩阵中的每个数字都保留两位有效数字 D2 = np.round( D,decimals=2,out=None )
    D_inv = np.linalg.inv( D )
    求 D_inv_sqrt,对对角线上的每个值求平方根的倒数，即 1/sqrt( rambda1 )
    过度矩阵 S = U* D_inv_sqrt* U' *C , 需要求得U 和 C；
    U是M的特征向量矩阵，即(V,U) =np.linalg.eig(M) 可以求得 U；
    M_inv_sqrt = U * D_inv_sqrt * U'
    规范正交方法：S=U* D_inv_sqrt* U' *C, { C=U} = U* D_inv_sqrt
    规范正交后的因子没有稳定的对应关系。规范正交和PCA一样，在每个截面上以方差最大的方向来确定第一主成分，但是不同截面上第一主成分的方向可能会差别很大，这样就导致不同截面上主成分序列上的因子没有稳定的对应关系[Qian 2007]。
    比较分析：施密特正交由于在过去若干个截面上都取同样的因子正交顺序，因此正交后的因子和原始因子有显式的对应关系，而规范正交在每个截面上选取的主成分方向可能不一致，导致正交前后的因子没有稳定的对应关系。由此可见，正交后组合的效果，很大一部分取决于正交前后因子是否有稳定的对应关系。 br
    对称正交（SymmetricOrthogonalization，[Löwdin1970, Schweinler1970]）是一种特殊的正交方法，它的过渡矩阵是取CK×K=𝐼𝐾×𝐾，即
    对称正交 S = U* D_inv_sqrt* U'
    对称正交有几个重要的性质[Klein 2013]：1.相对于施密特正交法，对称正交不需要提供正交次序，对每个因子是平等看待的；
    2.在所有正交过渡矩阵中，对称正交后的矩阵和原始矩阵的相似性最大，即正交前后矩阵的距离最小。我们用变化前后的矩阵的距离（Frobenius 范数）𝜙来衡量因子正交前后变化的大小
    '''
    obj_factor["col_list_zscore"] = col_list_to_zscore

    # obj_factor至少包括 "df_factor","col_list_zscore"
    obj_factor =  analysis_factor_1.indicator_indicator_orthogonal( obj_factor )
    # df_factor_ortho是只有给定columns的正交后的df，df_factor是包括全部columns的df。
    # obj_in["df_factor_ortho"] = df_factor_ortho

    print("col_list_zscore",obj_factor["col_list_zscore"])
    print( df_factor.head().T )
    # df_factor = obj_factor["df_factor"] 
    df_factor_ortho = obj_factor["df_factor_ortho"] 
    file_name_output= "df_factor_ortho_" +obj_in_index["date_start"] +"_"+ obj_in_index["code_index"] + "_ortho.csv"
    print("file_name_output:", file_name_output )
    df_factor_ortho.to_csv(path_factor_model_sub +file_name_output  )

    ################################################################################
    ### 保存当期每个股票的因子值和：df_date_factor_return
    # factor在df_factor_ortho里，ret_excess在df_factor
    for temp_i in df_factor.index :
        temp_code = df_factor.loc[temp_i,"wind_code"]
        ### 保存Factor 部分
        for temp_f in obj_factor["col_list_zscore"] :
            # notes:df_factor_ortho的列名指标是例如"ep_ttm",没有"_zscore"前缀
            # Debug=== Qs:cannot reindex from a duplicate axis
            # 20191129第一期出现 20191129 300142.SZ_ep_ttm 0 ep_ttm
            print(temp_date, temp_code+"_"+temp_f,temp_i,temp_f)
            df_date_factor_return.loc[temp_date, temp_code+"_"+temp_f ] = df_factor_ortho.loc[temp_i,temp_f ]
        
        ### 保存相对行业超额收益部分
        # ["ret_alpha_ind_citic_1_20d","ret_alpha_ind_citic_1_120d"] || line331，analysis_indicators.py
        df_date_factor_return.loc[temp_date, temp_code+"_ret_alpha_ind_citic_1_20d" ] = df_factor.loc[temp_i,"ret_alpha_ind_citic_1_20d" ]
        df_date_factor_return.loc[temp_date, temp_code+"_ret_alpha_ind_citic_1_120d" ] = df_factor.loc[temp_i,"ret_alpha_ind_citic_1_120d" ]

    file_name_output= "df_date_factor_return_" +obj_in_index["date_start"] +"_"+ obj_in_index["code_index"] +".csv"
    df_date_factor_return.to_csv(path_factor_model_sub +file_name_output  ) 

    
    ################################################################################
    ### 2.5,计算各因子12个月ICIR，作为各因子权重  
    ### 2.5.1,对每只个股，计算IC单期的IC值；信息系数（Information Coefficient，简称 IC
    ### 2.5.2, 对每只个股，计算多期IC计算ICIR值，ic_ir= ic_miu/ic_std; Grinold的算法是IR=ic*sqrt(N)
    ### notes:只有当累计月份count_month大于3时才计算当月的IC_adj值，IC_adj值大于3，也就是累计月份大于6时才计算ICIR;count_month >=6
    # input:df_date_factor_return,df_ic_ir,file_name_output
    obj_factor["count_month"] = count_month
    # obj_factor已经有了的keys： df_factor
    obj_factor["temp_date"] = temp_date
    obj_factor["df_date_factor_return"] = df_date_factor_return
    # 判断df的df_ic_ir 是否存在，注意需要用字符串判断
    if "df_ic_ir" in locals() or "df_ic_ir" in globals() :
        obj_factor["df_ic_ir"] = df_ic_ir 

    obj_factor = analysis_factor_1.indicator_indicator_icir( obj_factor )
    
    # Save to csv 
    if count_month >= 6 :
        file_name_output= "df_IC_adj_" +obj_in_index["date_start"] +"_"+ obj_in_index["code_index"]+".csv"
        obj_factor["df_ic_ir"].to_csv(path_factor_model_sub + file_name_output )
    
    ########################################
    ### 2.5.3,根据IC_IR的值，计算股票i在因子k{1，2,...,K}上的因子权重 
    ''' 
    因子收益和个股收益的投影： sum{i,1,N}(W_i_k*r_i)=f_k, k=1,2,...,K
    1,对IC_ir的极端值进行处理，求zscore：
    1.1,有大有小，当前导出的ic_ir数值，有"inf","-inf"两种是excel无法识别的，也有极大值和极小值需要剔除
    由于有极大值和极小值，因子指标的中位数基本是0.0，但均值、最大值、最小值、标准差的数值都非常大无法使用。
    1.2,关于数值：绝大部分还是处于+1/-1，尾部超过+5/-5的基本在20个以内/100个值
    1.3，IC_IR值越接近1，表示因子值和相对行业的超额收益越正相关，或者和超额收益的波动率越负相关。

    2,对于个股i，在因子1~K上的暴露之和为1，因此可以用历史IC均值或IC_IR均值算出个股在每个因子上的权重w_s_i_k;
        w_s_i_k = IC_IR_i_k_miu / sum(k,1,K)( IC_IR_i_k_miu ) , i=1,2,...,N
    3，对于市场组合，在因子k上的暴露为 IC_IR_k_miu / sum(k,1,K)( IC_IR_k_miu )
    '''
    
    if  count_month >= 6 :
        obj_factor["df_ic_ir"] = df_ic_ir
        obj_factor = analysis_factor_1.indicator_factor_weight( obj_factor )

        ### save to csv file 
        file_name_output = "df_factor_weight_" +obj_in_index["date_start"] +"_"+ obj_in_index["code_index"] + ".csv"
        obj_factor["df_factor_weight"].to_csv( path_factor_model_sub + file_name_output )
                
    ### finish current loop 
    count_month = count_month +1 
    obj_factor["count_month"] = count_month
asd





















































#################################################################################
### 1，导入指数数据，获取区间日/周波动率

# obj_in={}
# obj_in["type_date"] = 2 # 1 means 1date ;2 means period
# obj_in["date_start"] = "20050501"
# obj_in["date_end"] = "20050701"
# obj_in["type_indi"] = 1  # 1 means price ;2 means vol
# obj_in["code"] = "000300.SH"
# obj_in["table_name"] = "AIndexEODPrices"

# obj_in["date_freq"] = 1 # 1 means day, 2 means week, 3 means month, 4 means quarter
# obj_out_i = indicator_ashares_1.ashares_index_price_vol_sub(obj_in )

# ### TODO 如何根据日期频率date_freq，计算周/月/季度的平均收益率和波动率？？？
# # line 188, analysis_indicators.py

# ### 2，导入个股数据，获取区间日/周波动率，
# obj_in={}
# obj_in["type_date"] = 2 # 1 means 1date ;2 means period
# obj_in["date_start"] = "20050501"
# obj_in["date_end"] = "20050701"
# obj_in["type_indi"] = 1  # 1 means price ;2 means vol
# obj_in["code"] = "600028.SH"
# obj_in["table_name"] = "AShareEODPrices"

# obj_in["date_freq"] = 1 # 1 means day, 2 means week, 3 means month, 4 means quarter
# obj_out_s = indicator_ashares_1.ashares_stock_price_vol_sub(obj_in )

# ### 3,计算个股和指数的相关性
# X1 = obj_out_i["df_index_price"]["S_DQ_PCTCHANGE"]
# Y1 = obj_out_s["df_index_price"]["S_DQ_PCTCHANGE"]
# corr1 = X1.corr(Y1,method="pearson")
# print( round(corr1*100,2) )

# asd

# 以上等价于 X1.cov(Y1)/(X1.std()*Y1.std()) 
# print( X1.corr(Y1,method='spearman') )
# print( X1.corr(Y1,method='kendall') )

# X1.corr(Y1,method="pearson") #皮尔森相关性系数 #0.9481366640102855
# X1.cov(Y1)/(X1.std()*Y1.std()) #皮尔森相关性系数 # 0.9481366640102856
# X1.corr(Y1,method='spearman') #0.942857142857143
# X1.corr(Y1,method='kendall') #0.8666666666666666

'''
000300.SH vs
600036.SH 81.68
600000 75.83
600519 77.29
000002.SZ 75.32
600028.SH 80.1
'''