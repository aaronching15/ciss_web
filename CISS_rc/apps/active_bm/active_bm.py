# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function: realize active benchmark functions
功能：实现主动基准的实例测试。
last update 181018 | since 181018
Menu :
1,2012-2018年，抓去 5-31，11-30 的三个指数成份股 
    csi300,500,1000 index | 000300.SH, 000905.SH,000852.SH
    base date,20041231,20041231,20041231
    issue date,20050408,20070115,20141017   
    000906.SH = 000300.SH + 000905.SH
    notes:20110531抓取不到 000852 的CSI1000的成份股数据，000905可以。对于20140531
    抓取不到csi1000的数据，但是20141130就可以，说明是否有数据是跟着issue date 走的。

    
Notes:
1,wind-api数据提取： 限制条件：每周50万个，安全起见，每周45万。
2,wind api 每次只能抓取不超过100个数据。
===============================================
'''
import sys
sys.path.append("..")
### A test case 
## Get and save index weights, fundamental indicators
# from db.assets import stocks
# wind_api = stocks.wind_api()
# result = wind_api.Wind2Csv_test()
# result = wind_api.Data_funda_csvJson_test()

##################################################################

## Load and wash fundamental data 

## empirical steps 实证研究流程
## Algo: | base on p1_theory-model
'''
step 1 数据准备流程 
1.0，information process：
    TS --> strategy --> ranking --> signal --> portfolio rebalance 
1.1，data_in |原始研究数据,data_in 经过研究员初步梳理后的结构化信息     
1.2，estimates |根据data_in,生成对资产的某种预测，以变量，参数，方程等形式
1.3，TS | data_in --> estimates，logic分析逻辑，structure框架 --> TS    时间序列标准化:
     TS_i = {ts_i_j for asset i and information j in M sources | data_in, estimtes, logic, structure, N assets, M information sources  }
1.4，Analyzing：algo model,econ. model , ...... tail risk,active h_a ??
1.5, strategy：
1.6，ranking
1.7，trade：portfolio rebalance
1.8，port. evaluation performance and risk
'''
# step 1.1,data_in |原始研究数据,data_in 经过研究员初步梳理后的结构化信息 
'''At t0 time, we need to set an initial position, a natural idea might be replicate market 
portfolio.
Time horizon, at 20130531,8个指标中，行业指标非数值，其他7个需要计算均值/中位数，并对缺失值做处理。
对特征attribution标准化。
    Qs：如何确定初始值？ Ans：采用市场组合，或者主观的主动组合(基于基准的假设)
'''
### step1 data_in 
# 7个indicator从属于 核心因子:[行业，流动性，动量，主动收益，价值，成长，
# 资本结构，财务优势，经营能力，人力优势，信息优势]
#  
# 1，merge additional data into one pd： create/import database file,update data 
# with imported new info, save to database file.

from db.assets import stocks
stocks = stocks()
funda_wash = stocks.data_wash_funda()
file_path_funda = "C:\\zd_zxjtzq\\RC_trashes\\temp\\keti_sys_stra_24h\\p1\\wind_data\\funda\\"
time_stamp_input= '20181114'
output_type = 'dict'
dataG = funda_wash.Get_json_groupdata('','',file_path_funda,time_stamp_input,output_type ) 
# for given temp_index in dataG.index_list and temp_date in dataG.date_list

wind_code = '000300.SH'
temp_date = '2014-05-31'
temp_list = dataG.datagroup[wind_code+'_'+temp_date]
print('==========================')
print('temp_list, length ', len(temp_list['code']) )
print(temp_list.sum() )
print(temp_list.head() )

# # 2,因子标准化（Z-Score） 
# print( dataG.datagroup_zscore[wind_code+'_'+temp_date])

################################################################## 
##### 2,梳理数据结构和代码结构 
### 1.2，estimates |根据data_in,生成对资产的某种预测，以变量，参数，方程等形式
# todo 1,行业因子中性化；2，风格因子

# 对1,2,3,4级行业进行梳理 |分类梳理 新建wind行业对照表
#Name: Wind Industry Name | Wind Industry Code | Wind Industry Index Code
#indicator: industry_gics |industry_gicscode | indexcode_wind
# Wind-ind-index Name区别：Sector\industry group\industry\sub-industry
#case: 10101020 882401.WI
# history 行业管理 with 股票池管理，这是2个有关联但是独立的模块！
# process:建立Wind下 ind_code 和ind_index_code 的关系，一级到四级行业一一对应

#0，准备Wind名称下，行业代码，行业指数代码，行业名称，行业指数名称的对应关系(Wind或者GICS，其行业本身的定义未来都可能会变化的)
#1，根据t0时样本空间内(中证300+500+1500)的股票列表，抓取个股对应的wind 行业和行业成分股
#2，
'''
file: codelist_ind4.csv 
path: C:\zd_zxjtzq\RC_trashes\temp\sys_stra_24h\CISS_rc\db\db_assets
content:
wind_code    sec_name    ind4_index_code ind3_index_code ind2_index_code ind1_index_code ind4_code   ind3_index_code ind2_index_code ind1_index_code
000001.SZ   平安银行    882493.WI   882241.WI   882115.WI   882007.WI   40101010    401010  4010    40

notes:东方明珠等媒体III的股票没有四级的分类，因此目前用61个三级行业分类代表所有行业。
todo:
1,确定行业因子的标准化方式：
    zxjt P7 | 𝑤𝑖表示行业 i 中所有股票流通市值占全市场股票流通市值的比例，约束条件的选择不会影响模型拟
合，也不会影响模型的解释力，但其会对因子解释产生直接的影响
    sum( wi*fi ) = 0 
2，计算方法：选前40~100天总成交金额作为流通市值的一种替代。?
    Ana:1，行业角度，首先假设公司全部业务属于本行业(如果不是那么理论上有问题)应该用预测(假设预测准确)的收入或者利润
        对应行业内权重。例子：17Q4，苹果的iphone在智能手机市场收入占比,18%，但利润占比87%；但苹果公司业务中58%收
        入是iphone，估计利润占比要显著超过58%。
        2，理论上更合理的方式可能是对非本行业的估值与本行业估值比较计算换算系数，将公司模拟成一个只有本行业业务的主体
        3，数据限制的情况下，使用净利润及其预期增速进行行业内加权，可能是更合理的策略。
    todo，抓取分季度的财务数据，净利润等用于。

3，notes：如果财务数据q1q2q3q4的披露时间：time,最早，最晚,delay_max|days
    Q1,4-1,4-30,30
    Q2,7-1,9-1,60
    Q3,10-1,10-30,30
    Q4,1-1，4-30,120
2.1，财务数据管理：抓取季度财务数据，
file:tb_finance专业财务.csv
path:D:\db_dzh_dfw
Choice财务数据模板：C:\Eastmoney\Choice\Office\Template\Excel\东财财务估值模板
source 20180108_方正证券_金融工程专题_韩振国_【方正金工，专题】“星火”多因子系列报告（一）：Barra模型初探：A股市场风格解析
'''
'''
self 行业中性化：样本空间内，
1，搜集所有行业及行业内个股；
2，用财务指标确定行业内价值最大和成长最优质的股票作为锚；
(例如2017年的万华化学在wind三级行业中：价值角度：净利润111，其他公司合计157，本公司的行业占比41.4%；成长角度：净利润增长率+200%,其他公司平均低于30%。)
3，设置基于锚的算法，对个股进行相对内在价值估计。|
3.0，Qs：这样的话，行业因子会不会变成实质上的风格因子？Ans：我们认为不会，因为现有的风格factors\价值类,成长类因子都是相对比例型的指标，如PE，PB,roe,roa,现金流增长率，净利润增长率等；而用市值和净利润等绝对值类型指标来计算权重，是分组内部合理的计算权重方式。
3.1，假设锚股票内在价格1，企业价值1亿=内在价格*股本，计算其他股票企业价值，根据企业价值在行业内计算企业价值权重，得到在行业内的因子暴露
3.2，对于全市场行业，方案一是用锚的个股(1个个股同时代表价值和成长，或2个个股分别代表价值和成长，甚至还可以更多的个股来代表主营业务特别或者个股比较特别的情况。)直接代替行业，对各个行业的锚个股计算企业价值，并进行加权计算行业内的因子暴露；方案二是用锚的个股计算出行业的数值。
test：
t1，根据我们的行业因子，与市场指数，市场行业指数比较收益率和波动率特征，信息率等。
t2，参考zxjt，计算各个行业因子的收益率年化波动率，显著月份占比，对于市场的R^2增量。
4，performance：纯行业因子 年化超额收益率% 年化波动率% 信息比例 |T|平均值 |T|>2 占比（%）

assumptions:1,不同行业都存在强者恒强的情况，一方面龙头股经营收入和利润的市场占比持续提升，另一方面龙头股的市值行业占比在发展初期低于经营收入和利润占比，但后期趋同。
todo 模块计算步骤： 
1，数据准备：准备历年需要的财务数据，形成本模块/app需要的indicators。Keys：今年净利润和收入等
2，算法计算：计算分行业个股内在价值
3, 从Basic Info模块抓取Wind行业数据(4档，但一般用3档)
...

3，计算 市场组合(如中证全指或者自建)与某因子组合的月收益率差值
4，观察分行业的超额收益情况。
idea：不对因子有效性做强假设，专注于最优策略的流程发现。
'''
### sub-step 1 Load industry file 
# path: C:\zd_zxjtzq\RC_trashes\temp\sys_stra_24h\CISS_rc\db\db_assets
# logic: load ind. file from db_assets, match symbol list with ind. file 
from db.basics import industry
industry = industry()
ind_raw = industry.load_wind_ind('')
print("industry data from Wind-API: \n")
# Qs UnicodeEncodeError: 'charmap' codec can't encode characters in position
# Ans : cmd不能很好地兼容utf8,改一下编码，比如我换成“gb18030”，就能正常显示
print( ind_raw.head(5) )
### sub-step 2 get symbol lists for periods 
# for periods 13Q1 to 18Q2,
## ss2.1, get collection of symbol lists from CSI300+800+1000, 
# derived from Get_json_groupdata and Load_funda_csvJson | stocks_funda_wash
# object: temp_dataG.datagroup[wind_code+'_'+temp_date] = data_json_rc.file_csv
# C:\zd_zxjtzq\RC_trashes\temp\keti_sys_stra_24h\p1\wind_data
# path_ss2 = "C:\\zd_zxjtzq\\RC_trashes\\temp\\keti_sys_stra_24h\\p1\\wind_data"
# temp_file = "Wind_000300.SH_2013-05-31_20180923.csv"
# 对于 temp_list['code'], 抓取对应的wind三级行业,最近年度净利润，总收入 。
    # reference: 之前筛选行业龙头的策略。 如果年化计算的财务指标=1季报值/去年(季报/年报) 
    # load tb_finance_financeData.csv and tb_finance_capital.csv from D:\\db_dzh_dfw

### sub-step 3 fundamental financial and capital data
import pandas as pd 
path_db_dzh = "D:\\db_dzh_dfw\\"
file_tb_finance_finance = "tb_finance_financeData.csv"
file_tb_finance_capital = "tb_finance_capital.csv"
print("Loading financia data and capital data. ")
df_tb_fi_fi = pd.read_csv(path_db_dzh+file_tb_finance_finance, encoding="GBK",sep=",",low_memory=False)
# index 71004 | columns 245 | 
print('=========================================')
print("data head,sum of df_tb_fi_fi ")
print(df_tb_fi_fi.info() ) # 245 columns
print(df_tb_fi_fi.head() )

df_tb_fi_cap = pd.read_csv(path_db_dzh+file_tb_finance_capital, encoding="GBK",sep=",",low_memory=False )
# index 91935 | columns 20 | objects |phd 
# 证券代码,时间,股份总数,无限售股份合计,A股,B股,境外上市外资股,其他流通股份,限售股
# 份合计,国家持股,国有法人持股,境内法人持股,境内自然人持股,其他发起人股份,募集法人股份,境外法人持股,
# 境外自然人持股,内部职工股,优先股或其他,变动原因,变动原因2    91935 non-null object
# Ana:last column "变动原因",存在34个cell内有","分隔符，pandas会认为是多了一列(21th)，但首行columns
# 只有20个;用 header=0 也不解决问题
# pandas.errors.ParserError: Error tokenizing data. C error: Expected 20 fields in line 905, saw 21 
# Ana # 用了sep="\s+"后，出错line从901变成了line 55553 ;加了delimiter="\t"
# Ans: open csv file, add "变动原因2" as 21th column at "U1"
print("data head,sum of df_tb_fi_cap ")
print(df_tb_fi_cap.info() )
print(df_tb_fi_cap.head() )
print('=========================================')
# Ana：“时间”在日的级别上可能是{24,19,11,31，...}，但年和月似乎是稳定的
# format in loaded df:1990/9/24 0:00 || format in csv:1990-12-31 00:00:00
# python中.split()只能用指定一个分隔符;多个分隔符可以用re模块
import re
# re.split("[/,]",temp_date) || ['1990', '9', '24 0:00']
# re.split("[/ ]",temp_date) || ['1990', '9', '24', '0:00']
# 由于股本变动的时间变动不定，因此需要抓取对应的数据
# todo 为了缓解过大df带来的算法效率问题，可以考虑分代码将其拆分成df_code

### sub-step 3 match financial indicator into industry scale 
# 从13年开始，载入"2013-05-31"的股票代码300+500，逐个抓取13Q1-18Q2的财务数据
''' Create new pd to save analytical indicators
input：wind_code = '000300.SH',temp_date = '2014-05-31' 
    temp_list = dataG.datagroup[wind_code+'_'+temp_date]
    df_tb_fi_fi
    df_tb_fi_cap
algo: 1, all 
    
'''
### ss3.1, get all sample space codes from CSI300,500,CSI1000
temp_date = '2016-05-31'
# 看看样本空间里dataGroup 有啥好东西。
temp_list = pd.DataFrame()
for temp_i in dataG.index_list:
    # ['000300.SH', '000905.SH', '000852.SH'] 
    temp_list_sub = dataG.datagroup[temp_i+'_'+temp_date]
    temp_list = pd.concat([temp_list,temp_list_sub],ignore_index=False)
    #there are duplicate indexes, so we need to re-sort index here
    temp_list= temp_list.reset_index()
    print( temp_list.head() )
    temp_list= temp_list.drop(['index'] ,axis=1 ) 
    # print(temp_list.info() )
    temp_list.to_csv("D:\\temp_list_181025.csv")


# get code df in df_tb_fi_fi
# Notes："净利润"项目下，只有半年和年末数据，1,3季度净利润数值是0。应该使用"净利润（不含少数股东损益）"
#   收入应该使用 "一.营业收入"
#   经营性现金流 "经营活动产生的现金流量净额"
# 5-31时可以获得前一年Q4和本年Q1财务数据，11-30可以获得本年Q3,Q2的财务数据
# match_date = [['05-31','12-31','03-31'],['11-30','09-30','06-30'] ]
# df_match_date = pd.DataFrame(match_date, columns=["date_raw","date_f1","date_f2"])
# date_f1 = df_match_date[df_match_date["date_raw"]== temp_date[-5:] ].loc[0,'date_f1']
# date_f2 = df_match_date[df_match_date["date_raw"]== temp_date[-5:] ].loc[0,'date_f2']

yymmdd = re.split('-',temp_date )
# From date for symbol list to date for fiscal quarter # "2013-03-31"  
# Notes:要用到前一年数据，但是财务数据从2013年开始的，2013年无法取得2012年的数据！！ 
if temp_date[-5:] == "05-31" :
    date_list = [str(int(yymmdd[0])-1)+'-12-31', yymmdd[0]+'-03-31',str(int(yymmdd[0])-1)+'-03-31']
elif temp_date[-5:] == "11-30" :
    date_list = [ yymmdd[0]+'-09-30', str(int(yymmdd[0])-1)+'-09-30',str(int(yymmdd[0])-1)+'-12-31' ]

## put these values to a big table 
# 计算日期属于哪个季度！！！ 对财务数据进行年化转换 | notes:现在都是假定财务数据在最晚时间统一
# 获得，但是实际上财务数据的披露可以通过 业绩预告，真实披露日期等数据更高效地获得，提高信息传递效率 。
for temp_i in temp_list.index :
    temp_code = temp_list.loc[temp_i,'code']
    # code2 = "SH600036" # notes: different code types in df_tb_fi_fi["证券代码"]
    # print('181025======temp_code :',type(temp_code),temp_code)
    # print(type(temp_code[-2:]),type(temp_code[:6]))
    # print('====================')
    # print('temp_code ',temp_code, '\n')
    code2 = temp_code[-2:] + temp_code[:6] 
    # print("code2  ",code2)
    if df_tb_fi_fi[df_tb_fi_fi["证券代码"]== code2 ].any().any() :
        df_code = df_tb_fi_fi[ df_tb_fi_fi["证券代码"]== code2 ] 
        # get latest dates before given date from df_code
        #   trasform from string to datetime 

        df_code.loc[:,"date"] = df_code.loc[:,"时间"] 
        # from string to DatetimeIndex(['2013-03-31',...
        df_code["date"] = df_code["date"].apply( pd.to_datetime ) 
        # print("=====df_code[时间]==181025")
         
        temp_index1 = df_code[ df_code["date"] == date_list[0] ].index 
        temp_index2 = df_code[ df_code["date"] == date_list[1] ].index
        temp_index3 = df_code[ df_code["date"] == date_list[2] ].index
        # 根据 Q1,2,3,4的不同计算 注意：此处的q3数据对应当年前3季度之和，而不是分季度的，
        # q4对应当年全年的财务数据之和
        # df_code.index might be empty for 000562.SZ
        ## Initialize var. and para 参数初始化！由于股票缺少Q1或Q3的财务数据，要做好初始化的默认设置
        #   例：603589.SH have no records for 2015-03! 次新股。 

        if temp_date[-5:] == "05-31" and len(df_code.index) >0  :
            # Q4_pre, Q1, Q1_pre
            # just one date match | TypeError: cannot convert the series to <class 'float'>
            # print( " df_code.loc[temp_index1 ,净利润（不含少数股东损益）:" )
            # print( df_code.loc[temp_index1 , "净利润（不含少数股东损益）"]  )
            profit_q4_pre =  float(df_code.loc[temp_index1 , "净利润（不含少数股东损益）"].values )
            revenue_q4_pre = float(df_code.loc[temp_index1, "一.营业收入"].values )
            cf_oper_q4_pre = float(df_code.loc[temp_index1 , "经营活动产生的现金流量净额"].values )
            try :                
                profit_q1 =  float(df_code.loc[temp_index2 , "净利润（不含少数股东损益）"].values )
                revenue_q1 = float(df_code.loc[temp_index2, "一.营业收入"].values )
                cf_oper_q1 = float(df_code.loc[temp_index2 , "经营活动产生的现金流量净额"].values )
                profit_q1_pre =  float(df_code.loc[temp_index3 , "净利润（不含少数股东损益）"].values )
                revenue_q1_pre = float(df_code.loc[temp_index3 , "一.营业收入"].values )
                cf_oper_q1_pre = float(df_code.loc[temp_index3 , "经营活动产生的现金流量净额"].values )
                # synthesis profit,revenue and cash flow 
                # 1,计算去年1季度的全年占比
                if profit_q1_pre != 0 :
                    profit_q1_yoy = profit_q1/profit_q1_pre # yoy
                else :
                    profit_q1_yoy = 1
                profit_q1_pre_pct = profit_q1_pre/profit_q4_pre
                para_fi_max = 0.35 # for q1 
                para_fi_1 = min(profit_q1_pre_pct,para_fi_max )
                profit_q4_es = profit_q1 + (profit_q4_pre-profit_q1_pre)*((1-para_fi_1)+para_fi_1*profit_q1_yoy)
                
                if revenue_q1_pre !=0 :
                    revenue_q1_yoy = revenue_q1/revenue_q1_pre
                else :
                    revenue_q1_yoy =1
                revenue_q1_pre_pct = revenue_q1_pre/revenue_q4_pre
                para_fi_2 = min(revenue_q1_pre_pct,para_fi_max )
                revenue_q4_es = revenue_q1 + (revenue_q4_pre-revenue_q1_pre)*((1-para_fi_2)+para_fi_2*revenue_q1_yoy)

                if cf_oper_q1_pre != 0 :
                    cf_oper_q1_yoy = cf_oper_q1/cf_oper_q1_pre
                else :
                    cf_oper_q1_yoy = 1
                cf_oper_q1_pre_pct = cf_oper_q1_pre/cf_oper_q4_pre
                para_fi_3 = min(cf_oper_q1_pre_pct,para_fi_max )
                cf_oper_q4_es = cf_oper_q1 + (cf_oper_q4_pre-cf_oper_q1_pre)*((1-para_fi_3)+para_fi_3*cf_oper_q1_yoy)
                     
            except:
                # 603589.SH have no records for 2015-03! 次新股。
                # cautiously estimate current year/pre_year = 95% 
                print('There are missing quarterly finance records:')
                profit_q4_es = profit_q4_pre*0.95
                revenue_q4_es = revenue_q4_pre*0.95
                cf_oper_q4_es = cf_oper_q4_pre *0.95 
            
        elif temp_date[-5:] == "11-30" :
            # Q3, Q3_pre, Q4_pre,
            profit_q4_pre =  float(df_code.loc[temp_index3 , "净利润（不含少数股东损益）"].values )
            revenue_q4_pre = float(df_code.loc[temp_index3 , "一.营业收入"].values )
            cf_oper_q4_pre = float(df_code.loc[temp_index3 , "经营活动产生的现金流量净额"].values )

            try :
                profit_q3 =  float(df_code.loc[temp_index1 , "净利润（不含少数股东损益）"].values )
                revenue_q3 = float(df_code.loc[temp_index1, "一.营业收入"].values )
                cf_oper_q3 = float(df_code.loc[temp_index1 , "经营活动产生的现金流量净额"].values )
                profit_q3_pre =  float(df_code.loc[temp_index2 , "净利润（不含少数股东损益）"].values )
                revenue_q3_pre = float(df_code.loc[temp_index2, "一.营业收入"].values )
                cf_oper_q3_pre = float(df_code.loc[temp_index2 , "经营活动产生的现金流量净额"].values )
                
                # synthesis profit,revenue and cash flow 
                # 1,计算今年3季度的全年占比
                profit_q3_yoy = profit_q3/profit_q3_pre # yoy
                profit_q3_pre_pct = profit_q3_pre/profit_q4_pre
                para_fi_max = 0.75 # for q3
                para_fi_1 = min(profit_q3_pre_pct,para_fi_max)
                profit_q4_es = profit_q3 + (profit_q4_pre-profit_q3_pre)*((1-para_fi_1)+para_fi_1*profit_q3_yoy)
                
                revenue_q3_yoy = revenue_q3/revenue_q3_pre
                revenue_q3_pre_pct = revenue_q3_pre/revenue_q4_pre
                para_fi_2 = min(revenue_q3_pre_pct,para_fi_max)
                revenue_q4_es = revenue_q3 + (revenue_q4_pre-revenue_q3_pre)*((1-para_fi_2)+para_fi_2*revenue_q3_yoy)

                cf_oper_q3_yoy = cf_oper_q3/cf_oper_q3_pre
                cf_oper_q3_pre_pct = cf_oper_q3_pre/cf_oper_q4_pre
                para_fi_3 = min(cf_oper_q3_pre_pct,para_fi_max)
                cf_oper_q4_es = cf_oper_q3 + (cf_oper_q4_pre-cf_oper_q3_pre)*((1-para_fi_3)+para_fi_3*cf_oper_q3_yoy)
            except :
                # cautiously estimate current year/pre_year = 95% 
                profit_q4_es = profit_q4_pre*0.95
                revenue_q4_es = revenue_q4_pre*0.95
                cf_oper_q4_es = cf_oper_q4_pre *0.95 
        # 
        temp_list.loc[temp_i, "profit_q4_es" ] = profit_q4_es
        temp_list.loc[temp_i, "revenue_q4_es" ] = revenue_q4_es
        temp_list.loc[temp_i, "cf_oper_q4_es" ] = cf_oper_q4_es

temp_list.to_csv("D:\\temp_list_1026.csv")
### sub-step 3 get anchor stocks for every sector,sub-sector,industry
# 财务指标确定行业内价值最大和成长最优质的股票作为锚
## ss3.1, 提取个股的 1~3级行业代码
## ss3.2, 根据 1~3级行业，groupby获取样本内行业的【利润，收入，经营现金流金额】之和
    # notes:样本内个股不代表本细分行业的所有上市个股，更不代表经营环境中的公司

# calculate columns from ind1 to ind3 from temp_list['industry_gicscode']
# reference data: ind_raw = industry.load_wind_ind('') from line 165
# temp_df = ind_raw.drop_duplicates(subset=['ind3_code'], keep="first" )
for index01 in temp_list.index :
    # get industry code from 1 to 3 
    # notes: there might still be duplicate index 
    str1 = temp_list.loc[index01, "industry_gicscode"]
    temp_list.loc[index01,'ind1_code'] = str(str1)[:2]
    temp_list.loc[index01,'ind2_code'] = str(str1)[:4]
    temp_list.loc[index01,'ind3_code'] = str(str1)[:6]
# temp_INDS | [61 rows x 3 columns]
# notes: temp_INDS has no multiindex, RangeIndex(start=0, stop=61, step=1)
# columns= Index(['ind1_code', 'ind2_code', 'ind3_code', 'profit_q4_es', 'revenue_q4_es',
    #   'cf_oper_q4_es'], dtype='object')
# ===========================
# >>> temp_INDS_sum.index
# MultiIndex(levels=[['10', '15', '20', '25', '30', '35', '40', '45', '50', '55'
# '60'], ['1010', '1510', '2010', '2020', '2030', '2510', '2520', '2530', '2540'
#            labels=[[0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
# 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 6, 6, 6, 6,
#  names=['ind1_code', 'ind2_code', 'ind3_code'])

temp_INDS_sum=temp_list.groupby(["ind1_code","ind2_code","ind3_code"])["profit_q4_es","revenue_q4_es","cf_oper_q4_es"].sum()
temp_INDS_sum.to_csv("D:\\temp_INDS_sum_1026.csv")
## ss3.3, 按照算法计算锚定价格。
# 目标：计算每个ind3的样本内锚anchor，作为新增columns放在temp_INDS_sum里
#3 ss3.31, 定位，计算锚的基准数值
# todo compare 个股的pecentage with maximum one in temp_INDS_sum['','profit_pct_max']
# if current stock has larger percentage number, put it as the best value items.
df_ind3_sum = temp_INDS_sum.xs(["str_ind1","str_ind2"],level=["ind1_code","ind2_code"])
for temp_i3 in df_ind3_sum.index :
    str_ind3 = df_ind3_sum.loc[temp_i3,"ind3_code" ]
    print("Working on industry level3: ", str_ind3)
    

    
asd
# p582, pandas.pdf







## ss3.32 根据锚的数值，计算个股的理论价格。
for index01 in temp_list.index :
    # index01 = temp_list.index.values[0]
    # temp_code = "600036.SH" 
    temp_code = temp_list.loc[index01,'code']
    print("Calculating code: ",temp_code )
    # only one row for code in temp_list
    # get industry-3 code for given code 
    str_ind3 = temp_list.loc[index01, "ind3_code"]
    print('=============df_ind3')
    print( type(str_ind3),str_ind3 )
    print( temp_INDS_sum.index ) 
    print( temp_INDS_sum.columns ) # Index(['profit_q4_es', 'revenue_q4_es', 'cf_oper_q4_es']
    # get summary values of financial indicators from industry list
    # todo if error for int|string, then using str(str_ind3)
    # 多重索引的dataframe取值一般使用xs,可以传入多个不同级别的索引进行筛选,但不支持同一级索引多选并且xs返回的是数值而不是引用
    # temp_INDS_sum.xs(['10','1010'],level=["ind1_code","ind2_code"]).loc['101010','profit_q4_es']
    str_ind1 = str_ind3[:2]
    str_ind2 = str_ind3[:4]
    # temp_INDS_sum.xs(['10','1010'],level=["ind1_code","ind2_code"]).loc['101010','profit_q4_es']
    profit_total_ind3 = temp_INDS_sum.xs([str_ind1,str_ind2],level=["ind1_code","ind2_code"]).loc[str_ind3,'profit_q4_es']
    revenue_total_ind3 = temp_INDS_sum.xs([str_ind1,str_ind2],level=["ind1_code","ind2_code"]).loc[str_ind3,'revenue_q4_es']
    cf_oper_total_ind3 = temp_INDS_sum.xs([str_ind1,str_ind2],level=["ind1_code","ind2_code"]).loc[str_ind3,'cf_oper_q4_es']
    # df_ind3 =temp_INDS_sum[ temp_INDS_sum['ind3_code']== int(str_ind3) ]

    try:
        temp_list.loc[index01,'ind3_pct_profit_q4_es'] =temp_list.loc[index01,'profit_q4_es']/profit_total_ind3
        temp_list.loc[index01,'ind3_pct_revenue_q4_es'] =temp_list.loc[index01,'revenue_q4_es']/revenue_total_ind3
        temp_list.loc[index01,'ind3_pct_cf_oper_q4_es'] =temp_list.loc[index01,'cf_oper_q4_es']/cf_oper_total_ind3 

        
        





    except:
        print('=============df_ind3')
        print( df_ind3 )
        print( type(temp_list.loc[index01,"profit_q4_es"].values[0]) )
        print( type(temp_INDS_sum.loc[df_ind3.index,"profit_q4_es"].values[0]) )
        print("====================")
        print(profit_total_ind3,revenue_total_ind3,cf_oper_total_ind3  )
    



       
temp_list.to_csv("D:\\temp_list2_1026.csv")

## ss3.3, get anchor stocks for value and growth perspectives
# notes:1,行业内weights要标准化处理，剔除负值后取95%总权重加权，剩余5%分给负值的个股
# get list of ind3 from temp_INDS_sum or ind_raw
ind3_list = temp_list['ind3_code'].drop_duplicates()
ind3_list.to_csv("D:\\temp_list2_1027.csv")

# todo create class and modules to set such information. 


 
  









# sub-step 1
# sub-step 1
# sub-step 1
# sub-step 1




























'''
todo 
1,developing indicators
1.1, expected P/E in future 2 years | need (total_shares*price)/e_expected

2,历史每日市场数据应用，特别是关注wind的复权因子数据。
Wind_Input\Wind_all_A_Stocks_wind_170814_updated 


# Alpha因子和风险/系统/Beta因子评定：1，数值稳定且与其他因子相关性低
#2，因子价格变动部分的收益或风险角度价值，
#3，因子收益率显著性，每期个股收益率和因子暴露值回归，看平均值是否显著，显著月份占比
#4，看新增因子是否增加了信息价值。(从短期效应的角度，也许某一年某因子实现了价值，
# 在下一年度投资者仍然会使用该因子，直到投资结果持续不佳或者精确计量该因子效应的数据出现。)
# 数据区间：zxjt 用的数据时 2008-2018
# 难点1：而非线性规模因子则主要强调的是市值中等的股票，计算方法为规模因子的立方,然后和规模因子进行施密特正
#   交化处理去掉其共线性的部分，但非线性规模因子由于构造复杂超额收益一般较难获得。 
# 难点2：Growth 成长因子缺失值较多，多空收益计算误差较大，因此在这里没有加入对比
# 变化：值得注意的是规模因子，该因子在 2017 年之前一直被普遍认定为 Alpha 因子，A 股市场的小盘股溢价效应非常明显，但最近两
# 年大盘股的重新崛起和风格切换使得该风格因子的波动率急剧提升（年化波动率已经上升到 5%），风险属性逐
# 步增强，因此规模因子作为风险因子已经被大多数投资者所认可。
# source 20180830_中信建投_金融工程专题_丁鲁明_Barra风险模型介绍及与中信建投选股体系的比较
# Ana：传统行业因子按照市值加权中性化，我们考虑按照过去40-100天总成交金额中性化
#   逻辑是交易的角度，市场深度可能比流通市值更能反映一段时间内市场交易对手的交易意愿，
#   当然从数据处理的角度，也更容易获得每日成交金额；流动市值中很容易存在显著比例仓位不会出现交易的情况。



5，基准构建策略中的权重策略，zxjt用了行业内市值加权或等权，指数内行业市值加权/等权，衍生方面还对5挡股票的后20%做空
 
# idea 所有现有的模型方法都可以作为一个模块，在其之上进行加强。
idea db的角度，能不能建立一个开源的基本面数据库，有统一的数据标准。
'''