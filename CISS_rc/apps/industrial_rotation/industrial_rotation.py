# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
todo: 

功能： 
1，基于ABM模型的指标计算行业轮动策略的大类因子。

last update   | since 191211

信息：

0，基础信息：代码、1、2级行业分类							
	股票代码	一级行业代码	二级行业代码				
1，基础财务数据：直接打分方式：							
	一致预测ROE(FY2)	一致预测净利润2年复合增长率	长期股权投资	一致预测营业利润2年复合增长率	总资产周转率(TTM)		
2，3个ABM核心指标及变动：9个直接打分							
	当年净利润预测	当年收入预测	当年经营性现金流预测	当年净利润增长值	当年收入增长值	当年经营性现金流增长值	当年净利润增长率
3，相对打分1：找出价值靠近价值锚、成长靠近成长锚的 
4，相对打分2：判断行业整体是否向好；行业内的集中度-越高越利于大公司，越低越利于小公司

todo：对以上4个环节分别建模，观察不同时期每个大类的绩效，最后形成加权优化的方案。

/ 
===============================================
'''
import pandas as pd
import numpy as np
import json
import time
import datetime as dt
import os
import math

class ind_rotation():
    # 类的初始化操作
    def __init__(self):
        
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

        self.path_abm = "D:\\CISS_db\\Active_Benchmark_Model_2019\\abm_weights\\"

        self.path_output = "D:\\CISS_db\\csi800_industrial_rotation\\"

        self.file_return = "return_data_0531_1130.csv"
        
        # ### 导入季度收益数据
        # df_return = pd.read_csv( self.path_output+  self.file_return  ) 
        # print("df_return "  )
        # print( df_return.head() )

        ### get list of ind1 
        # file_date = "weights_"+"20140531" + "_1.csv"
        # df_abm = pd.read_csv( self.path_abm + file_date ) 
        # df_abm_des = df_abm.describe() 
        # self.list_ind1 = df_abm["ind1_code"].drop_duplicates().values
        self.list_ind1 = ["40","60","20","15","30","45","25","35","55","10","50"]
        print("List of industry level=1 ")
        print( self.list_ind1 )
        ###
        self.nan = np.nan 

    def print_info(self):
        ### Init...
        print("Initialization ...")
        print("path_abm:", self.path_abm )
        print( "path_output:" ,self.path_output) 

        ### print all modules for current clss
        print("get_score_mkt |FUNCTION 标准方程：根据指标计算行业内的标准化得分、根据标准分计算指标权重  ")


        print("get_score_ind1 |根据指标计算行业内的标准化得分 ")
        print("get_weight | 根据标准分计算指标权重")
        print("test_score_weight_return_list_indicator |CHOICE 1 :对indicator list 分别计算df_score")
        print("test_score_weight_return_sum |CHOICE2:将所有指标打分加总 ,所有的indicator分数和sum保存在 df_score_sum  ")
        return 1 


    def get_score_mkt(self,df_abm,df_score,temp_col ) :
        #################################################################################
        ### FUNCTION 标准方程：根据指标计算行业内的标准化得分、根据标准分计算指标权重
        '''score 计算方法：
        1，对样本内全部数据，计算均值、方差，最大值最小值，取2倍标准差，若超过则对于标准化0~1的分值+0.1或-0.1作为惩罚
        2,正太分布1~3个标准差对应的概率分布：68.27%、95.45%、99.74%
        notes:
        1,df_abm和df_score 应该有相同的index；
        '''
        df_abm_des = df_abm.describe()  

        # index of df_abm_des | Index(['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max'], dtype='object')
        ### 对给定指标temp_col,
        temp_mean = df_abm_des.loc["mean", temp_col ]
        temp_std  = df_abm_des.loc["std", temp_col ]
        temp_min  = df_abm_des.loc["min", temp_col ]
        temp_max  = df_abm_des.loc["max", temp_col ]        

        if temp_max - temp_min == 0.0 :
            # prevent cannot devided by zero error . 
            df_score.loc[df_abm.index ,  temp_col+"_mkt"] = 0.0  
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


    def get_score_ind1(self,df_abm,df_score,temp_col,para_dict ) :
        #################################################################################
        ###  根据指标计算行业内的标准化得分
        '''score 计算方法：
        1，对每个一级行业的列数据，计算均值、方差，最大值最小值，取2倍标准差，若超过则对于标准化0~1的分值+0.1或-0.1作为惩罚
        2,正太分布1~3个标准差对应的概率分布：68.27%、95.45%、99.74%

        last 191213 | since 191207
        notes:
        1,df_abm和df_score 应该有相同的index；

        '''
        if "ind_level" in para_dict :
            indX_code = "ind"+ str(para_dict["ind_level"] ) +"_code"
            ind_level = str(para_dict["ind_level"] )
        else :
            indX_code = "ind1_code"
            ind_level = "1" 

        list_indX = df_abm[ indX_code ].drop_duplicates().values
        # list_ind1  [40 60 20 15 30 45 25 35 55 10 50]
        print("list_indX ", list_indX)
        for temp_indX in list_indX :
            df_abm_sub = df_abm[ df_abm[indX_code]== temp_indX ]
            df_abm_sub_des = df_abm_sub.describe() 
            
            # index of df_abm_des | Index(['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max'], dtype='object')
            ### 对给定指标temp_col,
            temp_mean = df_abm_sub_des.loc["mean", temp_col ]
            temp_std  = df_abm_sub_des.loc["std", temp_col ]
            temp_min  = df_abm_sub_des.loc["min", temp_col ]
            temp_max  = df_abm_sub_des.loc["max", temp_col ]        

            if temp_max - temp_min == 0.0 :
                # prevent cannot devided by zero error . 
                df_score.loc[df_abm_sub.index ,  temp_col+"_ind_"+ind_level] = 0.0 

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

                    df_score.loc[temp_i,temp_col+"_ind_"+ind_level] = temp_score

        return df_score 

    
    def get_weight(self,df_weight,df_score,temp_col,list_indX ,para_dict ) :
        #################################################################################
        ### 根据标准分计算指标权重
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

        working columns：[temp_col+"_ind_"+ind_level, temp_col+"_mkt" ]
        notes:
        1,df_abm和df_score 应该有相同的index；
        2,para_dict 里包括了行业数量的选取和股票数量的选取
        '''
        if "ind_level" in para_dict :
            indX_code = "ind"+ str(para_dict["ind_level"] ) +"_code"
            ind_level = str(para_dict["ind_level"] )
        else :
            indX_code = "ind1_code"
            ind_level = "1" 

        ### 设置单一行业权重上限w_max_indX ：
        w_max_indX = 1.0
        
        if "w_max_indX" in para_dict :
            w_max_indX = float( para_dict["w_max_indX"] ) 

        num_indX = int( para_dict["para_indnumber"] ) # = 5  
        num_stock = int(para_dict["para_stocknumber"])  # = 100  
        
        df_describe = df_score.describe()
        print("df_describe ", df_describe  )
        ### 1,全市场：按行业计算平均得分,剔除排名靠后的4个行业后计算行业配置权重
        df_score_indX = df_score.groupby(indX_code).mean() 
        df_score_indX = df_score_indX.sort_values(by=temp_col+"_mkt",ascending=False)
        ### top 7 and last 4 indX 
        list_indX_keep = df_score_indX.index[:num_indX].values
        list_indX_drop = df_score_indX.index[num_indX:].values
        print("list_indX_drop",list_indX_drop)

        temp_df= df_score[ df_score[indX_code].isin( list_indX_drop )  ]
        temp_index =temp_df.index
        print( "temp_index ",temp_index )
        df_score.loc[temp_index , temp_col+"_mkt"] = 0.00

        df_weight[temp_col+"_mkt"] = df_score[temp_col+"_mkt"]/df_score[temp_col+"_mkt"].sum()

        ### 2, 对于每个行业,计算指标得分前20%的股票
        stock_pct_ind = 0.3
        list_index =[]
        for temp_indX in list_indX : 
            df_score_sub = df_score[ df_score[indX_code]==temp_indX ]
            temp_num = len( df_score_sub.index )
            temp_num2 = max(1,math.floor(temp_num* stock_pct_ind ) ) 
            ### 取行业内分数排名前20%的股票
            df_score_sub = df_score_sub.sort_values(by=temp_col+"_mkt",ascending=False)        
            list_index = list_index + list( df_score_sub.index[:temp_num2] ) 
            # print("list_index ",len(list_index)  )

        list_index = sorted(list_index) 

        # df_weight.loc[list_index, temp_col+"_ind_"+ind_level] =  df_score.loc[list_index, temp_col+"_ind_"+ind_level]
        df_weight[ temp_col+"_ind_"+ind_level] =  df_score[temp_col+"_ind_"+ind_level]

        df_weight[ temp_col+"_ind_"+ind_level] = df_weight[ temp_col+"_ind_"+ind_level].fillna(value=0.0)
        
        df_weight[ temp_col+"_ind_"+ind_level] = df_weight[temp_col+"_ind_"+ind_level]/df_weight[temp_col+"_ind_"+ind_level].sum()
        
        ### 3，对市场得分和行业得分进行加权，最后排序选择前150只股票，并剔除权重低于0.3%的股票
        df_weight[ temp_col ] = df_weight[temp_col+"_ind_"+ind_level]*0.5  +df_weight[temp_col+"_mkt"] *0.5

        ### Ana:合并后个股权重大于0.3%的有105个，对应9个行业，可以只选前5个行业
        list_indX_top5 = df_weight[indX_code].drop_duplicates().values[:num_indX]
        list_indX_drop = df_weight[indX_code].drop_duplicates().values[num_indX:] 

        ### 剔除需要剔除的行业股票
        list_index_drop = df_weight[ df_weight[indX_code].isin( list_indX_drop )  ].index
        df_weight.loc[list_index_drop , temp_col  ] = 0.0 
        df_weight[ temp_col ] = df_weight[temp_col]/df_weight[temp_col].sum()

        # 剔除行业权重小于0( 0.05% )的行业 | 除了50电信行业股票数量较小，其他行业股票这时有26~160个不等。
        temp_df_sum = df_weight.groupby(indX_code).sum()
        temp_df_sum = temp_df_sum[ temp_df_sum[temp_col] > 0.0005 ] 
        temp_df_sum = temp_df_sum.sort_values(by=temp_col,ascending= False)

        
        ########################################################
        ### 对每个行业内个股权重和的上限进行控制。
        '''若行业abcd的权重分别为 40%，30%，20%，10%，单一行业最大权重限制为25%，则需要首先判定权重最大的
        行业进行改造。
        1,首先判断行业权重合和行业权重上限的最小权重和
        2，判断最小权重和剩余可用权重额度的更小值

        '''
        # 中判断字典中是否存在某个键
        if "w_max_indX" in para_dict  :
            df_w_buffer = pd.DataFrame(index= list_indX_top5 ,columns=["w_buffer"] )
            weight_quota = 1.0 
            for temp_indX in list_indX_top5 :
                # print("Working on industry ", temp_indX )
                temp_weight = df_weight [df_weight[indX_code] == temp_indX ]
                temp_w_sum = temp_weight[temp_col ].sum()
                # print("weight sum indX ", temp_w_sum  ) 
                temp_pct = para_dict["w_max_indX"]/temp_w_sum
                ### 若行业权重合超过上限，则等比例缩减为上限：
                if temp_w_sum >  para_dict["w_max_indX"] :
                    df_weight.loc[temp_weight.index, temp_col ]=df_weight.loc[temp_weight.index, temp_col ]*temp_pct
                    weight_quota = weight_quota - para_dict["w_max_indX"] 
                    df_w_buffer = df_w_buffer.drop(temp_indX, axis=0 )  
                else :
                    weight_quota = weight_quota - temp_w_sum 
                    df_w_buffer.loc[temp_indX,"w_buffer"] =  para_dict["w_max_indX"]  - temp_w_sum 
                
            
            ### 若因行业权重过大，缩小后空出了权重，则按顺序分配给未打满权重的行业
            ### 按照距离行业权重上限从小到大顺序，把weight_quota 分配给每个行业
            df_w_buffer =df_w_buffer.sort_values(by = "w_buffer" )
            print( "df_w_buffer 可分配的组合权重和行业权重空间", weight_quota )
            print( df_w_buffer  )
            for temp_indX in df_w_buffer.index  :
                if weight_quota > 0.0005 :
                    w_diff  = min( df_w_buffer.loc[temp_indX,"w_buffer"] ,weight_quota)
                    temp_df = df_weight[ df_weight[indX_code]== temp_indX ]
                    w_new = w_diff + df_weight.loc[temp_df.index, temp_col ].sum()
                    temp_pct = w_new / df_weight.loc[temp_df.index, temp_col ].sum()
                    df_weight.loc[temp_df.index, temp_col ]=df_weight.loc[temp_df.index, temp_col ]*temp_pct
                    # update weight_quota 
                    weight_quota =weight_quota - w_diff
            
        ########################################################
        ### 分行业对每个行业内个股数量进行控制;如果直接对组合内所有个股权重降序排列，会出现所有股票都在金融行业的情况，
        # 这可能是因为金融行业个股财务数据普遍比较好，且市值较大。
        ### 分配每个行业的股票数量上限
        
        if "num_max_indX" in para_dict :
            num_max_indX = para_dict["num_max_indX"]
            df_stocknum =  df_weight.groupby(indX_code).count()
            
            if df_stocknum.loc[list_indX_top5,temp_col ].sum() > para_dict["para_stocknumber"] :
                temp_pct = para_dict["para_stocknumber"]/df_stocknum.loc[list_indX_top5,temp_col ].sum()

            for temp_indX in list_indX_top5 :
                ### 获取当前行业内股票数量
                temp_num = df_stocknum.loc[temp_indX, temp_col  ]
                if temp_num > num_max_indX :
                    ### 选择当前行业内权重排名前 num_max_indX的股票
                    df_weight_sub = df_weight[df_weight[indX_code] == temp_indX ]
                    # 记住行业权重和
                    temp_w_sum = df_weight_sub[ temp_col ].sum()
                    # 降序排列
                    df_weight_sub =df_weight_sub.sort_values(by=temp_col,ascending=False )
                    # 取前 num_max_indX 个股票的index
                    temp_list_index = df_weight_sub.index[:num_max_indX]
                    temp_list_index_drop = df_weight_sub.index[num_max_indX:]
                    # 更新权重
                    pct_adj = temp_w_sum /df_weight.loc[temp_list_index, temp_col].sum()
                    df_weight.loc[temp_list_index, temp_col  ] = df_weight.loc[temp_list_index, temp_col ]*pct_adj
                    df_weight = df_weight.drop(temp_list_index_drop , axis=0  )

        ### 降序排列，以20140531为例，用roe指标这时选出来有216只股票
        df_weight = df_weight.sort_values(by=temp_col, ascending=False) 
        ### 选择前100名的股票,
        ### notes:如果简单按组合权重排名，若权重最大的行业有50只股票，组合股票数量30，则全部股票都会落在单一行业内;
        ### 若多个因子都只选择1~2个相同的行业内的股票，则单一行业风险暴露过大，且起不到多个行业轮动的效果。
        list_index_drop = df_weight.index[num_stock:]
        df_weight.loc[list_index_drop , temp_col  ] = 0.0 
        df_weight[ temp_col ] = df_weight[temp_col]/df_weight[temp_col].sum()

        return df_weight 



    def test_score_weight_return_list_indicator(self,list_indicator,para_dict,date_list ) :
        ###############################################################################
        ## CHOICE 1 :对indicator list 分别计算df_score,df_weight,df_return 
        ''' temp_col = cols_financial_indicator[0]
        ### 设置组合参数
        para_dict= {}
        para_dict["para_indnumber"] = 5 # 默认是5个行业
        para_dict["para_stocknumber"] = 30  # 默认是100只股票
        #3 types: cols_financial_indicator  | cols_abm_3indicator | cols_ind1_indicator
        para_dict["indi_list"] = "ind1_indicator" # 根据col list 名称调整 
        list_indicator = cols_ind1_indicator

        对行业级别进行选择:
        para_dict["ind_level"] = 1 
        '''
        if "ind_level" in para_dict :
            indX_code = "ind"+ str(para_dict["ind_level"] ) +"_code"
            ind_level = str(para_dict["ind_level"] )
        else :
            indX_code = "ind1_code"
            ind_level = "1" 

        ### 导入季度收益数据
        df_return = pd.read_csv( self.path_output+  self.file_return  ) 
        print("df_return loaded"  )       
        
        para_dict["dir_output"] = "para_" +para_dict["indi_list"] +"_" + ind_level
        para_dict["dir_output"]= para_dict["dir_output"]+"_" + str(para_dict["para_indnumber"])+"_"+ str(para_dict["para_stocknumber"] ) 
        
        path_output2 = self.path_output +para_dict["dir_output"] + "\\"

        if not os.path.exists( path_output2 ):
            os.makedirs( path_output2 )

        print("Chosen indicator list : ", list_indicator )

        df_port_return = pd.DataFrame(index=date_list,columns=list_indicator )

        for temp_col in list_indicator  :
            ### 对每个指标新建历史打分df和权重配置df , if_1st = 0 
            if_1st = 0 
            count_date = 0

            for temp_date in date_list :
                
                # temp_date = "weights_"+"20140531" + "_1.csv"
                file_date = "weights_"+temp_date + "_1.csv"

                df_abm = pd.read_csv( self.path_abm + file_date ) 
                df_abm_des = df_abm.describe() 
                # index of df_abm_des | Index(['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max'], dtype='object')

                # print( df_abm_des )
                #################################################################################
                ### 0，基础信息：代码、1、2级行业分类  股票代码	一级行业代码	二级行业代码
                cols_basic = [ "code","ind1_code","ind2_code"]
                # 新建df_score,生成
                df_score = df_abm.loc[:,cols_basic]
                
                # print( df_score.head() ) 
                df_weight = df_abm.loc[:,cols_basic] 

                df_score = self.get_score_mkt(df_abm ,df_score,temp_col )
                df_score = self.get_score_ind1(df_abm ,df_score,temp_col ,para_dict)

                list_indX = df_abm["ind"+ ind_level +"_code"].drop_duplicates().values
                df_weight = self.get_weight( df_weight,df_score,temp_col,list_indX ,para_dict  )

                print("df_score ")
                # print( df_score.head() )
                print("df_weight ")
                # print( df_weight.head() )

                ### 对每个指标新建历史打分df和权重配置df
                if if_1st == 0 :
                    temp_df = df_score
                    temp_df["date"] = temp_date
                    df_score_hist = temp_df

                    temp_df = df_weight
                    temp_df["date"] = temp_date
                    df_weight_hist = temp_df 
                
                else :
                    temp_df= df_score
                    temp_df["date"] = temp_date
                    df_score_hist = df_score_hist.append( temp_df,ignore_index=True )

                    temp_df= df_weight
                    temp_df["date"] = temp_date
                    df_weight_hist = df_weight_hist.append( temp_df,ignore_index=True )
                
                if_1st = if_1st +1 
                ### score and weight goes to the same dir
                file_name_para = ind_level +"_"+ str(para_dict["para_indnumber"])+"_"+ str(para_dict["para_stocknumber"]) 
                df_score_hist.to_csv(self.path_output +"score\\"+"score_"+file_name_para+"_" + temp_col +".csv")
                df_weight_hist.to_csv(self.path_output +"weight\\"+"weight_"+file_name_para+"_" + temp_col +".csv")     
                
                ##############################################################################
                ### 对每一期行业配置进行统计：
                if count_date == 0 :
                    df_weight_sum = df_weight
                    df_allocation_indX = df_weight.groupby("ind"+ ind_level +"_code").sum()
                else :
                    df_weight_sum = df_weight_sum.append( df_weight)
                    df_allocation_indX = df_allocation_indX.append( df_weight.groupby("ind"+ ind_level +"_code").sum() )
                    
                ### append temp_weight_sum to df_weight_sum
                
                df_weight_sum.to_csv( path_output2 +"weight_sum_" +para_dict["str_output"] +".csv")
                df_allocation_indX.to_csv( path_output2 +"allocation_ind_" +para_dict["str_output"] +".csv")

                ##############################################################################
                ### 计算半年度收益  
                ### 1,把持仓对应的收益和历史收益相乘后相加
                ### temp_date；str || type( df_return["date"].values[-1] ) , <class 'numpy.int64'>
                # print("temp_date", type(temp_date), type( df_return["date"].values[-1] ) )

                # step1，找到对应日期且对应代码的列
                
                df_return2 = df_return[  df_return["date"] == int(temp_date) ]        

                df_return2 = df_return2[  df_return2["code"].isin( list(temp_df["code"]) ) ]
                # 把权重df和收益率df按代码排序
                
                df_return2 =df_return2.sort_values(by="code").reset_index() 
                temp_df =temp_df.sort_values(by="code").reset_index()
                
                temp_sum = 0.0
                for temp_i in df_return2.index :            
                    ret_attribution = float(df_return2.loc[temp_i,"return"])*temp_df.loc[temp_i, temp_col ] 
                    temp_sum =temp_sum + ret_attribution 
                
                df_port_return.loc[temp_date,temp_col] = temp_sum 

                print("Tail ", df_port_return.tail() )
                
                ###                
                print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                # input1 = input("Check to continue")

            ##############################################################################    
            ### 对于单个指标的权重文件，剔除权重低于0.1%的股票，并按wind-PMS模板的格式保存
            df_pms= df_weight_hist.loc[:,["code",temp_col,"date"] ]
            df_pms = df_pms[ df_pms[temp_col]>=0.001  ]
            df_pms[temp_col] =df_pms[temp_col]*100
            df_pms.columns = ["证券代码","持仓权重","调整日期"]
            df_pms["成本价格"]= ""
            df_pms["证券类型"] = "股票"
            df_pms.to_csv(path_output2 + "PMS_hist_" + temp_col +".csv",encoding="gbk",index=None)     

            ### 2，一个个导入PMS速度太慢，更合适的方式可能是对持仓数据每八年统一拉一个收益率数据，然后匹配计算区间收益
            # file return_data_0531_1130.csv
            #     code	date	date2	return 
            # 600683.SH	20140531	20141130	60.8%
            # 600816.SH	20140531	20141130	102.3%
            # 000848.SZ	20140531	20141130	24.8%

        ### Save return matrix to csv file 
        file_name_port_return = path_output2 + "0port_return_" +str(para_dict["para_indnumber"])+"_"+ para_dict["indi_list"]+".csv"

        df_port_return.to_csv( file_name_port_return,encoding="gbk",index=None)     

        return df_port_return

    def test_score_weight_return_sum(self,list_indicator,para_dict,date_list ) :
        ###############################################################################
        ### CHOICE2:将所有指标打分加总 ,temp_col = "sum",所有的indicator分数和sum保存在 df_score_sum 里了
        '''
        todo:其实这里还有很多工作要做，还不如统一放到之前的环节里一起计算了。
        ### 但独立也有独立的好处，比如之后可以按照设计的不同加权方式计算
        ### 已经完成对单一指标的权重计算，下一步是对所有指标计算最终的100只股票权重，并保存到csv文件
        
        '''
        if "ind_level" in para_dict :
            indX_code = "ind"+ str(para_dict["ind_level"] ) +"_code"
            ind_level = str(para_dict["ind_level"] )
        else :
            indX_code = "ind1_code"
            ind_level = "1" 
            
        ### 导入季度收益数据
        df_return = pd.read_csv( self.path_output+  self.file_return  ) 
        print("df_return loaded"  )

        cols_basic2 = [ "code","date","ind1_code","ind2_code","code_date"]
        temp_col = "sum"

        path_output2 = self.path_output + para_dict["dir_output"] + "\\"

        if not os.path.exists( path_output2 ):
            os.makedirs( path_output2 )
        print( "path that require score files ", path_output2 ) 

        print("Chosen indicator list : ", list_indicator )

        df_port_return_sum = pd.DataFrame(index=date_list,columns=[temp_col] )
        count_indicator = 0

        ###################################################
        ### 分不同时期,分别导入score计算权重,需要分时期计算权重文件，并且temp_col="sum"
        # for temp_col in cols_financial_indicator :
        file_name_para = ind_level +"_"+ str(para_dict["para_indnumber"])+"_"+ str(para_dict["para_stocknumber"]) 

        for temp_col in list_indicator  :
            ### Import score file as df 
            ### notes：这里可能会报错，需要把各个文件夹里的score_ 文件复制到当前文件夹内。
            print("notes：这里可能会报错，需要把各个文件夹里的score_ 文件复制到当前文件夹内。")
            print( path_output2 )

            df_score  =pd.read_csv( self.path_output+"score\\" +"score_"+file_name_para+"_" + temp_col +".csv")
            df_score  =df_score.drop("Unnamed: 0",axis=1 )

            ### 剔除特定行业：            
            if "drop_ind_list" in  para_dict :
                print( para_dict["drop_ind_list"] )
                for temp_indX in para_dict["drop_ind_list"] : 
                    # int versus str | length from 17594 to 16821 (-773)
                    df_score  =df_score[ df_score[indX_code] != int(temp_indX)  ] 

            df_score["code_date"] = df_score["code"].str.cat( df_score["date"].astype("str"),sep="_" )

            if count_indicator == 0 :        
                df_score_sum = df_score 
                # case "code_date" :603980.SH20180531,603996.SH20180531
                ### add indicator score to score_sum
                df_score_sum["sum_mkt"] = df_score[ temp_col+"_mkt" ] 
                df_score_sum["sum_ind_"+ind_level ] = df_score[ temp_col+"_ind_"+ind_level ] 
            else :
                ### notes:df_score_sum and df_score might have different order in code*date
                ### append df_score to df_score_sum
                # df_score中指标有2列，分别是indi_mkt,indi_indX
                ### 对两df分别按"code","date" 排序，
                # drop columns name="index"
                df_score_sum = df_score_sum.sort_values(by="code_date").reset_index(drop=True)  
                df_score = df_score.sort_values(by="code_date").reset_index(drop=True)
                
                ### add indicator score to score_sum
                df_score_sum["sum_mkt"] = df_score_sum["sum_mkt"] +df_score[ temp_col+"_mkt" ] 
                df_score_sum["sum_ind_"+ind_level ]= df_score_sum["sum_ind_"+ind_level ]+df_score[ temp_col+"_ind_"+ind_level ] 
                df_score_sum[temp_col+"_mkt"] = df_score[ temp_col+"_mkt" ] 
                df_score_sum[temp_col+"_ind_"+ind_level] = df_score[ temp_col+"_ind_"+ind_level ] 
            
            df_score_sum.to_csv( path_output2 +"score_sum_"+ para_dict["str_output"]  +"_.csv")
            count_indicator =count_indicator +1
        
        #############################################################################
        ### 对于每一期的总分，计算当期权重，并append到总df_weight_sum 理

        
        count_date = 0
        temp_col= "sum"
        list_indX = df_score_sum["ind"+ ind_level +"_code"].drop_duplicates().values
        for temp_date in date_list :
            
            ### calculate temp weight         
            temp_score = df_score_sum[ df_score_sum["date"]== int(temp_date) ]
            
            temp_weight = temp_score.loc[:,cols_basic2 ]
            print("Debug====== ")
            print( temp_score.columns )
            print( temp_score.head() )
            print( temp_weight.columns )
            print( temp_weight.head() )
            
            ### 有可能限制单一行业权重上限
            temp_weight = self.get_weight( temp_weight,temp_score,temp_col,list_indX ,para_dict  )

            ### 对每一期行业配置进行统计：
            if count_date == 0 :
                df_weight_sum = temp_weight
                df_allocation_indX = temp_weight.groupby("ind"+ ind_level +"_code").sum()
            else :
                df_weight_sum = df_weight_sum.append( temp_weight)
                df_allocation_indX = df_allocation_indX.append( temp_weight.groupby("ind"+ ind_level +"_code").sum() )
                
            ### append temp_weight_sum to df_weight_sum
            
            df_weight_sum.to_csv( path_output2 +"weight_sum_" +para_dict["str_output"] +"_.csv")
            df_allocation_indX.to_csv( path_output2 +"allocation_ind_" +para_dict["str_output"] +"_.csv")

            #############################################################################
            ### 计算半年度收益  
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

            df_port_return_sum.loc[temp_date,temp_col] = temp_sum 

            file_name_port_return = path_output2 + "0port_return_sum_"+ para_dict["str_output"]  +".csv"
            df_port_return_sum.to_csv(file_name_port_return,encoding="gbk") 

            print("Tail ", df_port_return_sum )

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
        df_pms.to_csv(path_output2 + "PMS_hist_" + temp_col +".csv",encoding="gbk",index=None)     

        ### 2，一个个导入PMS速度太慢，更合适的方式可能是对持仓数据每八年统一拉一个收益率数据，然后匹配计算区间收益
        # file return_data_0531_1130.csv
        #     code	date	date2	return 
        # 600683.SH	20140531	20141130	60.8%
        # 600816.SH	20140531	20141130	102.3%
        # 000848.SZ	20140531	20141130	24.8%
        

        return df_pms

















