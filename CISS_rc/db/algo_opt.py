# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
===============================================
功能：组合权重最优化 | Function: 
1, class algorithm:算法母类
1.1, class algorithm_ashare_weighting:A股组合加权算法   
    1.1.2，algo_ashare_weight_ind_allo |算法：策略组合的个股权重和行业配置算法。")   
    1.1.3，algo_ashare_weight_by_column |计算策略组合的个股权重：基于给定指标column对个股分配权重

1.2 class algorithm_port_ret:A股组合收益率计算
    1.2.1,algo_port_by_market_ana |对于df_market_ana中的每个分组，计算模拟组合区间收益率

2, class optimizer:优化器母类
    1，A股股票优化 optimizer_ashare
    2，多因子优化 optimizer_ashare_factor

2, TODO
3，关联脚本：对应配置文件 | db\data_io.py;config\config_data.py

4,OUTPUT:
    1,obj_1["dict"]，字典信息,json
    2,obj_1["df"]，表格信息,dataframe  

5,分析：目标是所有数据变量以object类型作为输入输出，其中主要是2个key:
    1,obj_1["dict"]:字典格式，数据io都采用json的字典格式。
    2,obj_1["df"]:DataFrame格式

6，Notes: 
    refernce: rC_Portfolio.py 
date:last 200528 | since 181110 
===============================================
'''
import pandas as pd 
import numpy as np 
import sys 
sys.path.append("..")

########################################################################
### 算法母类
class algorithm():
    def __init__(self, algo_name ):
        self.algo_name = algo_name

class algorithm_ashare_weighting():
    def __init__(self  ):
        #######################################################################
        # 继承母类 | 没想好做什么
        # 导入配置文件对象，例如path_db_wind等
        # sys.path.append( path_ciss_rc + "config\\")
        from config_data import config_data_factor_model
        config_data_factor_model_1 = config_data_factor_model()
        self.obj_config = config_data_factor_model_1.obj_config
        # print( self.obj_config["dict"] )
        
        ### factor_model 目录位置：
        # self.obj_config["dict"]["path_factor_model"] = dict_config["path_ciss_db"] +"factor_model\\"
        path_factor_model = self.obj_config["dict"]["path_factor_model"]
        #######################################################################
    def print_info(self):
        ###
        print("algo_ashare_weight_ind_allo |算法：策略组合的个股权重和行业配置算法。")   
        print("algo_ashare_weight_by_column |计算策略组合的个股权重：基于给定指标column对个股分配权重。") 
        print("algo_port_return_by_weight |计算策略组合的收益率：基于个股权重。")       
        print(" | ") 

        return 1

    def algo_ashare_weight_ind_allo(self,obj_data ):
        ### 算法：策略组合的个股权重和行业配置算法
        '''
        生成组合权重 plan:
        1,行业配置：df_ashare_ind
        1.1，对每个行业取"ret_top_30pct"的单指标最大值，并选取该指标下收益率最高的前N(=7)个行业
        1.2, 对于长期固定配置行业=长期看好的，判断是否在行业内;例如必选行业有大消费、电子计算机和医药、金融；
        1.3，目标行业权重，和上下限；

        2，个股权重：df_ashare_portfolio
        2.1，行业内取前30%进行权重分配；
        2.2，单一个股权重不超过10%
        '''
        ### df_ashare_ana包括了个股分析指标、zscore、signals等
        df_ashare_ana =obj_data["df_ashare_ana"]
        ### df_ret_all是根据指标对行业分组计算的最优指标、指标对应区间收益等
        df_ret_all =obj_data[ "df_ret_all"]
        # 删除表中含有任何NaN的行
        df_ret_all = df_ret_all.dropna(axis=0,how='any')
        ind_list_all = df_ret_all["ind_code"].to_list()
        ########################################################################
        ### 1,行业配置：
        ### 1.1，对每个行业取"ret_top_30pct"的单指标最大值，并选取该指标下收益率最高的前N(=7)个行业
        ind_list = df_ashare_ana["ind_code"].drop_duplicates().to_list()
        df_ashare_ind = pd.DataFrame(  columns= df_ret_all.columns )
        # 需要改成 
        df_ret_all["ret_top_30pct"] = df_ret_all["ret_top_30pct"].astype('float64')

        for temp_ind in ind_list :
            ### 确定选股效用最好的行业指标 indicator_max 
            df_ret_ind = df_ret_all[ df_ret_all["ind_code"] == temp_ind  ] 
            # df.idxmax() 返回某一列最大值对应的index
            # print( len( df_ret_ind["ret_top_30pct"] )   )
            # print( df_ret_ind["ret_top_30pct"].idxmax )
            ### notes:下边这一行会报错 ；是因为数据类型问题，TypeError: reduction operation 'argmax' not allowed for this dtype
            # temp_i_max = df_ret_ind["ret_top_30pct"].idxmax()
            #  df_ret_ind["ret_top_30pct"] 需要改成 .astype('float64')
            if len( df_ret_ind.index ) > 0 :
                temp_i_max = df_ret_ind["ret_top_30pct"].idxmax() 
                df_ashare_ind = df_ashare_ind.append( df_ret_ind.loc[temp_i_max,:] ,ignore_index=True  )

        df_ashare_ind = df_ashare_ind.sort_values(by="ret_top_30pct",ascending=False  )
        df_ashare_ind = df_ashare_ind.iloc[:7,:]

        ### 1.2, 对于长期固定配置行业=长期看好的，判断是否在行业内;例如必选行业有大消费、电子计算机和医药、金融；
        # 对每个行业选取上一周期内指标收益最高的indicators和固定指标（如roe_ave），用于下一期的选股。
        # 电子、建材、计算机、医药、基础化工、通信、农林牧渔；60 24 62 35 22 61 37。
        '''中信一级行业
        10	石油石化 11	煤炭 12	有色金属 20	电力及公用事业 21	钢铁
        22	基础化工 23	建筑 24	建材 25	轻工制造 26	机械 27	电力设备及新能源
        28	国防军工 30	汽车 31	商贸零售 32	消费者服务  33	家电 34	纺织服装
        35	医药 36	食品饮料 37	农林牧渔
        40	银行 41	非银行金融 42	房地产 43	综合金融 50	交通运输 
        60	电子 61	通信 62	计算机 63	传媒 70	综合
        '''
        if "ind_fixed" in obj_data["dict"].keys() :
            ind_fixed = obj_data["dict"]["ind_fixed"]
        else :
            ind_fixed = [22.0,25.0,27.0,30.0,33.0,35.0,36.0,37.0,40.0,41.0,62.0,63.0  ]
        
        ### 和全部股票的行业分类匹配|notes:对于单行业，下边这一行可以避免将一级行业纳入计算
        ind_fixed = [ind for ind in ind_fixed if ind in ind_list_all ]
        for temp_ind in ind_fixed :
            if not temp_ind in df_ashare_ind["ind_code"].values :
                ### 确定选股效用最好的行业指标 indicator_max 
                df_ret_ind = df_ret_all[ df_ret_all["ind_code"] == temp_ind  ] 
                ### notes:有可能在历史部分时期，选不出该行业的股票
                if len( df_ret_ind.index  ) > 0 :
                    # df.idxmax() 返回某一列最大值对应的index
                    temp_i_max = df_ret_ind["ret_top_30pct"].idxmax()
                    df_ashare_ind = df_ashare_ind.append( df_ret_ind.loc[temp_i_max,:] ,ignore_index=True  )

        ### 1.3，目标行业权重，和上下限；；根据 "ret_top_30pct"
        temp_sum = abs( df_ashare_ind["ret_top_30pct"].apply(lambda x : x if x >0 else 0 ).sum() )
        df_ashare_ind["weight_ind"] = df_ashare_ind["ret_top_30pct"].apply(lambda x : x*0.99/temp_sum if x >0 else 0.0 )

        ### 剔除ind_code == 0.0的，一般都是新股
        # print( df_ashare_ind["ind_code"].to_list() )
        if 0.0 in df_ashare_ind["ind_code"].to_list() :
            temp_i_0 = df_ashare_ind[df_ashare_ind["ind_code"]==0.0 ].index[0]
            temp_value = df_ashare_ind.loc[ temp_i_0 , "ret_top_30pct" ]
            if temp_value > 0 :
                temp_sum = temp_sum - temp_value 
                ### 计算行业权重            
                df_ashare_ind["weight_ind"] = df_ashare_ind["ret_top_30pct"].apply(lambda x : x*0.99/temp_sum if x >0 else 0.0 )
                df_ashare_ind.loc[ temp_i_0 ,"weight_ind"] = 0.0 
        
        ########################################################################
        ### 2，个股权重 df_ashare_portfolio：
        ### 2.1，行业内取前30%进行权重分配；
        df_ashare_portfolio = pd.DataFrame(  columns=df_ashare_ana.columns )
        ### 若单个一级行业，则取行业内前50%;默认前30%；若indi_quantile_tail=1, 则取尾部指标值，默认值0取指标最大的。
        indi_quantile_tail = 0
        if "indi_quantile_tail" in obj_data["dict"].keys():
            if obj_data["dict"]["indi_quantile_tail"] == 1 :
                indi_quantile_tail = 1

        ### 若单个一级行业，则取行业内前50%的个股;否则默认取前30%的个股，对应para_ind_top_pct = 0.7
        para_ind_top_pct = 0.7
        if "single_industry" in obj_data["dict"].keys() and obj_data["dict"]["single_industry"]> 0 :
            para_ind_top_pct = 0.5
        
        for temp_i in df_ashare_ind.index :
            temp_ind =df_ashare_ind.loc[temp_i, "ind_code"]

            ### 确定选股效用最好的行业指标 indicator_max 
            indicator_max = df_ashare_ind.loc[temp_i, "indicator"]            
            df_ashare_ana_sub = df_ashare_ana [ df_ashare_ana["ind_code"]== temp_ind  ]
            df_ashare_ana_sub2 = df_ashare_ana_sub[ df_ashare_ana_sub[ indicator_max +"_signal"] == 1  ]
                
            ### 用indicator_max 选择前30%的股票
            if df_ashare_ind.loc[ temp_i, "num_sp"] <= 5 :
                # 股票数量太少，全部纳入
                df_ashare_portfolio = df_ashare_portfolio.append( df_ashare_ana_sub2, ignore_index=True )
            else :
                ### indi_quantile_tail=1, 则取尾部指标值;indi_quantile_tail=0,取指标最大的。
                ### quantile_level_70pct 是前30%指标值分界线 ； quantile_level_70pct > quantile_level_30pct
                if indi_quantile_tail == 0 :
                    quantile_level_70pct = df_ashare_ana_sub2["zscore_" +  indicator_max ].quantile( para_ind_top_pct )
                    df_ashare_ana_sub3 = df_ashare_ana_sub2[ df_ashare_ana_sub2["zscore_" + indicator_max  ]>= quantile_level_70pct ]
                elif indi_quantile_tail == 1 :
                    quantile_level_70pct = df_ashare_ana_sub2["zscore_" +  indicator_max ].quantile( 1- para_ind_top_pct )
                    df_ashare_ana_sub3 = df_ashare_ana_sub2[ df_ashare_ana_sub2["zscore_" + indicator_max  ] < quantile_level_70pct ]
                ### 
                df_ashare_portfolio = df_ashare_portfolio.append( df_ashare_ana_sub3 , ignore_index=True )

        ########################################################################
        ### 计算个股权重：保持 df_ashare_portfolio index不减少
        ### 控制入选股票数量：30，50,100，200
        ashare_weight_max_number = obj_data["dict"]["ashare_weight_max_number"]  

        '''常用的几种加权方式：ew,mvfloat,weight_ind,others
        1，ew，等权重；2，mvfloat,流通市值加权,用zscore约等于log() ；
        3，weight_ind:给每个行业分配权重后加权；4，自定义  
        notes:肯定不能用总分加权，容易选到异常值
        '''
        if "ashare_weight_type" not in obj_data["dict"].keys() :
            obj_data["dict"]["ashare_weight_type"] = "mvfloat" 
        
        ashare_weight_type = obj_data["dict"]["ashare_weight_type"]
        ### notes:为了避免流通市值或流通市值标准分有NaN，进行替代;通过减列最小值并不会消除NaN
        # temp_mean = df_ashare_portfolio["zscore_"+"S_DQ_MV"].mean()
        # 取最小值30%分位数的值 
        temp_quantile_value = df_ashare_portfolio["zscore_"+"S_DQ_MV"].quantile( 0.1 )
        # method= pad/ffill：用前一个非缺失值去填充该缺失值;backfill/bfill：用下一个非缺失值填充该缺失值;None:指定一个值去替换缺失值.
        df_ashare_portfolio["adj_"+"S_DQ_MV"] = df_ashare_portfolio["zscore_"+"S_DQ_MV"].fillna(temp_quantile_value )
        
        ### 理论上这一步会消除所有负值
        ### 200711出现了负权重，部分流通市值复权早期是负数，这对应了股票未来的大涨，应该把市值转正
        temp_min = df_ashare_portfolio["zscore_"+"S_DQ_MV"].min() * 1.3
        if temp_min < 0 :
            df_ashare_portfolio["adj_"+"S_DQ_MV"] = df_ashare_portfolio["adj_"+"S_DQ_MV"] - temp_min
        # df_ashare_portfolio["adj_"+"S_DQ_MV"] = df_ashare_portfolio["adj_"+"S_DQ_MV"].apply(lambda x : x+abs(temp_min) if x <= 0 else x)

        if ashare_weight_type == "ew" :
            # notes:如果股票数量大于最大股票数量N，取流通市值前N个
            # rank() 数值越小的排越前
            temp_len = len( df_ashare_portfolio["rank_mvfloat"] )
            df_ashare_portfolio["rank_mvfloat"] = df_ashare_portfolio["adj_"+"S_DQ_MV"].rank(method="first")
            df_ashare_portfolio["weight_raw"] =df_ashare_portfolio["rank_mvfloat"].apply(lambda x : 1/ashare_weight_max_number if x> temp_len-100 else 0  ) 
            
        if ashare_weight_type == "mvfloat" :
            # 取出流通市值标准分的负值
            temp_value_min = df_ashare_portfolio["adj_"+"S_DQ_MV"].min()
            temp_sum = ( df_ashare_portfolio["adj_"+"S_DQ_MV"] - temp_value_min).sum()
            df_ashare_portfolio["weight_raw"] = df_ashare_portfolio["adj_"+"S_DQ_MV"].apply(lambda x : (x-temp_value_min)/temp_sum )

        if ashare_weight_type == "weight_ind" :
            ### 先计算所有股票的流通市值加权
            # 取出流通市值标准分的负值
            temp_value_min = df_ashare_portfolio["adj_"+"S_DQ_MV"].min()
            temp_sum = ( df_ashare_portfolio["adj_"+"S_DQ_MV"] - temp_value_min).sum()
            df_ashare_portfolio["weight_raw"] = df_ashare_portfolio["adj_"+"S_DQ_MV"].apply(lambda x : (x-temp_value_min)/temp_sum )

            ### 根据行业权重分配 df_ashare_ind["weight_ind"]
            ind_list = df_ashare_ind["ind_code"].to_list() 
            # 删除 无行业分类股票
            if 0.0 in ind_list :
                ind_list.remove(0.0)
            for temp_ind in ind_list :
                ### 获取行业分配到的权重
                df_ashare_ind_sub = df_ashare_ind[ df_ashare_ind["ind_code"]== temp_ind  ]                
                temp_weight_ind = df_ashare_ind.loc[ df_ashare_ind_sub.index[0], "weight_ind"] 
                print("Weight for industry ",temp_ind,temp_weight_ind)
                ### 避免 temp_weight_ind为负值
                if temp_weight_ind <= 0.01 :
                    temp_weight_ind = 0.03

                ### 行业内按流通市值加权计算权重
                df_ashare_portfolio_sub = df_ashare_portfolio[ df_ashare_portfolio["ind_code"] ==temp_ind ]
                temp_index_list = df_ashare_portfolio_sub.index
                df_ashare_portfolio.loc[temp_index_list, "weight_raw"] = df_ashare_portfolio.loc[temp_index_list, "weight_raw"] / df_ashare_portfolio.loc[temp_index_list, "weight_raw"].sum()
                
                ### 所有权重乘行业权重
                df_ashare_portfolio.loc[temp_index_list, "weight_raw"] = df_ashare_portfolio.loc[temp_index_list, "weight_raw"]  * temp_weight_ind
                                
        ### temp save 
        # df_ashare_portfolio.to_csv("D:\\df_ashare_portfolio_200528.csv")
        
        ### 控制入选股票数量：30，50,100，200
        # 剔除权重等于0 的股票
        df_ashare_portfolio["weight_raw"] = df_ashare_portfolio["weight_raw"].apply(lambda x : x if x >=0.0003 else 0 )
        ## 例如173个股票要减到100个，需要减去排名在1~73最小的股票
        temp_num_stock = df_ashare_portfolio["weight_raw"].count()
        if temp_num_stock > ashare_weight_max_number :
            # temp_len:权重大于0的股票数量  ;rank() 数值越小的排越前
            temp_len = df_ashare_portfolio[ df_ashare_portfolio["weight_raw"]>0]["weight_raw"].count() 
            df_ashare_portfolio["rank_weight"] = df_ashare_portfolio["weight_raw"].rank(method="first")
            df_ashare_portfolio["weight_raw"] =df_ashare_portfolio["weight_raw"].apply(lambda x : x if x> temp_len-100 else 0  ) 
            df_ashare_portfolio["weight_raw"] = df_ashare_portfolio["weight_raw"]/df_ashare_portfolio["weight_raw"].sum()

        ### 控制单只股票权重ashare_weight_max，默认不超过10%|注意：早期组合个股数量小于11时，不限制个股权重！
        ashare_weight_max = obj_data["dict"]["ashare_weight_max"] 

        temp_df = df_ashare_portfolio[ df_ashare_portfolio["weight_raw"]>= 0.0003 ]
        num_stocks = len( temp_df.index  )
        ### 早期组合个股数量小于11时，不限制个股权重！
        if num_stocks > 11 :
            # steps：1，对于权重超过10%的股票，计算权重与10%差值之和，权重改为9%；2，将差值按比例分配给剩余股票。
            temp_df = df_ashare_portfolio[ df_ashare_portfolio["weight_raw"]>= ashare_weight_max ]
            temp_weight_sum = temp_df["weight_raw"].sum()
            df_ashare_portfolio.loc[temp_df.index,  "weight_raw"]= 0.09
            temp_weight_diff = temp_weight_sum - len(temp_df.index )*0.09
            ### 将差异的权重均匀分配给其他股票
            temp_df_rest = df_ashare_portfolio[~ df_ashare_portfolio.index.isin( temp_df.index ) ]
            temp_factor = (df_ashare_portfolio.loc[temp_df_rest.index,  "weight_raw"]+temp_weight_diff )/df_ashare_portfolio.loc[temp_df_rest.index,  "weight_raw"]
            df_ashare_portfolio.loc[temp_df_rest.index,  "weight_raw"]= df_ashare_portfolio.loc[temp_df_rest.index,  "weight_raw"] * temp_factor

            df_ashare_portfolio.loc[temp_df_rest.index,  "weight_raw"]=df_ashare_portfolio.loc[temp_df_rest.index,  "weight_raw"]/df_ashare_portfolio.loc[temp_df_rest.index,  "weight_raw"].sum()
        else :
            df_ashare_portfolio["weight_raw"] = df_ashare_portfolio["weight_raw"].apply(lambda x : x if x >=0.001 else 0.0)
        
        ### 按权重降序排列  
        df_ashare_portfolio["weight_raw"] = df_ashare_portfolio["weight_raw"]*0.95/df_ashare_portfolio["weight_raw"].sum()
        df_ashare_portfolio = df_ashare_portfolio.sort_values(by="weight_raw",ascending=False )
        
        ### 剔除权重为负值的股票
        df_ashare_portfolio["weight_raw"] = df_ashare_portfolio["weight_raw"].apply(lambda x: x if x >= 0.003 else 0.0)
        
        ### 查错：若组合权重小于90%，重新分配
        if df_ashare_portfolio["weight_raw"].sum() < 0.9 :     
            df_ashare_portfolio["weight_raw"] = df_ashare_portfolio["weight_raw"]*0.95/df_ashare_portfolio["weight_raw"].sum()
        
                
        ########################################################################
        ### Save stock and industry_group to output 
        obj_data["df_ashare_ind"] = df_ashare_ind
        obj_data["df_ashare_portfolio"] = df_ashare_portfolio

        return obj_data

    def algo_ashare_weight_by_column(self,obj_data_w ):
        ### 计算策略组合的个股权重：基于给定指标column对个股分配权重
        '''
        input:para:
        赋权方法weighting_type={
            real_5level:参考自真实基金权重的5档，real_4level:参考自真实基金权重的4档，
            real_3level:参考自真实基金权重的3档，real_2level:参考自真实基金权重的2档     }
        
        notes: 默认是降序排列，即数值越大越好
        ref:sheet="组合仓位设计",file=PMS组合管理.xlsx,0基金持仓仿真.xlsx
        '''
        obj_data_w["df_ana"] = obj_data_w["df_ana"].sort_values(by= obj_data_w["name_column"],ascending=False)

        obj_data_w["df_ana"]["weight"] = 0.0

        num_stock = len( obj_data_w["df_ana"].index )
        list_num_stock = []
        #################################################################
        ### notes：只有股票数量大于15才可以用五档分类,否则使用二档 
        print("num_stock ",num_stock )
        if num_stock >= 15 :
            #################################################################
            ### 五档
            if obj_data_w["weighting_type"] in [5,"real_5level","5"] :                
                ### 计算5档对应的股票数量并取整：[ 0.1,0.28,0.56,0.83,1.0 ]
                list_num_stock_pct = [ 0.1,0.28,0.56,0.83 ]
                list_num_stock = [ round(x* num_stock) for x in list_num_stock_pct  ]
                list_num_stock = [0] + list_num_stock +[ num_stock ]
                ### 每一档权重之和  
                list_weight_sum = [0.18,0.24,0.27,0.195,0.07]
                for i in range(5): 
                    obj_data_w["df_ana"]["weight"].iloc[list_num_stock[i] : list_num_stock[i+1] ] = list_weight_sum[i]/(list_num_stock[i+1]-list_num_stock[i])
            #################################################################
            ### 四档参考 

        else :
            if num_stock >= 5 :
                ##############################
                ### 二档
                list_num_stock_pct = [ 0.3 ]
                list_num_stock = [ round(x* num_stock) for x in list_num_stock_pct  ]
                list_num_stock = [0] + list_num_stock +[ num_stock ]
                ### 每一档权重之和  
                list_weight_sum = [0.7,0.3]
                for i in range(2): 
                    obj_data_w["df_ana"]["weight"].iloc[list_num_stock[i] : list_num_stock[i+1] ] = list_weight_sum[i]/(list_num_stock[i+1]-list_num_stock[i])
            
            else :
                ##############################
                ### 股票数量小于5只，等权重
                obj_data_w["df_ana"]["weight"] = 1/num_stock

        return obj_data_w

    def algo_port_return_by_weight(self, obj_port_ret_by_rank ):
        ### 计算策略组合的收益率：基于个股权重
        ### derived from cal_port_ret_by_rank( obj_in):
        ### 根据股票列表重特定指标的分组排序，加权计算分组的区间收益率
        ### calculation 
        ### Initialization 
        df_port_ret = obj_port_ret_by_rank["df_port_ret"] 
        ### A股的不同指标
        df_ashare_ana = obj_port_ret_by_rank["df_ashare_ana"] 
        ### A股价格涨跌幅百分比
        df_ashare_pctchg = obj_port_ret_by_rank["df_ashare_pctchg"] 

        port_type = obj_port_ret_by_rank["port_type"]
        type_name = obj_port_ret_by_rank["type_name"] 
        name_column = obj_port_ret_by_rank["name_column"] 
        ### 分组计算方式，市场分组对应 "list_rank"，需要分组计算，行业分组"ind_value_growth"不需要按数量分组
        cal_type = obj_port_ret_by_rank["cal_type"] 
        
        ### 默认是降序排列，即数值越大越好
        df_ashare_ana = df_ashare_ana.sort_values(by=name_column,ascending=False)
        num_stocks = len( df_ashare_ana.index )

        ### 导入加权方式算法：使用何种加权方式？？等权重明显会偏离实际情况；策略：按照指标分档。
        # algo_opt.py 
        # from algo_opt import algorithm_ashare_weighting
        # algorithm_ashare_weighting_1 = algorithm_ashare_weighting()
        import math 

        #####################################################################
        ### 市场分组内按股票数量计算分组
        if cal_type == "list_rank" :
            ### notes：如果是行业分组，就不需要用list_rank 方式
            # list_rank =  [ [1,300],[301,800],[801,1800],[1801,10000] ]
            list_rank = obj_port_ret_by_rank["list_rank"] 
            for temp_rank in list_rank :
                
                # 判断股票数量是否足够大
                if num_stocks > temp_rank[0] :
                    # print( temp_rank[0],temp_rank[1] )         
                    ### 计算细分组合在过去20、60、120天收益率。
                    port_name = port_type + "_"+ type_name + "_" + str(temp_rank[0]) +"_"+ str(temp_rank[1])
                    print("port_name ", port_name )
                    # notes:第N~M个值在index里对应的是 N-1~M-1个。
                    temp_df_ana = df_ashare_ana.iloc[ temp_rank[0]-1:temp_rank[1] ,:]
                    # print( temp_df_ana.describe() )
                    
                    #########################################
                    # algo_ashare_weight_by_column |计算策略组合的个股权重：基于给定指标column对个股分配权重
                    obj_data_w = {}
                    ### 分5档权重
                    obj_data_w["weighting_type"] = "real_5level"
                    obj_data_w["name_column"] = name_column
                    obj_data_w["df_ana"] = temp_df_ana 
                    obj_data_w = self.algo_ashare_weight_by_column(obj_data_w )
                    
                    obj_data_w["df_ana"] = obj_data_w["df_ana"].loc[:,["S_INFO_WINDCODE","weight"] ]
                    ### 赋值给 df_ashare_ana | 似乎没必要
                    # df_ashare_ana.loc[ temp_df_ana.index, "weight"  ] = temp_df_ana.loc[:, "weight "]

                    ### 计算过去20~120 天组合收益率： calculate df_period_change_20d
                    # df_ashare_pctchg包括了全部股票数量，比obj_data_w["df_ana"] 数据量大。
                    # print(obj_data_w["df_ana"].head() )
                    # print( df_ashare_pctchg.head() ) 
                    # df_ashare_pctchg.to_csv("D:\\df_ashare_pctchg0.csv")    
                    
                    ### df_ashare_pctchg_w 是包括对应分组内个股权重的df
                    df_ashare_pctchg_w = pd.merge( obj_data_w["df_ana"],df_ashare_pctchg,on="S_INFO_WINDCODE"   ) 
                    # df_ashare_pctchg_w.to_csv("D:\\df_ashare_pctchg.csv")

                    for temp_date in obj_port_ret_by_rank["date_list"] : 
                        df_port_ret.loc[port_name, temp_date ] = (df_ashare_pctchg_w[temp_date]*df_ashare_pctchg_w["weight"]).sum()

        ##############################################################
        ### 单个行业内分价值和成长组合
        if cal_type == "ind_value_growth" :
            ### 分别计算：全行业、行业市值前30%数量、行业价值前30%数量、行业成长前30%数量的分组收益
            ### 分组名称 "","_mvtotal_30p","_value_30p","_growth_30p"
            ### type_name is ind_code, from float to str
            type_name = str(int(type_name )) 
            ##################################################################################
            ### 1， 全行业 "", 
            port_name = port_type + "_"+  type_name
            print("port_name ", port_name )
            # notes:第N~M个值在index里对应的是 N-1~M-1个。
            temp_df_ana = df_ashare_ana  
            #########################################        
            # algo_ashare_weight_by_column |计算策略组合的个股权重：基于给定指标column对个股分配权重
            obj_data_w = {}
            ### 分5档权重
            obj_data_w["weighting_type"] = "real_5level"
            obj_data_w["name_column"] = name_column
            obj_data_w["df_ana"] = temp_df_ana 
            obj_data_w = self.algo_ashare_weight_by_column(obj_data_w )

            obj_data_w["df_ana"] = obj_data_w["df_ana"].loc[:,["S_INFO_WINDCODE","weight"] ]
            ### 赋值给 df_ashare_ana | 似乎没必要
            # df_ashare_ana.loc[ temp_df_ana.index, "weight"  ] = temp_df_ana.loc[:, "weight "]

            ### 计算过去20~120 天组合收益率： calculate df_period_change_20d
            # df_ashare_pctchg包括了全部股票数量，比obj_data_w["df_ana"] 数据量大。
            # print(obj_data_w["df_ana"].head() )
            # print( df_ashare_pctchg.head() ) 
            # df_ashare_pctchg.to_csv("D:\\df_ashare_pctchg0.csv")    

            ### df_ashare_pctchg_w 是包括对应分组内个股权重的df
            df_ashare_pctchg_w = pd.merge( obj_data_w["df_ana"],df_ashare_pctchg,on="S_INFO_WINDCODE"   ) 
            # df_ashare_pctchg_w.to_csv("D:\\df_ashare_pctchg.csv")

            for temp_date in obj_port_ret_by_rank["date_list"] : 
                df_port_ret.loc[port_name, temp_date ] = (df_ashare_pctchg_w[temp_date]*df_ashare_pctchg_w["weight"]).sum()

            ##################################################################################
            ### 2， 行业市值前30%数量,"_mvtotal_30p",
            port_name = port_type + "_"+ type_name + "_mvtotal_30p" 
            print("port_name ", port_name )
            ### 指标降序排列取前30%数量 ，"S_VAL_MV" 
            df_ashare_ana = df_ashare_ana.sort_values(by="S_VAL_MV",ascending=False)
            num_stocks = len( df_ashare_ana.index )
            num_30p = round( math.floor( num_stocks*0.3 ))
            ### 有可能出现0只股票满足条件的情况
            if num_30p >= 1 :
                temp_df_ana = df_ashare_ana.iloc[ :num_30p, : ]
                #########################################        
                # algo_ashare_weight_by_column |计算策略组合的个股权重：基于给定指标column对个股分配权重
                obj_data_w = {}
                ### 分5档权重
                obj_data_w["weighting_type"] = "real_5level"
                obj_data_w["name_column"] = name_column
                obj_data_w["df_ana"] = temp_df_ana 
                obj_data_w = self.algo_ashare_weight_by_column(obj_data_w )

                obj_data_w["df_ana"] = obj_data_w["df_ana"].loc[:,["S_INFO_WINDCODE","weight"] ] 
                ### df_ashare_pctchg_w 是包括对应分组内个股权重的df
                df_ashare_pctchg_w = pd.merge( obj_data_w["df_ana"],df_ashare_pctchg,on="S_INFO_WINDCODE"   ) 
                # df_ashare_pctchg_w.to_csv("D:\\df_ashare_pctchg.csv")

                for temp_date in obj_port_ret_by_rank["date_list"] : 
                    df_port_ret.loc[port_name, temp_date ] = (df_ashare_pctchg_w[temp_date]*df_ashare_pctchg_w["weight"]).sum()

            ##################################################################################
            ### 3， 价值指标前30%数量,"_value_30p", notes:有可能出现0只股票满足条件的情况，例如全行业大跌
            port_name = port_type + "_"+ type_name + "_value" 
            print("port_name ", port_name )
            ### PE取正值且越小越好；指标降序排列取前30%数量 ，EST_PE_FY1
            df_ashare_ana_sub = df_ashare_ana[ df_ashare_ana["EST_PE_FY1"] >= 0.0 ]
            df_ashare_ana_sub = df_ashare_ana_sub.sort_values(by="EST_PE_FY1",ascending=True)
            num_stocks = len( df_ashare_ana_sub.index )
            num_30p = round( math.floor( num_stocks*0.3 ))
            
            ### 有可能出现0只股票满足条件的情况，例如全行业大跌
            if num_30p >= 1 :
                temp_df_ana = df_ashare_ana_sub.iloc[ :num_30p, : ]
                #########################################        
                # algo_ashare_weight_by_column |计算策略组合的个股权重：基于给定指标column对个股分配权重
                obj_data_w = {}
                ### 分5档权重
                obj_data_w["weighting_type"] = "real_5level"
                obj_data_w["name_column"] = name_column
                obj_data_w["df_ana"] = temp_df_ana 
                obj_data_w = self.algo_ashare_weight_by_column(obj_data_w )

                obj_data_w["df_ana"] = obj_data_w["df_ana"].loc[:,["S_INFO_WINDCODE","weight"] ] 

                ### df_ashare_pctchg_w 是包括对应分组内个股权重的df
                df_ashare_pctchg_w = pd.merge( obj_data_w["df_ana"],df_ashare_pctchg,on="S_INFO_WINDCODE"   ) 
                # df_ashare_pctchg_w.to_csv("D:\\df_ashare_pctchg.csv")

                for temp_date in obj_port_ret_by_rank["date_list"] : 
                    df_port_ret.loc[port_name, temp_date ] = (df_ashare_pctchg_w[temp_date]*df_ashare_pctchg_w["weight"]).sum()

            ##################################################################################
            ### 4， 成长指标前30%数量,"_growth_30p",notes:有可能出现0只股票满足条件的情况，例如全行业大跌
            port_name = port_type + "_"+ type_name + "_growth" 
            print("port_name ", port_name )
            ### PEG取正值且越小越好；指标降序排列取前30%数量 ，EST_PEG_FY1，且满足 EST_PEG_FY1>0
            df_ashare_ana_sub = df_ashare_ana[ df_ashare_ana["EST_PEG_FY1"] >= 0.0 ]
            df_ashare_ana_sub = df_ashare_ana_sub.sort_values(by="EST_PEG_FY1",ascending=True)
            num_stocks = len( df_ashare_ana_sub.index )
            num_30p = round( math.floor( num_stocks*0.3 ))

            ### 有可能出现0只股票满足条件的情况，例如全行业大跌
            if num_30p >= 1 :
                temp_df_ana = df_ashare_ana_sub.iloc[ :num_30p, : ]
                #########################################        
                # algo_ashare_weight_by_column |计算策略组合的个股权重：基于给定指标column对个股分配权重
                obj_data_w = {}
                ### 分5档权重
                obj_data_w["weighting_type"] = "real_5level"
                obj_data_w["name_column"] = name_column
                obj_data_w["df_ana"] = temp_df_ana 
                obj_data_w = self.algo_ashare_weight_by_column(obj_data_w )

                obj_data_w["df_ana"] = obj_data_w["df_ana"].loc[:,["S_INFO_WINDCODE","weight"] ] 

                ### df_ashare_pctchg_w 是包括对应分组内个股权重的df
                df_ashare_pctchg_w = pd.merge( obj_data_w["df_ana"],df_ashare_pctchg,on="S_INFO_WINDCODE"   ) 
                # df_ashare_pctchg_w.to_csv("D:\\df_ashare_pctchg.csv")

                for temp_date in obj_port_ret_by_rank["date_list"] : 
                    df_port_ret.loc[port_name, temp_date ] = (df_ashare_pctchg_w[temp_date]*df_ashare_pctchg_w["weight"]).sum()

        #####################################################################
        ### 对于给定的组合名称，反推出对应的行业或市场分组，例如：
        # industry_41_mvtotal_30p，industry_30_growth，market_amt_1_300 等



        # market_amount_1_300 market_amount_301_800 market_amount_801_1800
        obj_port_ret_by_rank["df_port_ret"] = df_port_ret

        return obj_port_ret_by_rank


#######################################################################
### class algorithm_port_ret:A股组合收益率计算
class algorithm_port_ret():
    def __init__(self  ):
        #######################################################################
        # 继承母类 | 没想好做什么
        # 导入配置文件对象，例如path_db_wind等
        # sys.path.append( path_ciss_rc + "config\\") 
        from config_data import config_data
        self.obj_config = config_data()
        
        # print( self.obj_config["dict"] )
        
        ### list_rank=[ [1,300],[301,800],[801,1800],[1801,10000] ]
        self.list_rank=[ [1,300],[301,800],[801,1800],[1801,10000] ]

        #######################################################################
    def print_info(self):
        ### 
        print("algo_port_by_market_ana |对于df_market_ana中的每个分组，计算模拟组合区间收益率。")
        print("algo_port_by_port_id | 基于组合id名称对应的分类和给定日期，计算模拟组合持仓和区间收益率。")       
        print(" | ") 

        return 1

    def algo_port_by_market_ana(self,obj_port ):
        ### 对于df_market_ana中的每个分组，计算模拟组合区间收益率
        algorithm_ashare_weighting_1 = algorithm_ashare_weighting()
        ##################################################################################
        ### 计算不同市场分组的区间收益，分别保存在 df_period_change_20d,df_period_change_60d,df_period_change_120d
        # ref: def market_status_abcd3d_ana(self,obj_ana) from analysis_indicators.py
        ### 分组 :[ [1,300],[301,800],[801,1800],[1801,10000] ] 
        list_rank= self.list_rank

        #########################################
        ### 根据股票列表重特定指标的分组排序，加权计算分组的区间收益率:
        ### INPUT: ( df_port_ret, df_ashare_ana ,port_type,type_name,name_column) 
        ### OUTPUT: df_port_ret
        obj_port_ret_by_rank = {}
        ### 交易日序列
        obj_port_ret_by_rank["date_list"] = obj_port["date_list"]
        ### 新建分组组合的时间序列收益率 df_port_ret
        df_port_ret = pd.DataFrame() 
        obj_port_ret_by_rank["df_port_ret"] = df_port_ret
        ### 给定指标和对应的A股列表
        obj_port_ret_by_rank["df_ashare_ana"] = obj_port["df_ashare_ana"]
        ### A股价格涨跌幅百分比
        obj_port_ret_by_rank["df_ashare_pctchg"] =  obj_port["df_ashare_pctchg"]

        #########################################
        ### sub 1 当日成交金额、市值、流通市值、
        obj_port_ret_by_rank["cal_type"] = "list_rank" 
        obj_port_ret_by_rank["list_rank"]  = [ [1,300],[301,800],[801,1800],[1801,10000] ]

        obj_port_ret_by_rank["port_type"]  = "market"
        obj_port_ret_by_rank["type_name"] = "amt"
        obj_port_ret_by_rank["name_column"] = "S_DQ_AMOUNT"
        # obj_port_ret_by_rank = cal_port_ret_by_rank( obj_port_ret_by_rank )
        obj_port_ret_by_rank = algorithm_ashare_weighting_1.algo_port_return_by_weight(obj_port_ret_by_rank )
        ###
        obj_port_ret_by_rank["port_type"]  = "market"
        obj_port_ret_by_rank["type_name"] = "mvfloat" 
        obj_port_ret_by_rank["name_column"] = "S_DQ_MV"
        # obj_port_ret_by_rank = cal_port_ret_by_rank( obj_port_ret_by_rank )
        obj_port_ret_by_rank = algorithm_ashare_weighting_1.algo_port_return_by_weight(obj_port_ret_by_rank )
        ###
        obj_port_ret_by_rank["port_type"]  = "market"
        obj_port_ret_by_rank["type_name"] = "mvtotal" 
        obj_port_ret_by_rank["name_column"] = "S_VAL_MV"
        # obj_port_ret_by_rank = cal_port_ret_by_rank( obj_port_ret_by_rank )
        obj_port_ret_by_rank = algorithm_ashare_weighting_1.algo_port_return_by_weight(obj_port_ret_by_rank )
        df_port_ret = obj_port_ret_by_rank["df_port_ret"]

        #########################################
        ### sub 2 行业、成长价值
        ###导入行业分类及对应的中文 

        obj_port_ret_by_rank["cal_type"] = "ind_value_growth" 
        path_ind_names = self.obj_config.obj_config["dict"]["path_wind_adj"]
        df_ind_names = pd.read_csv(path_ind_names+ "ind_code_name.csv"  ,encoding="gbk" )

        ### 获取中信一级行业列表
        df_ashare_ana = obj_port_ret_by_rank["df_ashare_ana"]
        # [0.0, 10.0, 11.0, 12.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 30.0, 
        # 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 40.0, 41.0, 42.0, 50.0, 60.0, 61.0, 62.0, 63.0, 70.0]
        list_ind_code = df_ashare_ana["ind_code"].drop_duplicates().to_list()
        list_ind_code.sort()
        list_ind_code_str = [ str(int(x)) for x in list_ind_code  ]

        for ind_code in list_ind_code :
            # code_str = list_ind_code_str( list_ind_code.index(ind_code) )
            code_str = str(int( ind_code ))
            # find code name in df_ind_names
            print( ind_code )
            df_ind_names_sub = df_ind_names[ df_ind_names["ind_code"]== int(ind_code) ]
            if len(df_ind_names_sub.index ) > 0 :
                ind_name = df_ind_names_sub["ind_name"].values[0]
                print( ind_code,ind_name  )
                df_ashare_ana_ind = df_ashare_ana[ df_ashare_ana["ind_code"]==ind_code ]

                temp_i = ind_code
                ### 设置输入项：
                ### 给定指标和对应的A股列表
                obj_port_ret_by_rank["df_ashare_ana"] = df_ashare_ana_ind
                obj_port_ret_by_rank["port_type"]  = "industry"
                obj_port_ret_by_rank["type_name"] = ind_code
                ### 分组已经都是同一行业个股，只需按总市值或成交金额选择。
                obj_port_ret_by_rank["name_column"] = "S_VAL_MV" 
                
                ## obj_port_ret_by_rank = cal_port_ret_by_rank( obj_port_ret_by_rank )
                obj_port_ret_by_rank = algorithm_ashare_weighting_1.algo_port_return_by_weight(obj_port_ret_by_rank )

        ### save to output 
        obj_port["df_port_ret"] = obj_port_ret_by_rank["df_port_ret"]

        ### save to csv :
        # df_port_ret = obj_port_ret_by_rank["df_port_ret"]
        # df_port_ret.to_csv("D:\\df_port_ret2.csv")    

        return obj_port

    def algo_port_by_port_id(self,obj_port ):
        ### 基于组合id名称对应的分类和给定日期，计算模拟组合持仓和区间收益率
        #  市场分组和行业分组内个股的权重是通过固定的算法计算的，并且没有保存组合的持股，因为持仓股票应该随时间重新计算。
        # 进度：line 578， def algo_port_return_by_weight(self, obj_port_ret_by_rank ):
        algorithm_ashare_weighting_1 = algorithm_ashare_weighting()
        
        ##################################################################################
        ### 初始化对象
        obj_port_ret_by_rank = {}
        ### 交易日序列
        obj_port_ret_by_rank["date_list"] = obj_port["date_list"]
        ### 新建分组组合的时间序列收益率 df_port_ret
        df_port_ret = pd.DataFrame() 
        obj_port_ret_by_rank["df_port_ret"] = df_port_ret
        ### 给定指标和对应的A股列表
        obj_port_ret_by_rank["df_ashare_ana"] = obj_port["df_ashare_ana"]
        df_ashare_ana  = obj_port["df_ashare_ana"]
        ### A股价格涨跌幅百分比
        obj_port_ret_by_rank["df_ashare_pctchg"] =  obj_port["df_ashare_pctchg"]
        
        ##################################################################################
        # obj_port["port_name_market_ana"] 来源于df_port_perf_eval.index,前5行是用市场分组方法market_ana计算出来匹配度最高的5个组合，index是组合的名称
        # 主要有3类名称：1，行业和行业细分：例如industry_41_mvtotal_30p，industry_30_growth，industry_36_mvtotal_30p
        # 2，市场细分，例如market_amt_801_1800，market_mvfloat_1_300
        str_list = obj_port["port_name_market_ana"].split("_") 
        if str_list[0] == "industry" :
            ### 行业和行业细分
            obj_port_ret_by_rank["cal_type"] = "ind_value_growth" 
            # notes：不用考虑是否细分行业或取前30%股票数量，直接给定行业让模块全部算出来，然后匹配组合名称
            
            ind_code  = int( str_list[1] ) 
            df_ashare_ana_ind = df_ashare_ana[ df_ashare_ana["ind_code"]==ind_code ] 
            ### 设置输入项：
            ### 给定指标和对应的A股列表
            obj_port_ret_by_rank["df_ashare_ana"] = df_ashare_ana_ind
            obj_port_ret_by_rank["port_type"]  = "industry"
            obj_port_ret_by_rank["type_name"] = ind_code
            ### 分组已经都是同一行业个股，只需按总市值或成交金额选择。
            obj_port_ret_by_rank["name_column"] = "S_VAL_MV" 
            
            ## obj_port_ret_by_rank = cal_port_ret_by_rank( obj_port_ret_by_rank )
            obj_port_ret_by_rank = algorithm_ashare_weighting_1.algo_port_return_by_weight(obj_port_ret_by_rank )


        if str_list[0] == "market" :
            ### 市场细分；market_amt_301_800，market_mvfloat_1_300，market_mvtotal_1_300
            market_type = str_list[1]
            if market_type == "amt":
                name_column = "S_DQ_AMOUNT"
            elif market_type == "mvfloat":
                name_column = "S_DQ_MV"
            elif market_type == "mvtotal":
                name_column = "S_VAL_MV"

            obj_port_ret_by_rank["cal_type"] = "list_rank" 
            obj_port_ret_by_rank["list_rank"]  = [ [1,300],[301,800],[801,1800],[1801,10000] ]

            obj_port_ret_by_rank["port_type"]  = "market"
            obj_port_ret_by_rank["type_name"] = market_type # "amt"
            obj_port_ret_by_rank["name_column"] = name_column # "S_DQ_AMOUNT"
            # obj_port_ret_by_rank = cal_port_ret_by_rank( obj_port_ret_by_rank )
            obj_port_ret_by_rank = algorithm_ashare_weighting_1.algo_port_return_by_weight(obj_port_ret_by_rank )

            
        ###################################################################################
        ### output
        
        return obj_port_ret_by_rank



########################################################################
### 生成组合权重
class algorithm_port_weight():
    def __init__(self  ):
        #######################################################################
        # 继承母类 | 没想好做什么
        # 导入配置文件对象，例如path_db_wind等
        # sys.path.append( path_ciss_rc + "config\\") 
        from config_data import config_data
        self.obj_config = config_data()
        
        ### list_rank=[ [1,300],[301,800],[801,1800],[1801,10000] ]
        self.list_rank=[ [1,300],[301,800],[801,1800],[1801,10000] ]

        #######################################################################
    def print_info(self):
        ### 
        print("algo_port_weight_by_indicator |根据indicator指标，计算组合内子组合或个股的权重")       
        print(" | ") 

        return 1

    def algo_port_weight_by_indicator(self,obj_port ):
        ### 根据indicator指标，计算组合内子组合或个股的权重
        ### 1， dict_weight_indi：df_weight_indi_2[ "ret_end" + "_" + "long" ] = 0.25 *0.33
        dict_weight_indi = obj_port["dict_weight_indi"]
        ### 2,df_port_perf_eval，index是市场分组或其他组合，columns是300498
        df_port_perf_eval = obj_port["df_port_perf_eval"]
        # 删除目标基金对应的行 |        temp_fund_code = "080001.OF"
        port_name = obj_port["port_name"] 
        df_temp = df_port_perf_eval.drop([port_name] ,axis=0  )
        ### 3，新建组合权重dict
        dict_weight_port ={}

        ### 分别计算各个指标，分组和和目标组合差值最小的前三个组合赋值，求
        for temp_indi in ["ret_end","ret_fromlow","mdd" ,"mdd_fromhigh"]:
            for temp_len in ["long","mid","short" ] :
                temp_indi_len = temp_indi + "_" + temp_len
                ### 获得目标组合对应的值
                temp_value = df_port_perf_eval.loc[port_name, temp_indi_len ] 

                ### 对指标 temp_indi_len  内，不同分组于目标基金最接近的值
                df_temp["value"] =  df_temp[temp_indi_len ] - temp_value
                df_temp["value"] = df_temp["value"].apply(lambda x: abs(x)  )
                #按绝对值排序
                # 数值越大越好:
                if temp_indi in ["ret_end","ret_fromlow"] : 
                    df_temp = df_temp.sort_values(by="value",ascending= False  )
                ### 数值越小大越好: 
                elif temp_indi in ["mdd" ,"mdd_fromhigh"] : 
                    df_temp = df_temp.sort_values(by="value",ascending= True  )
                ### 取前3的公司，赋予权重：|需要取值能体现出差异化
                # notes:部分指标可能有超过3个，甚至全部值都是一样的情况，例如0，0，0或1，1，1
                # notes:"value" 所有值应该 >0 
                if df_temp.loc[df_temp.index[0] , temp_indi_len ] > 0.0 : 
                    df_port_perf_eval.loc[ df_temp.index[0] , "score"] = 3 * dict_weight_indi[ temp_indi_len ]
                
                if df_temp.loc[df_temp.index[1] , temp_indi_len ] > 0.0 : 
                    df_port_perf_eval.loc[ df_temp.index[1] , "score"] = 2 * dict_weight_indi[ temp_indi_len ]

                if df_temp.loc[df_temp.index[3] , temp_indi_len ] > 0.0 : 
                    df_port_perf_eval.loc[ df_temp.index[3] , "score"] = 1 * dict_weight_indi[ temp_indi_len ]
                                
        obj_port["df_port_perf_eval"] = df_port_perf_eval
        ### 综合得分前5的组合
        df_temp = df_port_perf_eval.sort_values(by="score",ascending=False )
        obj_port["list_port"] = list( df_temp.index[:5 ])
        
        ### save to output 


        return obj_port 


########################################################################
### 优化器母类
class optimizer():
    def __init__(self):
        self.opt_name = ""

    def optimizer_weight(self,stra_estimates_group ):
    	# import all strategy estimation and return optimized weight list 
    	# todo todo 
        if len( stra_estimates_group) ==1 :
            optimizer_weight_list  = stra_estimates_group['key_1']
        else :
            # we need to make ranking list and calculation best strategy decision 
			# from all strategy suggestion.
            optimizer_weight_list  = 1

        return optimizer_weight_list

class optimizer_ashare_factor():
    def __init__(self  ):
        #######################################################################
        # 继承母类 | 没想好做什么
        # 导入配置文件对象，例如path_db_wind等
        # sys.path.append( path_ciss_rc + "config\\")
        from config_data import config_data_factor_model
        config_data_factor_model_1 = config_data_factor_model()
        self.obj_config = config_data_factor_model_1.obj_config
        # print( self.obj_config["dict"] )
        
        ### factor_model 目录位置：
        # self.obj_config["dict"]["path_factor_model"] = dict_config["path_ciss_db"] +"factor_model\\"
        path_factor_model = self.obj_config["dict"]["path_factor_model"]
        #######################################################################
        ### 导入日期list，日、周、月 | date_list_tradingday.csv  ...     
        file_name_month = self.obj_config["dict"]["file_date_month"]    
        # file_name_tradingday = self.obj_config["dict"]["file_date_tradingday"] 
        # file_name_week = self.obj_config["dict"]["file_date_week"]   
        
    def print_info(self):
        ###
        print("set_cons_bounds_init || 定义限制条件 1~ C个：constraint_c，输出对象obj_cons，上下界，初始权重")
        print("run_opt_min_model ||基于scipyoptimize.minimize生成最优化模型，输入项obj_cons至少需要包括cons，bnds，w_init。 ")
        print(" | ")
        print("opt_port_weights_factor ||用指标和因子数据计算组合最优权重 ")

        return 1
    
    def set_cons_bounds_init(self, obj_cons) :
        ### 定义限制条件 1~ C个：constraint_c，输出对象obj_cons，上下界，初始权重
        #####################################################################
        ### 根据输入对象，设置需要的变量和参数等
        len_factor = obj_cons["len_factor"]
        factor_weight_np = obj_cons["factor_weight_np"] 
        w_index_consti = obj_cons["w_index_consti"]
        ind_code_list = obj_cons["ind_code_list"]
        ind_code_np = obj_cons["ind_code_np"]
        len_stock = obj_cons["len_stock"]
        w_stock_np = obj_cons["w_stock_np"]
        # 限制条件分为紧1，和松0.
        method_cons = obj_cons["dict"]["method_cons"]  
        print("method_cons ", method_cons )
        
        if method_cons == 1 :
            # 严格条件
            para_list=[-0.3, 0.3, -0.1, 0.1, -0.02,0.02,0.95,0.99,0.99,20,50 ]
        elif method_cons == 0 :
            # 宽松条件
            para_list=[ -1,  2,  -0.3, 0.3,  -0.1,0.1, 0.8,0.95,0.99,30,150 ]

        #####################################################################
        ### cons对应的参数设置, 可变的变量
        ### 因子暴露：constraint_1_factor,constraint_2_factor
        # 生成一列都是-1或1的数组 ，或1*5尺寸的nan：np.full([1,len() ], np.nan )
        s_l = np.full([1, len_factor ], -1 )
        s_h = np.full([1,len_factor ], 1 )
        # 设置流通市值标准分的上下限
        s_l[0] = para_list[0]  #-1
        s_h[0] = para_list[1]  # 2
        ### 行业暴露：constraint_3_ind,constraint_4_ind
        # ind_lb = np.zeros( (1,len(ind_code_list ) ) ) 
        # 例：任意一个行业权重下限不应该低于min(0.01,0.1/len(ind_code_list) )
        ind_lb = np.ones( (1,len(ind_code_list ) ) ) 
        # ind_lb = ind_lb* min(0.01,0.1/len(ind_code_list))
        ind_lb = ind_lb* para_list[2]

        #任意一个行业权重上限不应该超过30%
        ind_ub = np.ones( (1,len(ind_code_list ) ) )
        ind_ub = ind_ub* para_list[3] # 0.3

        ### （不用）个股权重暴露：constraint_5_s，constraint_6_s
        w_l = np.ones( len_stock)
        w_h = np.ones( len_stock)
        w_s_l = para_list[4]
        w_s_h = para_list[5]
        w_l = w_l * w_s_l
        w_h = w_h * w_s_h
                
        ### 组合仓位限制：constraint_7_port，constraint_8_port
        w_total_l = para_list[6]
        w_total_h = para_list[7]

        ### 换手率限制；换手率限制turnover_limit ,80%
        turnover_limit =  para_list[8]
        # 上一期股票权重：
        w_stock_opt_pre = np.zeros( len_stock)

        ### 股票数量 ：
        num_stock_lb = para_list[9]
        num_stock_ub = para_list[10]
        
        #####################################################################
        def constraint_1_factor(w_stock_opt):
            ''' 1,X*w -X*w_b -s_l >= 0 
            因子约束条件:s_l，s_h是因子暴露的上下限；一般只对市值因子设置上下限，如市值中性设置：
            s_l_mv=0 and s_h_mv = 0 ；只限制市值因子，也就是 0<= sum{x_i_k,k=mv} <=0
            改写：s_l+ X*w_b <= X*w <= s_h+ X*w_b , 
            where X= factor_weight_np ,factor_weight_np from df_factor_weight;因子暴露矩阵,N*K matrix
            # notes:factor_weight_np第一行是市值因子
            第一个流通市值zscore_S_DQ_MV值在-0.59到7之间，均值0.0，这个对我们来说没有必要进行限制,[-0.1,1]可以近似看作无限制
                例如，200507是，浦发银行值2.15，深发展和万科分别是2.48和2.75，三个流通市值排名9th~11th，当时一种最优组合可能是仅仅持仓这三个
            
            obj_para["dict"]["method_cons"] = 1 # 限制条件分为紧1，和松0.
            '''

            # shape of factor_weight_np 16*300
            result = np.matmul( factor_weight_np,w_stock_opt) -np.matmul( factor_weight_np,w_index_consti) - s_l
            if len(result) == 1 :
                result = result[0]
            # print("1 factor ",len(result),result)
            return result

        def constraint_2_factor(w_stock_opt):
            ''' 2, s_h+ X*w_b - X*w >= 0 
            因子约束条件: X*w <= s_h+ X*w_b ,    '''
            result = -1*np.matmul( factor_weight_np,w_stock_opt) +np.matmul( factor_weight_np,w_index_consti) + s_h
            if len(result) == 1 :
                result = result[0]
            return result

        def constraint_3_ind(w_stock_opt):
            '''行业暴露矩阵ind_code_np，设置组合相对于基准行业权重的上限和下线:ind_lb[i]对应了第i个行业的权重下限
            ind_ub[i]对应第i个行业的权重上限。，例如行业中性设置：ind_lb=0.0，ind_ub= 0.0
            ind_bm:基准组合的行业权重
            from  ind_lb <= ind_code_np*( w_stock_opt - w_index_consti )   <= ind_ub
            to: 1,ind_code_np*w_stock_opt - ind_code_np*w_index_consti - ind_lb >= 0
            2,-1*ind_code_np*w_stock_opt + ind_code_np*w_index_consti + ind_ub >= 0
            notes:ind_code_list 行业分类数值从小到大排列，最后一个可能是nan, len(`)=30
            notes:ind_ub不能是 np.ones( (len(ind_code_list,1 ) ) )
            '''             
            result = np.matmul( ind_code_np,w_stock_opt) - np.matmul( ind_code_np,w_index_consti) - ind_lb
            if len(result) == 1 :
                result = result[0]
            return result  

        def constraint_4_ind(w_stock_opt):
            '''行业暴露矩阵ind_code_np
            from  ind_lb <= ind_code_np*( w_stock_opt - w_index_consti )   <= ind_ub
            to: 2,-1*ind_code_np*w_stock_opt + ind_code_np*w_index_consti + ind_ub >= 0
            # notes:ind_code_list 行业分类数值从小到大排列，最后一个可能是nan
            notes:ind_ub不能是 np.ones( (len(ind_code_list,1 ) ) )
            '''     
            
            result = -1*np.matmul( ind_code_np,w_stock_opt) + np.matmul( ind_code_np,w_index_consti) + ind_ub
            if len(result) == 1 :
                result = result[0]
            return result  
        # 变量的上限和下限似乎不是必须的，因为 optimize.minimize的bounds条件可以直接限制
        def constraint_5_s(w_stock_opt):
            '''4,个股相对于基准指数中权重暴露的上下限，w_l <= 1*(w-w_index) <= w_h
            例如上下限+2%/-2%；w_L = -0.02,w_h=0.02
            个股权重矩阵 w_stock_np:对于N个股票，N*N矩阵，每行仅对角线1个值为1，其余为0
            '''            
            result = np.matmul(w_stock_np, w_stock_opt)- np.matmul( w_stock_np,w_index_consti) - w_l 
            return result  

        def constraint_6_s(w_stock_opt):
            '''4,个股相对于基准指数中权重暴露的上下限，w_l <= 1*(w-w_index) <= w_h
            例如上下限+2%/-2%；w_L = -0.02,w_h=0.02
            # 例如：个股权重上限w_s_h不超过10%
            '''            
            result = -1*np.matmul(w_stock_np, w_stock_opt)+np.matmul( w_stock_np,w_index_consti) + w_h
            return result  

        def constraint_7_port(w_stock_opt):
            '''5,总仓位的上下限，例如最低80%，最高95%；w_total_l <= sum(w) <= w_total_h
            w_total_l = 0.8, w_total_h= 0.95 
            np.sum(rr),对array的rr的所有值求和， np.sum(rr,axis=0)对每一列求和， np.sum(rr，axis=1)对每一行求和
            '''            
            result = np.sum( w_stock_opt ) - w_total_l
            return result  

        def constraint_8_port(w_stock_opt):
            '''5,总仓位的上下限，例如最低80%，最高95%；w_total_l <= sum(w) <= w_total_h
            w_total_l = 0.8, w_total_h= 0.95 
            np.sum(rr),对array的rr的所有值求和， np.sum(rr,axis=0)对每一列求和， np.sum(rr，axis=1)对每一行求和
            '''
            
            result = -1*np.sum( w_stock_opt ) + w_total_h
            return result  

        def constraint_9_turnover(w_stock_opt):
            '''6,当期权重变动，当期个股权重减上期个股权重的绝对值之和,例如每个季度60%对应每年240%，买入和卖出都算。
            sum( abs(𝑤_s_total_t − 𝑤_s_total_t_pre )) ≤ turnover_limit 
            上一期股票权重：w_stock_opt_pre,如果是第一期可以取0.0
            abs(array1-array2 ) 会取两列的每一个差值
            换手率限制turnover_limit 第一期应该是100%，之后按单季度应该是75%，按单月应该是30%。
            turnover_limit = 1.0
            '''
            result = -1*np.sum( abs( w_stock_opt-w_stock_opt_pre )) + turnover_limit

            return result  

        def constraint_10_s(w_stock_opt):
            '''7，控制股票数量，如50个或300个。
            num_stock_lb  <= len()  <= num_stock_ub
            '''            
            result = len( w_stock_opt[w_stock_opt>=0.0] ) - num_stock_lb
            return result  

        def constraint_11_s(w_stock_opt):
            '''7，控制股票数量，如50个或300个。
            num_stock_lb  <= len()  <= num_stock_ub
            '''            
            result = -1*len( w_stock_opt[w_stock_opt>=0.0] ) + num_stock_ub
            return result  
        ########################################################################
        ### Setting constraints
        # eq表示 函数结果等于0 ； ineq 表示 表达式大于等于0 ||  'type':'ineq' >= ; 'type':'eq' ==  
        # 例子：{'type': 'ineq', 'fun': lambda w_stock_opt:  -1*np.matmul( x,np.matmul(cov_asset_df,x) )+ var_bench  }
        cons = ({'type': 'ineq', 'fun': constraint_1_factor },
                {'type': 'ineq', 'fun': constraint_2_factor },
                {'type': 'ineq', 'fun': constraint_3_ind },
                {'type': 'ineq', 'fun': constraint_4_ind },

                {'type': 'ineq', 'fun': constraint_7_port },
                {'type': 'ineq', 'fun': constraint_8_port },
                {'type': 'ineq', 'fun': constraint_9_turnover },
                {'type': 'ineq', 'fun': constraint_10_s },
                {'type': 'ineq', 'fun': constraint_11_s } ) 

                # {'type': 'ineq', 'fun': constraint_5_s },
                # {'type': 'ineq', 'fun': constraint_6_s },
        ########################################################################
        ### Setting bounds
        # ：个股权重上限w_s_h不超过10%
        bnds = [(0.0 ,0.1 )] * len_stock
        # 设置所有股票的初始权重，可以单股票为1，均匀权重，直接取市场基准权重等 
        # w_init = np.ones( (len_stock) )/len_stock
        # notes:未来应考虑其他初始权重
        w_init =  w_index_consti 


        obj_cons["cons"] = cons
        obj_cons["bnds"] = bnds
        obj_cons["w_init"] = w_init
        
        return obj_cons
        
    def run_opt_min_model(self,obj_cons) :
        ### 基于scipyoptimize.minimize生成最优化模型，输入项obj_cons至少需要包括"cons"，"bnds"，"w_init"
        '''
        todo:未来可能有多种不同目标方程
        
        input:
        ret_stock_change_np 当月的收益率
        ret_stock_change_6m_np,近6个月/120天收益率;temp_date_pre_120d

        output对象 obj_opt主要内容：
        1，"dict":字典
        2，"df_index_consti": df,指数成分
        3,"w_index_consti":array
        4,"code_list_csi300":list,
        5,"col_list": column list ,
        6,"factor_weight_np"
        7,"len_factor";8,"df_ind_code"
        9,"ret_stock_change_np"
        10,"ret_stock_change_6m_np"
        11,"ind_code_list"
        12，"len_stock"
        13，"ind_code_np"
        14，"w_stock_np"
        15，"df_4opt"
        16，"col_list_4opt"
        17~20,"cons","fun","bnds","w_init"
        21,"res":{fun,jac,... }    
            jac：返回梯度向量的函数,array
            message: 'Positive directional derivative for linesearch'
            nfev: 27355
            nit: 92
            njev: 88
            status: 8
            success: False
            x: array()
        '''
        ### 读取输入对象
        obj_opt = obj_cons
        # 最优化方法：method_opt =  "minvar"
        method_opt = obj_opt["dict"]["method_opt"]  

        # 限制条件
        cons=  obj_opt["cons"]
        bnds=  obj_opt["bnds"]
        w_init =  obj_opt["w_init"]

        # 用于目标方程的变量
        ret_stock_change_np = obj_opt["ret_stock_change_np"] 
        ret_stock_change_6m_np = obj_opt["ret_stock_change_6m_np"] 
        # 用于组合优化设置的其他指标值,df_4opt,col_list_4opt
        df_4opt = obj_opt["df_4opt"]  
        col_list_4opt = obj_opt["col_list_4opt"]  
        col_list = obj_opt["col_list"]
        # notes：factor_weight_np里的columns对应col_list
        factor_weight_np = obj_opt["factor_weight_np"] 

        '''col_list_4opt = ["ret_accumu_20d","ret_accumu_120d","ret_mdd_20d","ret_mdd_120d"]
        ret_accumu_20d，ret_accumu_120d，20天和120天累计收益
        ret_alpha_ind_citic_1_120d: 120天相对于中信一级行业的收益
        ret_mdd_20d，ret_mdd_120d, 20天内最大回撤，120天内最大回撤
        file=df_factor_20050531_000300.SH_20050531.csv
        ''' 
        # print("mdd_stock_change_6m_np" , mdd_stock_change_6m_np.shape ) 
        
        ########################################################################
        ### Setting Objective function 
        # obj_fun中，min_port_ret = -1 逻辑是最小化组合的收益，
        # 这样在optimize.minimize求最小值时变成了最大化组合收益
        min_port_ret = -1  
        
        if method_opt == "min_var" :
            ### 方案：min_var;均值方差模型，收益不佳：最大化前6月收益，最小化最大回撤 | 方差对应了下行压力，可以使用最大回撤代替 
            mdd_stock_change_6m_np = df_4opt["ret_mdd_120d"].values
            obj_fun = lambda w_stock_opt: min_port_ret * np.matmul( ret_stock_change_6m_np, w_stock_opt ).sum()/(-1*np.matmul( mdd_stock_change_6m_np, w_stock_opt ).sum() )
        elif method_opt == "max_ret" :
            ### 方案：max_ret;最大化前6月收益 | 实践证明基于过去收益率的最大化很不好
            obj_fun = lambda w_stock_opt: min_port_ret * np.matmul( ret_stock_change_6m_np, w_stock_opt ).sum()

        elif method_opt == "short_long" :
            ### 方案：short_long,短空长多：另一个方案是追求(前120天收益最大和近20天收益最小 )| 实践证明基于过去收益率的最大化很不好
            ret_diff_6m_1m = ret_stock_change_6m_np - ret_stock_change_np
            obj_fun = lambda w_stock_opt: min_port_ret * np.matmul( ret_diff_6m_1m, w_stock_opt ).sum()
        
        elif method_opt == "max_score" :
            ### 方案：max_score ：最大化因子标准分 | 1,使用标准分；2，使用ic_ir
            obj_fun = lambda w_stock_opt: min_port_ret * np.matmul( factor_weight_np, w_stock_opt ).sum()

        # 
        obj_opt["obj_fun"] = obj_fun
        
        ### 方案：最大化因子指标值

        ########################################################################
        ### 导入相关包 
        from scipy import optimize
        # 默认的最大迭代次数是1000；maxiter[int, optional] Maximum number of algorithm iterations. Default is 1000.
        options_1={"maxiter":300 }
        # optimize.minimize第一个input：obj_fun: 求最小值的目标函数
        res = optimize.minimize( obj_fun , w_init, method='SLSQP', bounds=bnds,constraints=cons,options=options_1)

        obj_opt["res"] = res

        
        return obj_opt


    def opt_port_weights_factor(self, obj_port):
        ### 用指标和因子数据计算组合最优权重
        '''
        组合测试目的和计划：
            1，观察策略用于全市场择时的有效性：全部A股、创业板、科创板
            2，观测用于不同行业、风格的有效性
            3，观察不同调仓频率的组合有效性：季度、月、周；以及不同的偏移交易日
            4，年换手率限制
        组合回测参数：
        1，回测周期、交易、组合参数：config_port.py
            1.1，回测周期
            1.2，建仓：初始资金、
            1.3，调仓频率、调仓频率得偏移日期、
        2，股票池：中证800；全市场组合：大小盘轮动组合{大、中、小、微小}；stockpools.py
        3，指标和因子选股，abcd3d：analysis_indicators.py
            3.1，单指标：abcd3d_ave_mvfloat, abcd3d_ave_num,abcd3d_pct_down,abcd3d_pct_up
            3.2，单指标参数：数值越大越好或越小越好、极值处理 ；
            3.3，筛选条件-选股：多指标得处理逻辑{and、or}；前X%股票、分行业前X%股票；top,bottom,rank,top_pct,bottom_pct,rank_pct。
        4，股票�������������������������重计算：algo_opt.py
            4.1, 选择指标、设置因子权重，进行打分排序；
            4.2，筛选条件-加权：前X%股票、分行业前X%股票；top,bottom,rank,top_pct,bottom_pct,rank_pct
            4.3，加权方式：流通市值、指标或因子得分加权{成长g、价值pe、peg、净利润等}
        5，组合管理：交易、持仓等：ports.py
        6，perf_eval绩效分析：{累计收益、年化收益、逐年收益、年化波动率、最大回撤、最大年还手率、平均年还手、
            Alpha、Beta、Sharpe、IR=基准收益}
            信息比率 =（策略每日收益-基准每日收益）的年化均值/年化标准差
            Sharpe=(Rp− Rf)/σp ,其中Rp为策略年化收益率，Rf为一年定存利率，σp为策略年化波动率

        测试策略的有效性：T日，根据策略打分对行业进行配置，分数越高配越多
        定期：20天调整一次
        临时：若理论配置比例变动太大，则临时调整。

        '''

        temp_date = obj_port["dict"]["date"] 
        ### 设定组合类型，例如成长内行业轮动、市场内大小流通市值轮动、不同行业成长和价值轮动
        # "market","value_growth","industry","mixed"=所有分组都考虑
        group_type = obj_port["dict"]["group_type"] 

        df_ashare_ana = obj_port["df_ashare_ana"] 

        df_market_ana = obj_port["df_market_ana"] 

        ####################################################################
        ### 根据分组选项决定筛选策略："market","value_growth","industry","mixed"=所有分组都考虑
        if group_type == "industry" :
            df_market_ana_sub = df_market_ana[ df_market_ana["group_type"]==group_type ]
            # Unnamed: 0 : 36.0,33.0
            df_market_ana_sub["ind_code"] = df_market_ana_sub["Unnamed: 0"]
            ### 中证一级行业有31个，一般最少也会有15个行业，选择前1/3的数量进行配置
            num_group = round( len(df_market_ana_sub.index)/3  )
            df_market_ana_sub = df_market_ana_sub.sort_values(by="abcd3d_ave_mvfloat",ascending=False )
            df_market_ana_sub = df_market_ana_sub.iloc[:num_group, :] 
            ### 此处不用控制太严格，因为还会在行业内控制个股得分
            df_market_ana_sub = df_market_ana_sub[ df_market_ana_sub["abcd3d_ave_mvfloat"]>=3 ]
            
            ### todo，是否要剔除分值小于1的行业；是否判断市场行情，若太小则降低股票配置比例；是否看个股涨跌百分比

            if len( df_market_ana_sub.index ) > 0 :
                count_group = 0 
                ### 对于入选的前1/3的行业，进行配置
                for temp_i in df_market_ana_sub.index :
                    temp_ind_code = df_market_ana_sub.loc[temp_i,"ind_code"]

                    ### 取行业内个股的分组，取个股得分大于等于3.0的 || df_ashare_ana["ind_code"] is float 
                    df_ashare_ana_sub = df_ashare_ana[ df_ashare_ana["ind_code"]== float(temp_ind_code) ] 
                    ### todo:阈值应测试0,...6 | 从0改为5有显著提升。
                    df_ashare_ana_sub_filter = df_ashare_ana_sub[ df_ashare_ana_sub["abcd3d"]>=6 ] 
                    if len( df_ashare_ana_sub_filter.index ) > 0 : 
                        if count_group == 0 :
                            df_port_weight = df_ashare_ana_sub_filter
                            count_group = 1  
                        else :
                            df_port_weight = df_port_weight.append(df_ashare_ana_sub_filter,ignore_index=True) 

        ### todo， market情况下，有mvfloat，mvtotal,amt 3种
        '''todo,对大中小，小微四种组合判断市场状态，若上涨则对上涨的分组内取个股。
        '''
        if group_type == "market" :
            df_market_ana_sub = df_market_ana[ df_market_ana["group_type"]==group_type ]
            # Unnamed: 0 : 36.0,33.0
            
            num_group = round( len(df_market_ana_sub.index)/3  )
            df_market_ana_sub = df_market_ana_sub.sort_values(by="abcd3d_ave_mvfloat",ascending=False )
            df_market_ana_sub = df_market_ana_sub.iloc[:num_group, :] 
            ### 此处不用控制太严格，因为还会在行业内控制个股得分
            ### todo
            
            
            # df_market_ana_sub = df_market_ana_sub[ df_market_ana_sub["abcd3d_ave_mvfloat"]>=4 ]

            # if len( df_market_ana_sub.index ) > 0 :
            #     count_group = 0 
            #     ### 对于入选的前1/3的行业，进行配置
            #     for temp_i in df_market_ana_sub.index :
            #         temp_ind_code = df_market_ana_sub.loc[temp_i,"ind_code"]

            #         ### 取行业内个股的分组，取个股得分大于等于3.0的 || df_ashare_ana["ind_code"] is float 
            #         df_ashare_ana_sub = df_ashare_ana[ df_ashare_ana["ind_code"]== float(temp_ind_code) ] 

            #         df_ashare_ana_sub_filter = df_ashare_ana_sub[ df_ashare_ana_sub["abcd3d"]>=6 ] 
            #         if len( df_ashare_ana_sub_filter.index ) > 0 : 
            #             if count_group == 0 :
            #                 df_port_weight = df_ashare_ana_sub_filter
            #                 count_group = 1  
            #             else :
            #                 df_port_weight = df_port_weight.append(df_ashare_ana_sub_filter,ignore_index=True) 



        ####################################################################
        ### 加权计算各组选中的个股权重        
        if "df_port_weight" in locals() and len( df_port_weight.index ) > 0 : 
            ### 剔除不同组内重复的个股,keep=False 表示不保存重复值
            df_port_weight = df_port_weight.drop_duplicates("S_INFO_WINDCODE",keep=False )

            ### 初步权重
            ### mvfloat=市值加权
            if obj_port["dict"]["weighting_type"] == "mvfloat" :
                df_port_weight["w_raw"] = df_port_weight["abcd3d"]*df_port_weight["S_DQ_MV"]
            
            ### ew=等权重
            if obj_port["dict"]["weighting_type"] == "ew" :
                df_port_weight["w_raw"] = df_port_weight["abcd3d"]

            ### growth = 成长加权"growth_fy1" ，
            if obj_port["dict"]["weighting_type"] == "growth" :                        
                df_port_weight["growth_fy1"]=df_port_weight["EST_PE_FY1"]/df_port_weight["EST_PEG_FY1"]
                df_port_weight["w_raw"] = df_port_weight["abcd3d"]*df_port_weight["growth_fy1"]

            ### value=价值加权 "EST_PE_FY1"
            if obj_port["dict"]["weighting_type"] == "value" :
                df_port_weight["w_raw"] = df_port_weight["abcd3d"]/df_port_weight["EST_PE_FY1"]
            
            ###
            df_port_weight["w_raw"] = df_port_weight["w_raw"]/ df_port_weight["w_raw"].sum()

            df_port_weight["w_port"] = df_port_weight["w_raw"]*0.95

        else :
            ### 股票配置比例为 0 
            df_port_weight = df_ashare_ana
            df_port_weight["w_raw"] =0.0
            df_port_weight["w_port"] =0.0
        
        ### Save df_port_weight to obj_port
        obj_port["df_port_weight"] = df_port_weight

        return obj_port
