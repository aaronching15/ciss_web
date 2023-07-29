# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
===============================================
TODO： 

功能：根据给定的指标和规则计算股票池
承接部分analysis_indicators.py 功能
last update 220126 | since  181102
Notes: 
related files:  
1, 

===============================================
'''
import sys,os
import pandas as pd
import numpy as np
import json
import time
import datetime as dt

### path_root = 'C:\\rc_2023\\rc_202X\\'
path0 = os.getcwd().split("ciss_web")[0]  
path_ciss_web = path0+ "ciss_web\\"
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )

sys.path.append( path0 + "ciss_web\\CISS_rc\\db\\db_assets\\" ) 
from get_wind_api import wind_api
class_wind_api = wind_api()

#########################################################
class stockpool():
    def __init__(self ):
        #########################################################
        ###  
        self.nan = np.nan 
        ### path_root = 'C:\\rc_2023\\rc_202X\\'
        path_root = os.getcwd().split("ciss_web")[0]  
        self.path0 =path_root  
        ### 'C:\\rc_2023\\rc_202X\\ciss_web\\'
        path_ciss_web = self.path0 + "ciss_web\\"  

        self.path = self.path0 + "data_pms\\"
        self.file= "pms_manage.xlsx" 
        
        self.path_wpf = self.path + "wpf\\" 
        self.path_data = self.path + "wind_terminal\\" 
        self.path_adj = self.path +  "data_adj\\"
        import datetime as dt  
        self.time_now = dt.datetime.now()
        self.time_now_str = dt.datetime.strftime(self.time_now   , "%Y%m%d")
        self.time_pre_str = dt.datetime.strftime(self.time_now - dt.timedelta(days=1) , "%Y%m%d")
        self.time_pre10_str = dt.datetime.strftime(self.time_now - dt.timedelta(days=10) , "%Y%m%d")
        ######################################################################
        # file= "pms_manage.xlsx"
        self.df_pms_manage = pd.read_excel(self.path+ self.file , sheet_name="组合列表"  ) 

        ######################################################################

    def print_info(self):
        ### print all modules for current script
        print("### 标准化计算模块================================================")  
        print("cal_stockpool_indi | 给定指标，计算量化股票池 ")   
        # print("cal_stockpool_active | 给定股票池，计算主观股票池 ")   
        print("  |  ") 
        print("  |  ") 

        return 1 

    def cal_stockpool_indi(self, obj_shares ):
        ### 给定指标，计算量化股票池
        ##########################################
        ### 导入股票数据：a_shares_20220121.xlsx 或 ah_shares_20220121.xlsx
        date_latest = obj_shares["date_latest"]
        temp_key = "a"
        file_name = temp_key +"_shares_" + date_latest +".xlsx" 

        df_shares = pd.read_excel(self.path_adj + file_name )
        # print("df_shares \n", df_shares.head().T )
        ### 替换异常值 ||  
        df_shares.replace(np.inf, 0.0 ) #替换正inf为-1
        
        print( df_shares.head().T )
        ####################################################################################
        ### 股票池筛选条件：
        count_ind = 0 
        ##########################################
        ### 1，总市值：中信三级行业内前50%，越高越好; 成交金额市场前20%
        list_ind3 = list( df_shares["中信三级行业"].drop_duplicates() )
        for temp_ind3 in list_ind3 :
            df_sub = df_shares[ df_shares["中信三级行业"] == temp_ind3 ]

            ########################################## 
            ### 月平均市值； notes：quantile需要反过来 前5%对应 quantile(0.95)
            mv_ave_head = df_sub["m_ave_mv"].quantile( 0.4 )
            df_sub = df_sub[ df_sub["m_ave_mv"] >= mv_ave_head ]
            ##########################################
            ### 月均成交金额 m_ave_amt ;这个更反映短期情绪，不如市值好？
            # amt_ave_head = df_sub["m_ave_amt"].quantile( 0.60 )
            # df_sub = df_sub[ df_sub["m_ave_amt"] >= amt_ave_head ] 
            if len( df_sub.index ) > 0 : 
                ##########################################
                ### 基金持股比例；大于1%，且总金额（乘以流通市值）大于5亿元；越高越好？
                # df_sub = df_sub[ df_sub["基金持股比例"] >= 0.5 ] 
                df_sub = df_sub[ df_sub["基金持股比例"] >= 1 ] 
                if len( df_sub.index ) > 0 :
                    ##########################################
                    ### 市盈率(TTM)；无
                    ##########################################
                    ### 净资产收益率(TTM) ；大于 10%
                    df_sub = df_sub[ df_sub["净资产收益率(TTM)"] >= 9.9 ] 
                    if len( df_sub.index ) > 0 :
                        ##########################################
                        ### 归母净利润同比增长率：三级行业内前50%，或前5名
                        growth_head = max(1, df_sub["归母净利润同比增长率"].quantile( 0.51 )) 
                        df_sub = df_sub[ df_sub["归母净利润同比增长率"] >= growth_head  ]

                        if len( df_sub.index ) > 0 :
                            ##########################################
                            ### 动量：中期趋势大于0 ;短空长多;近120天涨跌幅为正是赚钱的，
                            df_sub = df_sub[ df_sub["60日涨跌幅"] >= -0.001 ]
                            # 200107，强势股应该有容忍度；妙可蓝多涨停收盘价16.24，ma40价13.73，p_ma40=0.1828 
                            df_sub = df_sub[ df_sub["trend_mid"] >= -0.001 ]                             
                            ##########################################
                            ### 主动股票池：参考权重
                            if len( df_sub.index ) > 0 :
                                ### 保存至股票池
                                if count_ind == 0 :
                                    df_stockpool = df_sub 
                                    count_ind = 1
                                else :
                                    df_stockpool = df_stockpool.append(df_sub ,ignore_index=True)
                ##########################################
                ### PARA SET
                # Idea:p_ma40不能只看正值，还应该避免买的太贵，比如不超过 +10%？


        if len( df_stockpool.index ) > 0 :
            ### save to output file 
            file_name = temp_key +"_stockpool_"+ "indi_" + date_latest +".xlsx" 
            df_stockpool.to_excel(self.path_adj + file_name ,index=False )
            file_name = temp_key +"_stockpool_"+ "indi" +".xlsx" 
            df_stockpool.to_excel(self.path_adj + file_name ,index=False )

        obj_shares["df_stockpool"] = df_stockpool

        return obj_shares 




###############################################################################
### BEFORE 181102
class gen_stockpools():
    def __init__(self, id_time_stamp,temp_df={},config={},sp_name0=''):
        self.sp_name = sp_name0 
        self.sp_head = self.gen_stockpool_head(id_time_stamp, config,sp_name0)        
        self.sp_df = temp_df 
         
    def gen_stockpool_head(self,id_time_stamp,config={},sp_name0='') :
        ''' 
        generate stock pool using input.
        stockpool is a subset of whole asset universe 
        since input dataframe might include too many columns, we only choose what we want and 
        output:
        1, stock pool table which include all basic and fundamental information we need 
        2, head dict(json) file which include: generate and update time, operator,market, country
        3, 
        '''  
        stockpool_head = {} 
        ### ref. previous portfolio file. 
        ## get stockpool id using  time stamp
        # import sys
        # sys.path.append("..")
        # from db.basics import time_admin
        # time_admin1 = time_admin()
        # time_stamp = time_admin1.get_time_stamp()
        if config == {} :
            ## Basic info
            if sp_name0 == '' :
                stockpool_head["name_sp"] = "name_sp_" + id_time_stamp
                stockpool_head["id_sp"] = "id_sp_" + id_time_stamp
                stockpool_head["sp_id_time"]= id_time_stamp
            else :
                stockpool_head["name_sp"] = "name_sp_" + sp_name0
                stockpool_head["id_sp"] = "id_sp_" +id_time_stamp+"_"+sp_name0
                stockpool_head["sp_id_time"]= id_time_stamp
                
            stockpool_head["code"] = ""
            stockpool_head["code_wind"] = ""
            stockpool_head["date_update"] = "" # derived from "Date"
            stockpool_head["date_start"] = ""
            stockpool_head["date_stop"] = ""
            stockpool_head["bsh"] = "" # buy,sell,hold previous:B/S/H
            stockpool_head["value"] = ""    # stock attribution in value     
            stockpool_head["growth"] = ""   # stock attribution in growth
            stockpool_head["pnl_last"] = 0.0  # profit or loss in last trade 
            stockpool_head["pnl_pct_last"] = 0.0  # percentage of profit or loss  in last trade 
            stockpool_head["w_optimal"] = 0.0 # derived from "w_ideal"
            stockpool_head["w_max"] = 0.0 # maximum weight should not exceed such level
            # stockpool_head["w_ideal"] = 0.0
            stockpool_head["if_Hold"] = 0
            stockpool_head["if_ST"] = 0
            stockpool_head["info"] = "" 
            stockpool_head["filename_sps_df"] = "" 


        return stockpool_head 
 

###################################################
class update_stockpools():
    def __init__(self, id_time_stamp,temp_head,temp_df,sp_name0=''):
        self.sp_name = sp_name0 
        self.sp_head = temp_head
        self.sp_df = temp_df 



