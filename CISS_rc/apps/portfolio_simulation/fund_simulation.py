# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
拆基仿真研究支持组件

MENU :
### Given table name 
### 
### 
### 

功能

todo：
'''
#################################################################################
### Initialization 
import sys
sys.path.append( "C:\\zd_zxjtzq\\ciss_web\\CISS_rc\\db\\db_assets\\" )

import pandas as pd 
import numpy as np 

#################################################################################
###  
class fund_simu():
    ### 类的初始化操作 
    def __init__(self, Sys_name ):
        self.Sys_name =Sys_name

    def print_info(self) :
        ### Print infomation for all modules 


        return 1 
    
    def weight_rebalance(self,para_ana,file_name_csv,file_path ,if_nan ) :
        ###  需要把权重调整计算的部分用python模块实现，可以节省大量时间
        ###  sicne 191021
        '''
        需求分析：1，对本步骤内的指标进行打分；2，根据上一步骤权重和本步骤分值，计算新的权重

        file_path = "C:\\zd_zxjtzq\\rc_reports_cs\\行业量化_医疗保健\\"
        file_data_raw = "data_raw_w_rebal.csv"
        '''
        #################################################################################
        ### Initialization
        import pandas as pd  
        df_raw = pd.read_csv(file_path + file_name_csv   ) 
        ### columns= ["date_ann","ana1","ana2","ana3","ana4","w_pre"]
        # df_raw.index = df_raw["date_ann"] # 有很多重复项，因此不太合适
        df_output = df_raw
        df_output["s_sum"]= 0

        if if_nan ==1 :
            #################################################################################
            ### 对于不同公告日期，计算日期内指标。
            # df_raw =df_raw.drop(["date_ann"] , axis=0 )
        
            date_list_i =[] 
            for temp_i in df_output.index :
                temp_date = df_output.loc[temp_i, "date_ann" ]
                if temp_date not in date_list_i :                 
                    df_raw_sub = df_output[ df_output["date_ann"] ==temp_date ]

                    ### calculate score1 for ana1 
                    for temp_col in df_raw_sub.columns :
                        if temp_col[:3] =="ana"  :
                            temp_max = df_raw_sub[temp_col].max()
                            temp_min = df_raw_sub[temp_col].min()
                            
                            df_output.loc[df_raw_sub.index, "s_"+temp_col ] = (df_output.loc[df_raw_sub.index, temp_col ] -temp_min)/(temp_max-temp_min)
                            date_list_i = date_list_i +[ temp_date ]
                            ### add to sum score, weuse para_ana["ana2"] here 
                            df_output.loc[df_raw_sub.index, "s_sum"]=df_output.loc[df_raw_sub.index, "s_sum"] +df_output.loc[df_raw_sub.index, "s_"+temp_col ]*para_ana[temp_col] 
            
            ### 对于不同公告日期，将日期内组合权重进行调整。 adjust previous weight w_pre to w_adj
            date_list_i =[] 
            temp_col ="w_pre"
            for temp_i in df_output.index :
                temp_date = df_output.loc[temp_i, "date_ann" ]
                if temp_date not in date_list_i :   
                    df_raw_sub = df_raw[ df_raw["date_ann"]==temp_date  ]

                    temp_sumproduct = (df_output.loc[df_raw_sub.index, "w_pre"]*df_output.loc[df_raw_sub.index, "s_sum"] ).sum()
                    temp_sum = df_output.loc[df_raw_sub.index, "w_pre"].sum()
                    
                    for temp_i2 in df_raw_sub.index :
                        df_output.loc[temp_i2, "w_adj"]= temp_sum* df_raw_sub.loc[temp_i2,"w_pre"] *df_raw_sub.loc[temp_i2,"s_sum"]/ temp_sumproduct 
                    
                    date_list_i = date_list_i +[ temp_date ]
        else :
            date_list_i =[] 
            temp_col ="w_pre"
            for temp_i in df_output.index :
                temp_date = df_output.loc[temp_i, "date_ann" ]
                if temp_date not in date_list_i :   
                    df_raw_sub = df_raw[ df_raw["date_ann"]==temp_date  ] 

                    temp_sum = df_output.loc[df_raw_sub.index, "w_pre"].sum()
                    
                    for temp_i2 in df_raw_sub.index :
                        df_output.loc[temp_i2, "w_adj"]= df_raw_sub.loc[temp_i2,"w_pre"] /temp_sum *0.94 
                    
                    date_list_i = date_list_i +[ temp_date ]
        
        ### save output to csv file 

        file_data_out = "data_raw_w_rebal_out.csv"
        df_output.to_csv(  file_path + file_data_out )

        return df_output
    
    def weight_adj_anndate(self,file_name_csv,file_path ) :
        ###  Choice 2：半年报累加权重计算
        ###  sicne 191023
        '''
        ana分析：参考H列ANN_DATE，通常1、7月披露季度前十大持仓后，3或4、 8月会披露半年报的全部或剩余持仓，
        这会导致一定的问题需要用python进行处理。合理的做法一是统一延后调整、这样会损失时效性；
        二是提前调整，之后不调整；这样会错过非权重股的机会；
        三先按照top10调整仓位，之后再按照后续披露事项调整；这样做的问题是仓位角度，需要先用前十大打满仓位，
        之后再根据持仓比例卖出非前十大部分持仓的权重，买入新增的股票。
        info:基金半年度报告：基金管理人应当在上半年结束之日起六十日内，编制完成基金半年度,
            基金管理人应当在每年结束之日起九十日内，编制完成基金年度报告，并将年度报告正文登载于网站
        逻辑：按照日期升序排列，半年报和年报可能发生的月份： 1，2，3，7，8都有可能出现，其中1，7会
            和q4、q2季度后15个交易日内披露重叠；季度调整对应的是1、4、7、10月份。
            即便间隔1个月，期间对应的一致预期数据也有可能发生调整，因此再做一次计算也是合理的。
        办法：如果1、7月出现了某天更新，合计权重加上一次调整权重之和小于1.00，
            且股票单票最大权重小于上一次调整的平均权重，则判定为部分权重信息，此时进行加总计算权重。

        file_path = "C:\\zd_zxjtzq\\rc_reports_cs\\行业量化_医疗保健\\"
        file_data_raw = "data_raw_w_adj_anndate.csv"
        columns= [date_quarter	w_pre	date_ann code_wind]
        '''
        #################################################################################
        ### Initialization 
        import pandas as pd  
        df_raw = pd.read_csv(file_path + file_name_csv   )  
        ### 剔除持仓比例小于 0.3%的，这里对应的值应该是 0.3
        df_output = df_raw[ df_raw["w_pre"] >= 0.3  ]
        
        ### 日期升序排列
        df_output=df_output.sort_values(by="date_ann",axis=0,ascending=True)
        
        date_list_i =[]
        # define last index, last sum of wegihts and last minimum weight  
        temp_i_pre = 0 
        w_sum_pre = 1.0 
        w_min_pre = 0.00
        date_quarter_pre = 20000101
        for temp_i in df_output.index :
            temp_date = df_output.loc[temp_i, "date_ann" ]
            temp_date_pre = df_output.loc[ temp_i_pre,"date_ann" ]
            temp_month = str( temp_date )[4:6]
            
            if temp_date not in date_list_i :  
                ### 半年报和年报可能发生的月份： 1，2，3，7，8
                if temp_month in ["01","02","03","07","08"] :
                    df_raw_sub = df_output[ df_output["date_ann"] ==temp_date ]
                    w_sum = df_raw_sub["w_pre"].sum()
                    w_max= df_raw_sub["w_pre"].max()
                    w_min= df_raw_sub["w_pre"].min()
                    date_quarter  = df_output.loc[temp_i,"date_quarter"]

                    print("Working on " ,temp_date , temp_date_pre , date_quarter,date_quarter_pre,date_quarter_pre == date_quarter ,temp_date > temp_date_pre )
                    ### 判断是否属于同一个财务季度，且当前披露日大于前一个披露日
                    if date_quarter_pre == date_quarter and temp_date > temp_date_pre  :
                        print("IF: ", w_max <= w_min_pre and w_sum+w_sum_pre <= 1,w_max <= w_min_pre ,w_sum+w_sum_pre <= 100 )
                        ### 判断是否属于半年报只有非前十权重股的情况
                        ### TODO 更好的选择可能是加入代码列，匹配是否有重复出现的代码，当前的 w_sum+w_sum_pre <= 100 还是有点问题
                        # if w_max <= w_min_pre and w_sum+w_sum_pre <= 100:
                        if w_max <= w_min_pre and w_sum+w_sum_pre <= 100:
                            # notes：w_sum_pre<w_sum 不一定成立，对于持仓非常分散的基金来说
                            ### create new weight for current date_ANN period 
                            ### step1 get weights from previous date_ann
                            
                            df_raw_sub_pre = df_output[ df_output["date_ann"] ==temp_date_pre ]
                            df_raw_sub_pre["date_ann"] = temp_date

                            # print("df_raw_sub_pre \n" ,df_raw_sub_pre )
                            ### step2 change date_add and append to original matrix
                            df_output = df_output.append( df_raw_sub_pre,ignore_index=False   )

                            # print("Length of df_output ", len( df_output.index) )
                    
                    ### Update previous w_sum and w_min
                    w_sum_pre = w_sum
                    w_min_pre = w_min
                    date_quarter_pre = date_quarter
                    

            date_list_i = date_list_i + [ temp_date ]
            
            ### Update all previous variables 
            temp_i_pre = temp_i 


        ### sort in ascending order 
        df_output = df_output.sort_values(by= "date_ann", ascending = True )
        ### save output to csv file 
        file_data_out = "data_raw_w_adj_anndate_out.csv"
        df_output.to_csv(  file_path + file_data_out ) 

        return df_output

    def weight_list_event(self,file_name_csv,file_path,col_list  ) :
        ###  Choice 3：将时间顺序的交易记录或事件记录转化成每一期的配置权重 
        ###  sicne 191105
        
        #################################################################################
        ### Initialization 
        import pandas as pd  
        df_raw = pd.read_csv(file_path + file_name_csv   )
        df_raw = df_raw.sort_values(by="date")

        df_raw["weight"] = 0  
        ### i= 0 means we have no previous portfolio 
        i = 0  
        done_index_list= []

        for temp_i in df_raw.index :
            if i == 0 :
                temp_df = pd.DataFrame( df_raw.loc[temp_i,:] ).T                 
                temp_df.loc[temp_i,"weight"] = 0.95 
                df_output = temp_df
                i= i+1 
                print( temp_df )
                done_index_list= done_index_list + [temp_i ]
            else :
                ### append event row to temp_df 
                temp_date = df_raw.loc[temp_i,"date"] 

                if temp_i in done_index_list :
                    ### pass because we have done 
                    pass ;
                else :

                    ### 如果同一日期有多个股票事件，则统一加入temp_df                
                    ### 检查持仓中是否已经有这个股票，如果有，增加amount的金额
                    temp_df_date = df_raw[df_raw["date"]== temp_date]  
                    print("temp_df_date \n",temp_df_date )

                    for temp_i2 in temp_df_date.index :
                        temp_code = temp_df_date.loc[temp_i2,"code"]
                        
                        df_samecode = temp_df[ temp_df["code"] ==temp_code  ]
                        print("df_samecode \n",df_samecode , len( df_samecode.index) )
                        if len( df_samecode.index) ==1 :
                            ### update amount and date 
                            temp_df.loc[ df_samecode.index[0], "amount" ] =temp_df.loc[ df_samecode.index[0], "amount" ]+temp_df_date.loc[temp_i2,"amount"] 
                            temp_df.loc[ df_samecode.index[0], "date" ] = temp_df_date.loc[temp_i2,"amount"]
                        else :
                            ### update only date
                            temp_df = temp_df.append( temp_df_date.loc[temp_i2,:] )

                        done_index_list= done_index_list + [temp_i2 ]
                    ### 改变日期
                    temp_df["date"] = temp_date 

                    temp_df.loc[:,"weight"] = temp_df.loc[:,"amount"] /temp_df["amount"].sum()* 0.95 
                    print(temp_df)
                    ### debug
                    # input2= input("Continue")
                    
                    ### append temp_df to df_output 
                    df_output = df_output.append( temp_df, ignore_index = True  )

        ### save output to csv file 
        file_data_out = "weight_list_event_out.csv"
        df_output.to_csv(  file_path + file_data_out ) 

        return df_output