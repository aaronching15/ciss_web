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
        print("按照季度日期，将原有权重转化为总和为0.95的权重")
        print("weight_rebalance(self,para_ana,file_name_csv,file_path ,if_nan ) ")
        
        print()
        print("Choice 4：按给定column名字将内部的指标分别计算标准分值，并控制异常值的影响")
        print("indicators2score_1p(self,file_name_csv,file_path,col_name ,para_w ) :")
        ###  

        return 1 
    
    def weight_rebalance(self,para_ana,file_name_csv,file_path ,if_nan ) :
        ###  按照季度日期，将原有权重转化为总和为0.95的权重
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

    def indicators2score_1p(self,code_index,temp_date,file_name_csv,file_path,col_name ,para_w ) :
        ###  Choice 4：按给定column名字将内部的指标分别计算标准分值，并控制异常值的影响
        ###  sicne 191112
        
        # from x1	x2	y1	y2	z1	z2	ind，to  sum	x	y	z	
        #################################################################################
        ### Initialization 
        import pandas as pd  
        df_raw = pd.read_csv(file_path + file_name_csv ,encoding="gbk"  )
        # df_raw = df_raw.sort_values(by=col_name)
        print(df_raw.head()  )
        df_raw["weight"] = 0  
        ### preperation,将指标列潜在的str转换为数值格式
        col_list = ["x1","x2","y1","y2","z1","z2"]
        # df_raw[ col_list]
        df_raw[ col_list ]= df_raw[ col_list ].apply(pd.to_numeric)
        ### i= 0 means we have no previous portfolio 
        i = 0    
        # col_name = "ind"
        list_ind= df_raw[col_name].drop_duplicates().values
        for temp_ind in list_ind : 
            temp_df = df_raw[ df_raw["ind"]== temp_ind ]
            ### function for extreme value 
            for temp_x in ["x1","x2","y1","y2","z1","z2"] :
                ### 判断数值里是否有超过3倍标准差的，如果有要换成3倍标准差
                # temp_x ="x"
                temp_s = "s_" + temp_x 
                
                df_raw.loc[temp_df.index, temp_s] = 0.0
                # print("DEBUG=============",temp_x)
                # print( df_raw.describe() )
                temp_std =df_raw.loc[temp_df.index, temp_x].std()
                temp_mean =df_raw.loc[temp_df.index, temp_x].mean()
                temp_max =min( df_raw.loc[temp_df.index, temp_x].max(),temp_mean +3*temp_std )
                temp_min =max(df_raw.loc[temp_df.index, temp_x].min(),temp_mean -3*temp_std )

                for temp_i in temp_df.index :
                    if df_raw.loc[temp_i, temp_x] > temp_mean +3*temp_std :
                        df_raw.loc[temp_i, temp_x] = temp_mean +3*temp_std
                    elif df_raw.loc[temp_i, temp_x] < temp_mean -3*temp_std :
                        df_raw.loc[temp_i, temp_x] = temp_mean -3*temp_std
                    ### 计算标准分值
                    temp_v =(df_raw.loc[temp_i, temp_x] - temp_min)/( temp_max- temp_min)
                    df_raw.loc[temp_i, temp_s] =temp_v
                
            ### Calculate x,y,z
            # print("asd==========\n", df_raw.columns )
            
            temp_s = "s_x"
            df_raw.loc[temp_df.index,temp_s] =  df_raw.loc[temp_df.index,"s_x1"] +df_raw.loc[temp_df.index,"s_x2"]  
            temp_s = "s_y"
            df_raw.loc[temp_df.index,temp_s] =  df_raw.loc[temp_df.index,"s_y1"] +df_raw.loc[temp_df.index,"s_y2"]  
            temp_s = "s_z"
            df_raw.loc[temp_df.index,temp_s] =  df_raw.loc[temp_df.index,"s_z1"] +df_raw.loc[temp_df.index,"s_z2"]  
            ### summary
            temp_s = "s_total"
            df_raw.loc[temp_df.index,temp_s] =df_raw.loc[temp_df.index,"s_x"]*para_w[0] +df_raw.loc[temp_df.index,"s_y"]*para_w[1]+df_raw.loc[temp_df.index,"s_z"]*para_w[2]    
            ### weight 
            df_raw.loc[temp_df.index,"weight_ind1"] =  df_raw.loc[temp_df.index,temp_s]/df_raw.loc[temp_df.index,temp_s].sum()

        ### weight 
        df_raw["weight"] =  df_raw[temp_s]/df_raw[temp_s].sum()

        ### Saved to csv 
        df_raw = df_raw.sort_values(by= "no")
        df_raw.to_csv(file_path+"output_esti2w_"+code_index+"_"+temp_date+".csv",encoding="gbk"  )

        df_output = df_raw
        return df_output
    def weight2weight_sub2(self,df_output,col_name ,code_index,temp_date ,file_path) :
        ### Choice 5：根据给定行业或日期column，按指数成分里的细分组合计算细分类别里的权重
        ### sicne 191114
        
        #################################################################################
        ### Initialization 
        import pandas as pd   
        
        print(df_output.head()  )
        df_output["weight_sub"] = 0  
        # col_name = "ind"
        list_ind= df_output[col_name].drop_duplicates().values
        print("ind ",list_ind)
        for temp_ind in list_ind : 
            
            temp_df = df_output[ df_output["ind"]== temp_ind ]
            print(df_output.loc[temp_df.index,"w_csi800"].sum() )
            
            df_output.loc[temp_df.index,"weight_sub"]=df_output.loc[temp_df.index,"w_csi800"]/df_output.loc[temp_df.index,"w_csi800"].sum()
            # print(df_output.loc[temp_df.index,"weight_sub"] )
        ### save to csv 
        
        df_output = df_output.sort_values(by= "no")
        df_output.to_csv(file_path+"output\\"+"output_esti2w_"+code_index+"_"+temp_date+".csv",encoding="gbk"  )

        return df_output

    def weight2weight_sub(self,file_name_csv,file_path,col_name ,para_w ) :
        ### Choice 5：根据给定行业或日期column，按细分组合计算细分类别里的权重
        ### sicne 191113
        # col_name = "ind"
        #################################################################################
        ### Initialization 
        import pandas as pd  
        df_raw = pd.read_csv(file_path + file_name_csv ,encoding="gbk"  )
        
        print(df_raw.head()  )
        df_raw["weight_sub"] = 0  

        list_ind= df_raw[col_name].drop_duplicates().values
        for temp_ind in list_ind : 
            
            temp_df = df_raw[ df_raw["ind"]== temp_ind ]
            print("ind ", df_raw.loc[temp_df.index,"weight"].sum() )
            
            df_raw.loc[temp_df.index,"weight_sub"]=df_raw.loc[temp_df.index,"weight"]/df_raw.loc[temp_df.index,"weight"].sum()
            # print(df_raw.loc[temp_df.index,"weight_sub"] )
        ### save to csv 
        df_output= df_raw 
        df_output.to_csv(file_path+"output.csv",encoding="gbk"    )

        return df_output
    
    def weight2wind_pms(self,code_index ) :
        ### 6：导入季度调整的BL行业组合权重，生成Wind可以识别的季度调整文件
        import os
        
        import pandas as pd 
        ''' 需要匹配 test_bl_rc.py 里的顺序
        0 能源
        1 材料
        2 工业
        3 可选消费
        4 日常消费
        5 医疗保健
        6 金融
        7 信息技术
        8 电信服务
        9 公用事业
        10 房地产'''

        ind_list =["能源","材料","工业","可选消费","日常消费","医疗保健","金融","信息技术","电信服务","公用事业","房地产"]
        file_path = "D:\\CISS_db\\db_bl\\data\\output\\"
        date_list = []
        for temp_y in ["2014","2015","2016","2017","2018","2019"]:
            for temp_m in ["0301","0601","0901","1201"] :
                temp_date = temp_y +temp_m

                if temp_date not in ["20140301","20140601","20191201"] :
                    date_list = date_list + [temp_date ]                
        
        print( date_list  ) 
        for temp_i in range( len(ind_list)) :
            temp_ind = ind_list[temp_i]
            i=0
            for temp_date in date_list :
                print(temp_date )
                file_name_csv = "w_"+  temp_date+"_"+str(temp_i)+".csv"
                ### csv文件名有中文时要加 "rb", 报错提示增加 engine="python" ，只有1个column
                ### 而且str在split(",")时会因为有的数值0,0 导致取数有问题。最好从源头上避免中文。
                # df_raw = pd.read_csv( file_path + file_name_csv ,"rb",encoding="gbk",engine="python"  )               
                # ### 注意：导入的是只有1个columns的文件
                # col_list = df_raw.columns[0].split(",")
                
                # for temp_i in df_raw.index :
                #     for j in range(len(col_list )) :
                #         temp_list = df_raw.loc[temp_i,df_raw.columns[0] ].split(",")
                #         df_raw.loc[temp_i,col_list[j] ] = temp_list[j]
                
                df_raw = pd.read_csv( file_path + file_name_csv  ,encoding="gbk"  )

                df_raw["date"] = temp_date 
                ### change from str to float 
                print("debug========" )
                print( df_raw.head()  )
                

                df_raw["w_bl"]= df_raw["w_bl"].astype("float")
                df_raw["w_mkt"]= df_raw["w_mkt"].astype("float64")
                ### Notes:剔除权重小于0.003，且权重之和等于0.95
                df_raw =df_raw[ df_raw["w_bl"]>=0.003 ]
                df_raw["w_bl"] = df_raw["w_bl"]/df_raw["w_bl"].sum() 
                df_raw["w_bl"] = df_raw["w_bl"] *100
                df_raw["w_bl"] = df_raw["w_bl"]/df_raw["w_bl"].sum() *95
                ### 导入wind-PMS需要百分位的数值
                
                
                df_raw =df_raw[ df_raw["w_mkt"]>=0.003 ]
                df_raw["w_mkt"] = df_raw["w_mkt"]/df_raw["w_mkt"].sum() 
                df_raw["w_mkt"] = df_raw["w_mkt"] *100
                df_raw["w_mkt"] = df_raw["w_mkt"]/df_raw["w_mkt"].sum() *95

                if i<1 :
                    df_ind = df_raw
                else :
                    df_ind = df_ind.append(df_raw, ignore_index=True) 
                
                i=i+1  
            

            # print("debug========",len(df_ind.index) )
            # print(df_ind.head(2)  )
            # print(df_ind.tail(2)  )
            
            ### save single allocation plan of single industry to csv and in wind-PMS format
            df_wind_pms_bl = df_ind.loc[:,["code","w_bl","date"] ]
            df_wind_pms_mkt = df_ind.loc[:,["code","w_mkt","date"] ]

            df_wind_pms_bl = df_wind_pms_bl.sort_values(by="date",axis=0 )
            df_wind_pms_mkt = df_wind_pms_mkt.sort_values(by="date",axis=0 )

            df_wind_pms_bl.columns= ["证券代码","持仓权重","调整日期"]
            df_wind_pms_mkt.columns= ["证券代码","持仓权重","调整日期"]

            df_wind_pms_bl["成本价格" ] ="" 
            df_wind_pms_bl["证券类型"] = "股票"
            df_wind_pms_mkt["成本价格" ] ="" 
            df_wind_pms_mkt["证券类型"] = "股票"

            ### 
            file_name_pms_bl = "PMS_bl_"+code_index+"_"+temp_ind+"_"+date_list[0]+"_"+date_list[-1]+ ".csv"
            file_name_pms_mkt = "PMS_mkt_"+code_index+"_"+temp_ind+"_"+date_list[0]+"_"+date_list[-1]+ ".csv"
            file_name_w = "w_"+code_index+"_"+temp_ind+"_"+date_list[0]+"_"+date_list[-1]+ ".csv"
            
            df_wind_pms_bl.to_csv(file_path+ file_name_pms_bl )
            df_wind_pms_mkt.to_csv(file_path+ file_name_pms_mkt )
            df_ind.to_csv(file_path+file_name_w)





        df_output=file_name_w 
        return df_output