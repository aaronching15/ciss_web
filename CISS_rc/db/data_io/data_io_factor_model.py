# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
notes: 导入的指标主要是基于沪深300成分股的已经计算好的csv文件

derived from data_io.py
date:last 200526 | since 180601
===============================================
'''
import sys,os
# 当前目录 C:\zd_zxjtzq\ciss_web\CISS_rc\db
path_ciss_web = os.getcwd().split("CISS_rc")[0]
path_ciss_rc = path_ciss_web +"CISS_rc\\"
sys.path.append(path_ciss_rc + "config\\" )
sys.path.append(path_ciss_rc + "db\\" )
sys.path.append(path_ciss_rc + "db\\db_assets\\" )
sys.path.append(path_ciss_rc + "db\\data_io\\" )

import pandas as pd
import numpy as np

#######################################################################
from data_io import data_io 

#######################################################################
class data_factor_model():
    def __init__(self):
        #######################################################################
        ### 导入配置文件对象，例如path_db_wind等
        sys.path.append(path_ciss_rc+ "config\\")
        from config_data import config_data_factor_model
        config_data_1 = config_data_factor_model()
        self.obj_config = config_data_1.obj_config

        self.data_io_1 = data_io()
        #######################################################################
        ### factor_model 目录位置：
        # self.obj_config["dict"]["path_factor_model"] = dict_config["path_ciss_db"] +"factor_model\\"
        path_factor_model = self.obj_config["dict"]["path_factor_model"]
        #######################################################################
        ### 导入日期list，日、周、月 | date_list_tradingday.csv  ...     
        file_name_month = self.obj_config["dict"]["file_date_month"]    
        # file_name_tradingday = self.obj_config["dict"]["file_date_tradingday"] 
        # file_name_week = self.obj_config["dict"]["file_date_week"]   

        df_date_month = pd.read_csv(self.obj_config["dict"]["path_dates"] + file_name_month )
        date_list_month = df_date_month["date"].values

        date_list_month.sort()
        # 日期升序排列
        self.obj_data_io = {}
        self.obj_data_io["dict"] = {}
        self.obj_data_io["dict"]["date_list_month"] = date_list_month


    def print_info(self):        
        print("   ")
        print("import_data_wds |导入wds表格列表和对应的公布日期列 keyword_anndate ")
        ### 多因子模型数据I/O
        print("import_data_opt |导入单一时期优化模型需要的数据，obj_para包括了基准指数和最新日期等信息 ")
        print("export_data_opt |输出优化模型结果和重要过程数据  ")

        ### 导入指标和因子数据I/O
        print("import_data_factor |导入已经下载的指标和因子数据 ")
        print("export_data_factor |输出已经下载的指标和因子数据 ")
        
        print("export_data_1factor |输出单因子分组模型结果和重要过程数据 ")

        ### 导入财务分析和财务模型相关指标I/O
        print("import_data_financial_ana |导入财务分析和财务模型相关指标 ")
        print("export_data_financial_ana |输出财务分析和财务模型相关指标 ")

        return 1

    def import_data_wds(self, obj_in ) :
        ### 导入wds表格列表和对应的公布日期列 keyword_anndate
        ########################################################################
        ### 导入表格列表
        file_name = obj_in["dict"]["file_name_columns_wds"] # "list_financial_columns_wds.csv"
        # C:\ciss_web\CISS_rc\apps\active_bm
        # path_name = self.obj_config["dict"]["path_active_bm"]
        path_name = self.obj_config["dict"]["path_apps"] + obj_in["dict"]["path_name_columns_wds"] 
        # including chinese char
        df_cols = pd.read_csv( path_name + file_name,encoding="gbk" )

        # print("df_cols \n " , df_cols.head() )

        table_list = list(df_cols["table_name"].drop_duplicates() )
        '''  ['AShareFinancialIndicator', 'AShareProfitExpress', 'AShareProfitNotice', 
        'AShareTTMAndMRQ', 'AShareTTMHis', 'AIndexConsensusData', 'AIndexConsensusRolling', 
        'AShareConsensusindex', 'AShareConsensusRollingData']
        '''
        # print(" table_list\n " ,  table_list )
        ########################################################################
        ### 对每个表格，导入关键词分类 "keyword_anndate"
        file_4_keyword_anndate = "log_data_wds_tables.csv"
        df_keyword_anndate = pd.read_csv( self.obj_config["dict"]["path_rc_data"] + file_4_keyword_anndate,encoding="gbk" )
        df_keyword_anndate =df_keyword_anndate[df_keyword_anndate["name_table"].isin(table_list)]
        print( df_keyword_anndate  )

        # notes: AShareTTMAndMRQ 是没有特定的anndate
        # for temp_table in table_list :
        #     print(temp_table )
        #     temp_keyword_anndate = df_keyword_anndate[ df_keyword_anndate["name_table"]==temp_table ]["keyword_anndate"].values[0]
        #     print( temp_keyword_anndate )

        ########################################################################
        ### 导入月末日期列表,一般默认从200501开始
        date_list_month = self.obj_data_io["dict"]["date_list_month"]
        # 'numpy.int64'> type(date_list_month[0])
        date_list_month_050101 = [ t for t in date_list_month if t >= 20050101 ]
        
        obj_in["dict"]["df_fi_cols"] = df_cols
        obj_in["dict"]["table_list_fi"] = table_list
        obj_in["dict"]["df_keyword_anndate"] = df_keyword_anndate
        obj_in["dict"]["date_list_month_050101"] = date_list_month_050101
        notes = "df_fi_cols,wds财务相关表格列表分类;table_list_fi，表格列表；df_keyword_anndate,用于匹配的日期csv文件的列关键词；date_list_month_050101，2005年开始的月末交易日。"
        obj_in["dict"]["notes"] = notes
        

        return obj_in

    def import_data_opt(self,obj_para):
        ### 导入单一时期优化模型需要的数据，obj_para包括了基准指数和最新日期等信息
        
        ########################################################################
        code_index = obj_para["dict"]["code_index"]
        # 20060228是第一次有factor weight的月份
        date_last_month = obj_para["dict"]["date_last_month"]

        ### 设定变量和导入数据，从factor_model\\文件夹获取
        path_factor_model = self.obj_config["dict"]["path_factor_model"]
        # 日期升序排列
        date_list_month = self.obj_data_io["dict"]["date_list_month"]
        # print("date_list_month ",date_list_month)
        path_factor_model_sub = path_factor_model+ code_index+ "\\"

        ########################################################################
        ### 给定日期，导入相关数据，20151231时count_month=6了，
        # 导入t时期指数成分 w_b=w_stock_bm,df_factor_weight，导入t+1，下个月末20160131时的股票收益率 ret_stock_change_np
        # notes:假定factor_weight_np第一行factor_weight_np[0] 对应的是每个股票在市值因子上的暴露
        # date_list_month_pre 是已经计算过的月份
        date_list_month_pre = [date for date in date_list_month if date<= date_last_month ]
        # date_list_month_pre 是根据最新日期还未计算的月份
        date_list_month = [date for date in date_list_month if date> date_last_month ]

        count_month = len(date_list_month_pre )
        # 取最后一个日期
        temp_date = date_list_month_pre[-1]
        temp_date_pre = date_list_month_pre[-2]
        if count_month > 6 :
            temp_date_pre_6m = date_list_month_pre[-6]
        else :
            temp_date_pre_6m = date_list_month_pre[0]

        ########################################################################
        ### 导入指数成分 df_index_consti

        from analysis_indicators import indicator_ashares,analysis_factor
        indicator_ashares_1 = indicator_ashares()
        analysis_factor_1 = analysis_factor()

        obj_in_index={} 
        obj_in_index["date_start"] = obj_para["dict"]["date_last_month"]
        obj_in_index["code_index"] = obj_para["dict"]["code_index"]
        obj_in_index["table_name"] = "AIndexHS300FreeWeight"

        obj_out_index = indicator_ashares_1.ashares_index_constituents(obj_in_index) 
        # code_list = obj_out_index["df_ashares_index_consti"]["S_CON_WINDCODE"].values
        # 获取指数成分股、权重
        df_index_consti = obj_out_index["df_ashares_index_consti"].loc[:, ["S_CON_WINDCODE","I_WEIGHT","TRADE_DT" ] ]
        ### 注意，将wind_code 按升序排列
        df_index_consti = df_index_consti.sort_values(by="S_CON_WINDCODE"  )
        print("df_index_consti   \n", df_index_consti.head().T )

        ### 指数权重成分 w_index_consti ,np.array | notes: "I_WEIGHT"的值7.5等，需要除以100
        w_index_consti = df_index_consti["I_WEIGHT"].values /100
        print("w_index_consti ", type(w_index_consti ) ) 

        ###########################################################################
        ### 仅保留沪深300成分股，保留仅属于本期的项目
        code_list_csi300 = list( df_index_consti[ "S_CON_WINDCODE"].values )

        ########################################################################
        ### 导入因子权重矩阵 factor_weight_np,df_factor_weight_20060228_000300.SH.csv
        '''
        因子列表：由factor_weight_np和len_factor决定
        Factor list: [
        'zscore_S_DQ_MV', 流通市值标准分
        'f_w_ic_ir_S_VAL_PCF_OCFTTM', 经营性现金流
        'f_w_ic_ir_amt_ave_1m_6m', 过去1个月和6个月的平均成交金额比
        'f_w_ic_ir_close_pct_52w', 收盘价在52周价格区间百分比
        'f_w_ic_ir_ep_ttm', 市盈率倒数
        'f_w_ic_ir_ma_20d_120d', 20天和120天均线比值
        'f_w_ic_ir_ret_accumu_20d', 20天累计收益率
        'f_w_ic_ir_ret_accumu_20d_120d', 20天和120天累计收益率比值
        'f_w_ic_ir_ret_alpha_ind_citic_1_120d', 相对于中信一级行业的120天相对收益
        'f_w_ic_ir_ret_alpha_ind_citic_1_20d', 相对于中信一级行业的20天相对收益
        'f_w_ic_ir_ret_averet_ave_20d_120d', 20天和120天平均收益率比值
        'f_w_ic_ir_ret_mdd_20d', 20天最大回撤
        'f_w_ic_ir_ret_mdd_20d_120d', 20天和120天最大回撤比值
        'f_w_ic_ir_roe_ttm', 净资产收益率
        'f_w_ic_ir_turnover_ave_1m_6m', 20天和120天换手率比值
        'f_w_ic_ir_volatility_std_1m_6m'，20天和120天波动率比值 ]
        '''
        file_name_output= "df_factor_weight_" +str( date_last_month) +"_"+ code_index +".csv"
        df_factor_weight  = pd.read_csv( path_factor_model_sub + file_name_output,index_col=0 )
        df_factor_weight = df_factor_weight[ df_factor_weight["date"]== date_last_month ]

        df_factor_weight = df_factor_weight[ df_factor_weight["wind_code"].isin(code_list_csi300 ) ]
        
        print("df_factor_weight_",len(df_factor_weight ) ) 
        # input1= input("Check to proceed......  ")
        if len(df_factor_weight ) > len(code_list_csi300) :
            asd
        # df_factor_weight to factor_weight_np
        # a.remove(value):删除列表a中第一个等于value的值；a.pop(index):删除列表a中index处的值；del(a[index]):删除列表a中index处的值
        # sub step 1:wind_code 升序排列：
        df_factor_weight = df_factor_weight.sort_values(by="wind_code")
        # sub step 2:剔除非因子值；因子icir不包括市值，需要先导入！
        col_list = list( df_factor_weight.columns.values ) 
        col_list.remove( "wind_code" )
        col_list.remove( "date" )
        print("col_list",col_list )

        # col_list 是有因子权重数据的代码列表，col_list_csi300是当期指数成分股
        ######################################################################## 
        ### 导入总市值因子、中信一级行业和其他因子，原始指标等 | "zscore_S_DQ_MV" | df_factor_20060228_000300.SH_20060228.csv
        file_name_output= "df_factor_" +str( date_last_month) +"_"+ code_index +"_"+str( date_last_month) +".csv"
        try:
            temp_df_factor = pd.read_csv( path_factor_model_sub + file_name_output,index_col=0 )
        except:
            # notes:20191129开始后缀没有日期了
            file_name_output= "df_factor_" +str( date_last_month) +"_"+ code_index +".csv"
            temp_df_factor = pd.read_csv( path_factor_model_sub + file_name_output,index_col=0 )
        
        ######################################################################## 
        ### 设置因子列表col_list ；第一行要设置为市值因子; notes: szcore不是ic_ir,可能会有点问题。
        col_list= [ "zscore_S_DQ_MV" ] + col_list
        '''第一个流通市值zscore_S_DQ_MV值在-0.59到7之间，均值0.0，这个对我们来说没有必要进行限制
        第一个必须是这个值.
        notes:200506选出以下9个因子:
        'zscore_S_DQ_MV', 流通市值标准分;'f_w_ic_ir_close_pct_52w', 收盘价在52周价格区间百分比
        'f_w_ic_ir_ep_ttm', 市盈率倒数;'f_w_ic_ir_ma_20d_120d', 20天和120天均线比值
        'f_w_ic_ir_ret_accumu_20d', 20天累计收益率；'f_w_ic_ir_ret_mdd_20d', 20天最大回撤
        'f_w_ic_ir_roe_ttm', 净资产收益率；'f_w_ic_ir_S_VAL_PCF_OCFTTM', 经营性现金流
        'f_w_ic_ir_turnover_ave_1m_6m', 20天和120天换手率比值;
        '''
        col_list= ['zscore_S_DQ_MV','f_w_ic_ir_close_pct_52w','f_w_ic_ir_ep_ttm','f_w_ic_ir_ma_20d_120d']
        col_list= col_list+['f_w_ic_ir_ret_accumu_20d','f_w_ic_ir_ret_mdd_20d','f_w_ic_ir_roe_ttm' ]
        col_list= col_list+['f_w_ic_ir_S_VAL_PCF_OCFTTM','f_w_ic_ir_turnover_ave_1m_6m' ]
        
        ### 用于组合优化设置的其他指标值,df_4opt,col_list_4opt
        '''ret_accumu_20d，ret_accumu_120d，20天和120天累计收益
        ret_alpha_ind_citic_1_120d: 120天相对于中信一级行业的收益
        ret_mdd_20d，ret_mdd_20d_mad
        ret_mdd_120d,这个没计入标准因子，只有原始指标值
        ret_mdd_20d_120d：20天内最大回撤比120天内最大回撤 =  (1+ret_mdd_20d)/((1+ret_mdd_120d)
        '''
        col_list_4opt = ["ret_accumu_20d","ret_accumu_120d","ret_mdd_20d","ret_mdd_120d"]
        
        ###
        for temp_i in df_factor_weight.index :
            temp_code = df_factor_weight.loc[temp_i, "wind_code" ]
            # find code and "zscore_S_DQ_MV" in temp_df_factor
            # print("temp_code ",temp_code )
            temp_df_factor_sub = temp_df_factor[ temp_df_factor["wind_code"]==temp_code ]
            if len( temp_df_factor_sub.index ) > 0 :
                temp_j = temp_df_factor_sub.index[0]
                df_factor_weight.loc[ temp_i, "zscore_S_DQ_MV" ] = temp_df_factor.loc[temp_j, "zscore_S_DQ_MV"  ]
                for temp_col in col_list_4opt:
                    df_factor_weight.loc[ temp_i, temp_col ] = temp_df_factor.loc[temp_j, temp_col ]
            else :
                print("No record for code ", temp_code  )
                df_factor_weight.loc[ temp_i, "zscore_S_DQ_MV" ] = -1
                for temp_col in col_list_4opt:
                    df_factor_weight.loc[ temp_i, temp_col ] = 0.0
        ### df填充空值,取较小值
        df_factor_weight = df_factor_weight.fillna(-0.00001 )

        df_4opt = df_factor_weight.loc[:,col_list_4opt ]
        df_4opt.index = df_factor_weight["wind_code"]

        ### 和其他因子
        df_factor_weight_values = df_factor_weight.loc[:,col_list ]
        df_factor_weight_values.index = df_factor_weight["wind_code"]
        # print( "df_factor_weight_values"  )
        # example,from 300*16 to 16*300,shape of factor_weight_np 16*300
        factor_weight_np = df_factor_weight_values.T.values

        len_factor = np.size(factor_weight_np,0 )

        # 把 factor_weight_np 中nan替换为较低值 -0.5
        # where_are_NaNs = np.isnan(d)  >>> d[where_are_NaNs] = 0
        where_nan = np.isnan( factor_weight_np ) 
        factor_weight_np[ where_nan] = -0.5 

        print("Shape of factor_weight_np" ,factor_weight_np.shape )

        # 中信一级行业 || df_ind_code 是代码升序排列的行业分类
        df_ind_code = temp_df_factor[ temp_df_factor["date"]==date_last_month ]  
        df_ind_code = df_ind_code.loc[:, ["wind_code","citics_ind_code_s_1"] ]  
        df_ind_code = df_ind_code.sort_values(by="wind_code")

        ########################################################################
        ### 2,ret_stock_change_np 当月的收益率| 给定日期，导入指数成分股、权重和当月的收益率
        obj_in_index={} 
        obj_in_index["date_pre"] = temp_date_pre
        obj_in_index["date"] = temp_date
        obj_in_index["df_change"] = df_index_consti
        obj_in_index["df_change"]["wind_code"] = obj_in_index["df_change"]["S_CON_WINDCODE"]
        # print("df_index_consti" ,obj_in_index["df_change"].head()  ) 
        obj_in_index = indicator_ashares_1.ashares_stock_price_vol_change( obj_in_index )

        ret_stock_change_np =  obj_in_index["df_change"]["s_change_adjclose"].values

        ### ret_stock_change_6m_np,近120天收益率;temp_date_pre_6m
        obj_in_index["date_pre"] = temp_date_pre_6m
        obj_in_index = indicator_ashares_1.ashares_stock_price_vol_change( obj_in_index )
        ret_stock_change_6m_np =  obj_in_index["df_change"]["s_change_adjclose"].values

        ########################################################################
        ### 构建行业暴露矩阵ind_code_np和上下限ind_lb,ind_ub;ind_code_np矩阵取值都是1或0，其中每一行对应一个行业，
        # 每一行与最优权重变量相乘得到最优组合在某一行业的权重，ind_lb[i]对应了第i个行业的权重下限
        # ind_ub[i]对应第i个行业的权重上限。ind_bm:基准组合的行业权重。
        # 从 df_factor_20060228.csv 里获取行业分类 "citics_ind_code_s_1"
        # df_ind_code.columns ["wind_code","citics_ind_code_s_1"]
        # notes: df_ind_code一般多少有几个code是空值，np.nan,也应该给与一定的权重。例如200602的600087.SH长油。
        # notes:ind_code_list数值从小到大排列，最后一个可能是nan
        ind_code_list = list( df_ind_code["citics_ind_code_s_1" ].drop_duplicates() )
        ind_code_list.sort()
        print("ind_code_list",ind_code_list ) 

        len_stock = len(df_ind_code.index )
        ind_code_np = np.zeros( (len(ind_code_list),len_stock ) )

        df_ind_code_m = pd.DataFrame(ind_code_np ,index=ind_code_list,columns = df_ind_code["wind_code"] )

        for temp_i in df_ind_code.index :
            temp_code = df_ind_code.loc[temp_i, "wind_code"] 
            temp_ind_code = df_ind_code.loc[temp_i, "citics_ind_code_s_1"] 
            df_ind_code_m.loc[temp_ind_code,temp_code] = 1 

        print("ind_code_np from df_ind_code_m"  )
        # from df to np.arrays 
        ind_code_np = df_ind_code_m.values

        ########################################################################
        ### 构建个股权重矩阵 w_stock_np，对于N个股票，N*N矩阵，每行仅对角线1个值为1，其余为0
        w_stock_np = np.zeros( (len_stock ,len_stock ) )
        for temp_i in range( len_stock ) :
            w_stock_np[temp_i][temp_i] = 1 

        print("w_stock_np")

        ########################################################################
        ### 保存变量到相关对象
        obj_out = obj_para
        obj_out["date_list_month_pre"] = date_list_month_pre
        obj_out["date_list_month"] = date_list_month
        obj_out["df_index_consti"] = df_index_consti
        obj_out["w_index_consti"] = w_index_consti
        obj_out["code_list_csi300"] = code_list_csi300
        obj_out["col_list"] = col_list
        obj_out["factor_weight_np"] = factor_weight_np
        obj_out["len_factor"] = len_factor
        obj_out["df_ind_code"] = df_ind_code
        obj_out["ret_stock_change_np"] = ret_stock_change_np
        obj_out["ret_stock_change_6m_np"] = ret_stock_change_6m_np
        obj_out["ind_code_list"] = ind_code_list
        obj_out["len_stock"] = len_stock
        obj_out["ind_code_np"] = ind_code_np
        obj_out["w_stock_np"] = w_stock_np
        obj_out["df_4opt"] = df_4opt
        obj_out["col_list_4opt"] = col_list_4opt
        # 单因子回测用
        obj_out["df_factor_weight"] = df_factor_weight

        return obj_out


    def export_data_opt(self,obj_perf_eval, obj_opt) :
        ### 输出优化模型结果和重要过程数据
        '''obj_opt 中包括了优化模型结果和计算过程的各类信息
        1,优化模型结果和输出信息result：res.fun,success,x
        2，优化模型参数para:
        3,优化模型输入变量input:
        4,优化模型输入变量参数input_para:

        notes:
        obj_para 包括了重要的输入参数入指数代码、日期等。
        obj_opt["dict"] == obj_para["dict"]
        obj_perf_eval["df_perf_eval"]，每一期的组合未来1m和6m收益
        obj_perf_eval[ 20060228 ]："df_"+ str(temp_index),当期组合列表
        obj_opt["dict"]["id_output"] 指的是输出文件夹目录名称

        '''
        ########################################################################
        ### 设置文件命名和导出路径等
        path_factor_model = self.obj_config["dict"]["path_factor_model"]
        # 日期升序排列
        date_last_month = obj_opt["dict"]["date_last_month"]
        # print("date_last_month ",date_last_month)
        path_factor_model_sub = path_factor_model+ obj_opt["dict"]["code_index"] + "\\"

        path_export = path_factor_model_sub +"\\export\\"+obj_opt["dict"]["id_output"]+ "\\"
        if not os.path.exists( path_export  ) :
            os.mkdir( path_export  )
        print("path_export: ",path_export  )
        ########################################################################
        '''obj_opt主要内容：
        1，"dict":字典，包括["code_index"]，["date_last_month"]等；
        2，"date_list_month_pre"：2005年以来到给定交易日的月份list
        2，"date_list_month" ：给定交易日到最新交易日例如202003的月份list
        2，"df_index_consti": df,指数成分
        3,"w_index_consti":array
        4,"code_list_csi300":list,
        5,"col_list": column list ,
        6,"factor_weight_np"
        7,"len_factor";8,"df_ind_code"
        9,"ret_stock_change_np"
        10,"ret_stock_change_6m_np"
        11,"ind_code_list"
        12~15,opt_model:"cons","fun","bnds","w_init"
        16,"res":{fun,jac,... }    
            jac：返回梯度向量的函数,array
            message: 'Positive directional derivative for linesearch'
            nfev: 27355
            nit: 92
            njev: 88
            status: 8
            success: False
            x: array()

        1,优化模型结果和输出信息result：res.fun,success,x
        2，优化模型参数para:
        3,优化模型输入变量input:
        4,优化模型输入变量参数input_para:
        '''
        import json
        
        ########################################################################
        ### 整合数据字典信息 obj_opt["dict"] ||把所有变量的中英文注释存在字典的"notes"里
        # list to string || string to list, list(str1 )
        obj_opt["dict"]["col_list_str"]  = ",".join( obj_opt["col_list"] )
        obj_opt["dict"]["len_factor"] = obj_opt["len_factor"] # 16个

        ## notes obj_opt["ind_code_list"]都是float类型，如 12.0，nan等
        # ind_code_list = []
        # for item in obj_opt["ind_code_list"]:
        #     try :
        #         ind_code_list = ind_code_list +[str(int(item) )]
        #     except :
        #         ind_code_list = ind_code_list +[ "nan" ]
        
        # # 直接用 ",".join( obj_opt["ind_code_list"]) 会出问题 
        # obj_opt["dict"]["ind_code_list"] = ",".join( ind_code_list )

        obj_opt["dict"]["keys_opt_model"] = "Keys in obejct obj_opt:cons,bnds,w_init,obj_fun"
        ### 1,优化模型结果和输出信息result：res.fun,res.success,res.message,x等 
        obj_opt["dict"]["keys_result"] = "Keys in obejct obj_opt.res: fun,message,success,x,status,nfev,nit,njev"
        # # TypeError: Object of type function is not JSON serializable
        # obj_opt["dict"]["result_x"] = obj_opt["res"]["x"]

        str_notes = ""
        str_notes = str_notes + "前缀keys_xxxx对应变量xxxx的主要keys；"
        str_notes = str_notes + "前缀opt_model_对应优化模型设置；前缀result_对应模型计算结果，其中fun是最小化的目标方程值，x是最优配置权重按股票代码升序排列,success对应是否找到最优解。"
        obj_opt["dict"]["notes"] = str_notes 

        # notes:TypeError: Object of type int64 is not JSON serializable；因为里边有int64，np.nan等
        # ans：先str()变成字符串 ||还没试过的方法二：直接把dict直接序列化为json对象）加上 cls=NpEncoder,data就可以正常序列化了
        file_name = "temp_obj_opt.json"
        with open( path_export+ file_name,"w+") as f :
            json.dump( str(obj_opt["dict"]) ,f  )
        print("Dict data saved in Json file \n",path_export )

        ### save dataframe to csv
        # 多期组合绩效
        temp_index = obj_opt["dict"]["date_last_month"]
        file_name ="df_perf_eval.csv"
        obj_perf_eval["df_perf_eval"].to_csv( path_export+ file_name,index=False)
        # 单期个股收益
        file_name ="df_"+ str(temp_index)+".csv"
        obj_perf_eval["df_"+ str(temp_index)].to_csv( path_export + file_name,index=False)
        # 单期中信行业收益
        file_name ="df_ind_"+ str(temp_index)+".csv"
        obj_perf_eval["df_ind_"+ str(temp_index)].to_csv( path_export + file_name,index=False)

        return obj_opt


    def import_data_factor(self,obj_port):
        ### 导入已经下载的指标和因子数据
        '''
        path = D:\db_wind\data_adj\ashare_ana\\
        file = ADJ_timing_TRADE_DT_20200522_ALL.csv
        '''
        import datetime as dt 
        ### parameters
        temp_date = obj_port["dict"]["date"] 
        ### 设定组合类型，例如成长内行业轮动、市场内大小流通市值轮动、不同行业成长和价值轮动
        # "market","value_growth","industry","mixed"=所有分组都考虑
        group_type = obj_port["dict"]["group_type"] 

        ### 导入T日股票类的分析指标
        path_ashare_ana = self.obj_config["dict"]["path_wind_adj"] + "ashare_ana\\"
        file_ashare_ana = "ADJ_timing_TRADE_DT_"+ str(temp_date ) +"_ALL.csv"
        obj_port["dict"]["path_ashare_ana"] = path_ashare_ana

        try :
            df_ashare_ana = pd.read_csv( path_ashare_ana+file_ashare_ana,encoding="gbk"  )
        except :
            df_ashare_ana = pd.read_csv( path_ashare_ana+file_ashare_ana)

        obj_port["df_ashare_ana"] =df_ashare_ana

        ### 剔除上市不足60天的股票；假设公司上市20天后价格恢复正常，因此T日时上市应满20+40天。
        ### 导入个股上市时间
        # ["S_INFO_WINDCODE","S_INFO_NAME","S_INFO_LISTBOARDNAME","S_INFO_LISTDATE","S_INFO_DELISTDATE"]
        # 17,300830.SZ,N金现代,创业板,20200506.0,NaN
        df_stock_des = obj_port["df_stock_des"]  
        col_list_stock_des = obj_port["col_list_stock_des"]  

        for temp_i in df_ashare_ana.index :
            temp_code = df_ashare_ana.loc[temp_i,"S_INFO_WINDCODE" ]
            # temp_date 是统一的，不需要获取， df_ashare_ana.loc[temp_i,"TRADE_DT" ]
            # 差temp_code的上市日期
            df_stock_des_sub = df_stock_des[ df_stock_des["S_INFO_WINDCODE"]==temp_code ]
            if len(df_stock_des_sub.index) > 0 :
                listdate = df_stock_des.loc[ df_stock_des_sub.index[0], "S_INFO_LISTDATE" ]
                # 20200506.0
                temp_date_dt = dt.datetime.strptime( str(temp_date) ,"%Y%m%d" )
                listdate_dt = dt.datetime.strptime( str(int(listdate)) ,"%Y%m%d" )
                # print( temp_date_dt ,listdate_dt,  temp_date_dt-listdate_dt   )
                if temp_date_dt-listdate_dt >= dt.timedelta(days=60):
                    df_ashare_ana.loc[temp_i,"filter"] = 1 
            else :
                print(temp_code  )
        # 以20060105为例，股票数量从1350变成1349，少了一个上市不足60天的股票 000043.SZ
        df_ashare_ana =df_ashare_ana[ df_ashare_ana["filter"]==1 ]

        obj_port["df_ashare_ana"] =df_ashare_ana
        
        ### 导入T日市场状态
        path_abcd3d = self.obj_config["dict"]["path_ciss_db"] +  "timing_abcd3d\\"
        path_market_ana = path_abcd3d +  "market_status_group\\"
        obj_port["dict"]["path_abcd3d"] = path_abcd3d
        obj_port["dict"]["path_market_ana"] = path_market_ana

        file_market_ana = "abcd3d_market_ana_trade_dt_" +str(temp_date ) + ".csv"
        try :
            df_market_ana = pd.read_csv( path_market_ana+file_market_ana,encoding="gbk" )
        except :
            df_market_ana = pd.read_csv( path_market_ana+file_market_ana )

        obj_port["df_market_ana"] =df_market_ana
                
        return obj_port

    def export_data_factor(self,obj_port):
        ### 输出已经下载的指标和因子数据

        ### 设置输出文件夹
        port_name = obj_port["dict"]["port_name"] # "rc01"
        port_id = obj_port["dict"]["port_id"] # "200523"
        # 加权方式：mvfloat=市值加权,ew=等权重,growth = 成长加权,value=价值加权
        weighting_type = obj_port["dict"]["weighting_type"]
        # 定义股票池限定的范围
        sp_column = obj_port["dict"]["sp_column"] # ""
        ### 设定组合类型，例如成长内行业轮动、市场内大小流通市值轮动、不同行业成长和价值轮动
        # "market","value_growth","industry","mixed"=所有分组都考虑
        group_type = obj_port["dict"]["group_type"] # "industry"
        
        dir_export = port_name +"_" + port_id+ "_"+weighting_type +"_" + group_type +"_" + str(obj_port["dict"]["len_rebalance"] ) +"\\"
        path_export = obj_port["dict"]["path_abcd3d"] + dir_export
        if not os.path.exists( path_export  ) :
            os.mkdir( path_export  )
        print("path_export: ",path_export  )

        ### save obj_port["df_port_weight"] to csv as portfolio weight 
        file_export = "df_port_weight_" + str(obj_port["dict"]["date"]) + ".csv"
        obj_port["df_port_weight"].to_csv(path_export+file_export,index=False,encoding="gbk")
        obj_port["dict"]["path_export"] = path_export

        return obj_port

    def export_data_1factor(self,obj_perf_eval,obj_port):
        ### 输出单因子分组模型结果和重要过程数据
        ########################################################################
        ### 设置文件命名和导出路径等
        path_factor_model = self.obj_config["dict"]["path_factor_model"]
        # 日期升序排列
        date_last_month = obj_port["date"]
        # 增加因子名称
        path_factor_model_sub = path_factor_model+ obj_port["dict"]["code_index"]+"_"+ obj_port["dict"]["1factor"] + "\\"
        if not os.path.exists( path_factor_model_sub ) :
            os.mkdir( path_factor_model_sub )
        path_export = path_factor_model_sub +"\\export\\"
        if not os.path.exists( path_export  ) :
            os.mkdir( path_export  )
        print("path_export: ",path_export  )

        ########################################################################
        ### save dataframe to csv
        # 多期组合绩效
        temp_index = date_last_month
        file_name ="df_perf_eval.csv"
        obj_perf_eval["df_perf_eval"].to_csv( path_export+ file_name,index=False)
        # 单期个股收益
        file_name ="df_"+ str(temp_index)+".csv"
        obj_perf_eval["df_"+ str(temp_index)].to_csv( path_export + file_name,index=False) 

        return obj_perf_eval

    def import_data_financial_ana(self,obj_data):
        ### 导入财务分析和财务模型相关指标
        '''input :obj_data["dict"]["date_adj_port"]
        output:返回季度日期数据: obj_data["dict"]["date_q"]  
            obj_data["dict"]["date_q_pre"]; obj_data["df_ashare_ana"]   
        notes:
        1,为了避免股票上市初期大幅上涨的异常，需要剔除上市不足40天的股票
        数据更新：
        1，需要abcd3d_timing的数据表
        2，需要在披露截止日下载table=AShareFinancialIndicator，keyword=	REPORT_PERIOD
        '''
        data_io_1 = data_io()
        col_str = "Unnamed"
        ########################################################################
        ### step1 导入日期、行业、个股数据
        obj_date = {}
        obj_date["date"] = obj_data["dict"]["date_adj_port"]        
        temp_date = obj_data["dict"]["date_adj_port"]
                
        path_ashare_ana = self.obj_config["dict"]["path_wind_adj"]+"ashare_ana\\"
        file_ashre_ana_date = "ADJ_timing_TRADE_DT_" + temp_date + "_ALL.csv"
        try :
            df_ashare_ana = pd.read_csv(path_ashare_ana +file_ashre_ana_date ,encoding="gbk" )
        except :
            df_ashare_ana = pd.read_csv(path_ashare_ana +file_ashre_ana_date  )
        
        ### 删除部分列
        df_ashare_ana = data_io_1.del_columns( df_ashare_ana,col_str)

        ########################################################################
        ### 判断是否单一行业"single_industry",主要保存简码和中文名称
        # if "single_industry" in obj_data["dict"].keys() :
        # 设置长周期或短周期必选行业：如：22.0,24.0,25.0,27.0,30.0,34.0,40.0,41.0,42.0,63.0
        if obj_data["dict"]["single_industry"] >0 :
            df_ashare_ana = df_ashare_ana[ df_ashare_ana["ind_code"] == float( obj_data["dict"]["single_industry"] ) ]
            ### 导入中信二级行业 
            from db_assets.transform_wind_wds import transform_wds
            transform_wds_1 = transform_wds()
            ### 给定交易日获取清单中股票的所属行业，基于已经计算好了的数据
            code_list = df_ashare_ana[ "S_INFO_WINDCODE"].to_list()
            if_all_codes="1"
            object_ind = transform_wds_1.get_ind_date( code_list, obj_data["dict"]["date_adj_port"] ,if_all_codes )
            df_s_ind_out = object_ind["df_s_ind_out"]
            col_list_temp = ["citics_ind_code_s_3","citics_ind_code_s_2","citics_ind_code_s_1"]
            col_list_temp = col_list_temp +["citics_ind_name_1","citics_ind_name_2","citics_ind_name_3"  ]
            for temp_i in df_ashare_ana.index :
                temp_code = df_ashare_ana.loc[temp_i, "S_INFO_WINDCODE"]
                # find ind in object_ind["df_s_ind_out"]
                df_temp = df_s_ind_out[ df_s_ind_out["wind_code"]==temp_code ]
                if len( df_temp.index ) > 0 :
                    for temp_col in col_list_temp :
                        df_ashare_ana.loc[temp_i, temp_col] = df_temp[temp_col].values[0]
            ### 把ind_code 用中信二级行业 "citics_ind_code_2" 代替
            df_ashare_ana["ind_code"] = df_ashare_ana["citics_ind_code_s_2"]

        ########################################################################
        ### notes:为了避免股票上市初期大幅上涨的异常，需要剔除上市不足40天的股票
        # col_list = ["S_INFO_WINDCODE","S_INFO_LISTDATE","S_INFO_DELISTDATE"]
        obj_date = self.data_io_1.get_list_delist_day(obj_date)
        df_list_date = obj_date["df_list_delist_date"]   
        df_ashare_ana["filter"] = 0 
        # obj_date["df_list_delist_date"]         
        for temp_i in df_ashare_ana.index :
            temp_code = df_ashare_ana.loc[temp_i, "S_INFO_WINDCODE"]
            ### 计算个股上市日期
            df_sub = df_list_date[ df_list_date["S_INFO_WINDCODE"]==temp_code ]
            if len( df_sub.index )> 0 : 
                listing_dates =  float(temp_date) - df_sub["S_INFO_LISTDATE"].values[0]
                if listing_dates >=40 : 
                    df_ashare_ana.loc[temp_i, "filter"] = 1 
                else :
                    print("New stock? ", temp_code,temp_date, df_sub["S_INFO_LISTDATE"].values[0],listing_dates )
            
        df_ashare_ana = df_ashare_ana[ df_ashare_ana["filter"] == 1 ]
        df_ashare_ana = df_ashare_ana.drop("filter" ,axis=1 )
        
        ########################################################################
        ### 获取最近的2个财务披露日期

        obj_date = self.data_io_1.get_report_date(obj_date)    
        para_date = obj_date["list_para_date"] 
        date_q = obj_date["date_q"] 
        date_q_pre = obj_date["date_q_pre"] 
        
        ########################################################################
        ### step2 导入各类财务指标，若有则直接使用，若没有则按照分类从data_wds目录下获取
        '''
        指标列表：
        序号，简称，算法，对应指标，对应指标所在表格，重要参数列
        1，季度roe_ttm, 前2个季度的S_FA_ROE,S_FA_ROE,中国A股财务指标AShareFinancialIndicator，报告期，REPORT_PERIOD=20060930
        2，季度收入环比变动，{}，{营业收入同比增长率(%) S_FA_YOY_OR
        单季度.营业总收入同比增长率(%) S_QFA_YOYGR
        单季度.营业总收入环比增长率(%) S_QFA_CGRGR
        单季度.营业收入同比增长率(%) S_QFA_YOYSALES
        单季度.营业收入环比增长率(%) S_QFA_CGRSALES}，中国A股财务指标AShareFinancialIndicator，报告期，REPORT_PERIOD=20060930
        3，季度净利润扣非增速，{}，{单季度.净利润同比增长率(%) S_QFA_YOYPROFIT
        单季度.净利润环比增长率(%) S_QFA_CGRPROFIT}，中国A股财务指标AShareFinancialIndicator，报告期，REPORT_PERIOD=20060930
        4，季度盈利持续性,ROIC,"S_FA_ROIC",环比增长率,用ROIC替代-环比必须增加，总资产净利率,S_FA_ROA
            notes:永辉超市常年roic在4.7-12.2之间，2015年开始低于8.0；妙可蓝多在1.42~2.96之间；
            立讯精密10.02-19.93；北方华创3.8-6.89；
            201806圣农发展roic跌到历史记录地低3.83之后到年底反弹，股价在18年11月披露的3季报增长到8.41，股价开始起飞，到次年4-30时已经没有什么超额收益了。
            notes:圣农发展在20180213的FY1的预期净利润就9.78e多，之后慢慢增加到20180831的10.8e，再到181101的11.34e.
        5，季度资产质量：近2季度经营性现金流/总资产 or 经营性现金流/总负债;
            经营活动产生的现金流量净额/营业收入,S_FA_OCFTOOR;经营活动产生的现金流量净额/经营活动净收益,S_FA_OCFTOOPERATEINCOME
            单季度.经营活动产生的现金流量净额／营业收入,S_QFA_OCFTOSALES;单季度.经营活动产生的现金流量净额／经营活动净收益,S_QFA_OCFTOOR
            发展性指标(知识、资金、行业景气)：研发投入、员工增长；负债或股东权益增加；行业内收入增加
        6，估值：PE_FY1 <= 60，50 或 PE_FY1 <=,45，40；或者用同业前35%
        数据处理：
        1，对AShareFinancialIndicator表要按特定规则转换数据

        notes:1,未来每个行业要适用不同的指标参数，例如超市等消费者服务行业可能就不适合用roic指标。
        idea：从AShareFinancialIndicator表格里获取每个交易日所有股票的最近6个季度和5个年度的财务数据。
        '''
        ### 中国A股财务指标AShareFinancialIndicator，报告期，REPORT_PERIOD=20060930
        path = "D:\\db_wind\\data_wds\\"
        table_name = "AShareFinancialIndicator"
        file_name = "WDS_REPORT_PERIOD_" + date_q + "_ALL.csv"
        # print("Debug==== ",table_name,file_name)
        df_fi_indi = pd.read_csv(self.obj_config["dict"]["path_wind_wds"]+table_name+"\\"+ file_name   )
        file_name = "WDS_REPORT_PERIOD_" + date_q_pre + "_ALL.csv"
        df_fi_indi_q_pre = pd.read_csv(self.obj_config["dict"]["path_wind_wds"]+table_name+"\\"+ file_name   )

        col_list_add =[]
        ### 1,季度roe_ttm, 前2个季度的S_FA_ROE,S_FA_ROE, 净资产收益率 S_FA_ROE
        col_list_add = col_list_add + ["S_FA_ROE"    ]
        ### 2,单季度.营业总收入环比增长率(%) S_QFA_CGRGR;单季度.营业总收入同比增长率(%) "S_QFA_YOYGR"
        col_list_add = col_list_add + ["S_QFA_CGRGR","S_QFA_YOYGR"  ]
        ### 3,单季度.净利润同比增长率(%) S_QFA_YOYPROFIT;单季度.净利润环比增长率(%) S_QFA_CGRPROFIT
        col_list_add = col_list_add + ["S_QFA_YOYPROFIT","S_QFA_CGRPROFIT"  ]
        ### 4，季度盈利持续性:"S_FA_ROIC",ROIC环比增长率,总资产净利率,S_FA_ROA
        col_list_add = col_list_add + ["S_FA_ROIC","S_FA_ROA"  ]
        ### 5，季度资产质量：近2季度经营性现金流/总资产 or 经营性现金流/总负债，
            # 经营活动产生的现金流量净额/营业收入,S_FA_OCFTOOR;经营活动产生的现金流量净额/经营活动净收益,S_FA_OCFTOOPERATEINCOME
            # 单季度.经营活动产生的现金流量净额／营业收入,S_QFA_OCFTOSALES;
        col_list_add = col_list_add + ["S_FA_OCFTOOR","S_FA_OCFTOOPERATEINCOME","S_QFA_OCFTOSALES","S_QFA_OCFTOOR"    ]
        ### (已有数据)6，估值：PE_FY1 <= 60，50 或 PE_FY1 <=,45，40；或者用同业前35%

        ### Loop for code
        for temp_i in df_ashare_ana.index :
            temp_code = df_ashare_ana.loc[temp_i,"S_INFO_WINDCODE" ]
            
            ### 当季 date_q
            df_fa = df_fi_indi[ df_fi_indi["S_INFO_WINDCODE"]== temp_code ]
            if len(df_fa.index )>0 :
                for temp_col in col_list_add :
                    df_ashare_ana.loc[temp_i, temp_col ] =  df_fa[temp_col].values[0]
                    # if temp_col == "S_FA_ROE":
                    #     print("Debug==== ",temp_code,df_fa[temp_col].values[0])
            ### 上一季 date_q
            df_fa_q_pre = df_fi_indi_q_pre[ df_fi_indi_q_pre["S_INFO_WINDCODE"]== temp_code ]
            if len(df_fa_q_pre.index )>0 :
                for temp_col in col_list_add :
                    df_ashare_ana.loc[temp_i, temp_col +"_q_pre" ] =  df_fa_q_pre[temp_col].values[0]

            #######################################################################################
            ### 计算我们需要的值 ，"_diff" 和 "_diff_pct"
            ### 1，roe，关于数据：如果是第三季度S_FA_ROE是前3个季度的合计roe，如果是Q2择时前2个季度合计roe，需要用para_list前2项计算
            if "S_FA_ROE" in df_ashare_ana.columns :
                df_ashare_ana.loc[temp_i, "S_FA_ROE" +"_q_ave" ] = df_ashare_ana.loc[temp_i, "S_FA_ROE" ]/para_date[0]
                df_ashare_ana.loc[temp_i, "S_FA_ROE" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_FA_ROE" +"_q_ave" ] - df_ashare_ana.loc[temp_i, "S_FA_ROE" +"_q_pre" ]/para_date[1]
            
            ### 2,单季度.营业总收入环比增长率(%) S_QFA_CGRGR;单季度.营业总收入同比增长率(%) "S_QFA_YOYGR";都是单季度值，直接求差值即可
            if "S_QFA_CGRGR" in df_ashare_ana.columns :
                df_ashare_ana.loc[temp_i, "S_QFA_CGRGR" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_QFA_CGRGR" ] - df_ashare_ana.loc[temp_i, "S_QFA_CGRGR" +"_q_pre" ] 
                df_ashare_ana.loc[temp_i, "S_QFA_YOYGR" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_QFA_YOYGR" ] - df_ashare_ana.loc[temp_i, "S_QFA_YOYGR" +"_q_pre" ] 

            ### 3,单季度.净利润同比增长率(%) S_QFA_YOYPROFIT;单季度.净利润环比增长率(%) S_QFA_CGRPROFIT
            if "S_QFA_YOYPROFIT" in df_ashare_ana.columns :
                df_ashare_ana.loc[temp_i, "S_QFA_YOYPROFIT" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_QFA_YOYPROFIT" ] - df_ashare_ana.loc[temp_i, "S_QFA_YOYPROFIT" +"_q_pre" ] 
                df_ashare_ana.loc[temp_i, "S_QFA_CGRPROFIT" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_QFA_CGRPROFIT" ] - df_ashare_ana.loc[temp_i, "S_QFA_CGRPROFIT" +"_q_pre" ] 

            ### 4，季度盈利持续性:"S_FA_ROIC",ROIC环比增长率,总资产净利率,S_FA_ROA，需要用para_list前2项计算
            if "S_FA_ROIC" in df_ashare_ana.columns :
                df_ashare_ana.loc[temp_i, "S_FA_ROIC" +"_q_ave" ] = df_ashare_ana.loc[temp_i, "S_FA_ROIC" ]/para_date[0]
                df_ashare_ana.loc[temp_i, "S_FA_ROIC" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_FA_ROIC" ]/para_date[0] - df_ashare_ana.loc[temp_i, "S_FA_ROIC" +"_q_pre" ]/para_date[1]
                df_ashare_ana.loc[temp_i, "S_FA_ROA" +"_q_ave" ] = df_ashare_ana.loc[temp_i, "S_FA_ROA" ]/para_date[0]
                df_ashare_ana.loc[temp_i, "S_FA_ROA" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_FA_ROA" ]/para_date[0] - df_ashare_ana.loc[temp_i, "S_FA_ROA" +"_q_pre" ]/para_date[1]

            ### 5，季度资产质量：近2季度经营性现金流/总资产 or 经营性现金流/总负债，
            # 经营活动产生的现金流量净额/营业收入,S_FA_OCFTOOR;经营活动产生的现金流量净额/经营活动净收益,S_FA_OCFTOOPERATEINCOME
            # 单季度.经营活动产生的现金流量净额／营业收入,S_QFA_OCFTOSALES;单季度.经营活动产生的现金流量净额／经营活动净收益,S_QFA_OCFTOOR
            if  "S_FA_OCFTOOR" in df_ashare_ana.columns :
                df_ashare_ana.loc[temp_i, "S_FA_OCFTOOR" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_FA_OCFTOOR" ] - df_ashare_ana.loc[temp_i, "S_FA_OCFTOOR" +"_q_pre" ] 
                df_ashare_ana.loc[temp_i, "S_FA_OCFTOOPERATEINCOME" +"_diff" ] = df_ashare_ana.loc[temp_i, "S_FA_OCFTOOPERATEINCOME" ] - df_ashare_ana.loc[temp_i, "S_FA_OCFTOOPERATEINCOME" +"_q_pre" ] 


        ### save to obj
        obj_data["dict"]["list_para_date"]  =  obj_date["list_para_date"] 
        obj_data["dict"]["date_q"]  = date_q
        obj_data["dict"]["date_q_pre"]  =  date_q_pre 
        obj_data["df_ashare_ana"]  =  df_ashare_ana
        
        return obj_data

    def export_data_financial_ana(self,obj_data,obj_financial):
        ### 输出财务分析和财务模型相关指标,obj_data是单期数据，obj_financial是跨期日期数据和pms数据存放处
        ''' ### 注意：在调仓日T有3套时间：
        1，T和之前1次披露时间；[date_ann_pre, date_ann ]
        2，T之前的2个季末财务日期；[date_q_pre,date_q]
        3，T日至下一个财务披露日期:[date_ann, date_ann_next ]。
        注意：在调仓日T有3套时间：1，T之前的2个披露时间；2，T之前的2个季末财务日期；3，T日至下一个财务披露日期。
        obj_data["dict"]["date_adj_port"] 
        obj_data["dict"]["date_adj_port_next"] 
        obj_data["dict"]["date_ann"] 
        obj_data["dict"]["date_ann_next"] 
        obj_data["dict"]["date_ann_pre"]
        '''
        ### 组合调整日期：
        
        date_q = obj_data["dict"]["date_q"] 
        date_q_pre = obj_data["dict"]["date_q_pre"] 
        ### 
        data_io_1 = data_io() 
        
        ### 设置文件命名和导出路径等
        path_financial_ana = self.obj_config["dict"]["path_ciss_db"] +"financial_ana\\" 
        path_export = path_financial_ana + obj_data["dict"]["id_output"]+ "\\"
        if not os.path.exists( path_export  ) :
            os.mkdir( path_export  )
        print("path_export: ",path_export  )
        obj_data["dict"]["path_export"] =path_export
        
        #############################################################################
        ### 行业分布：df_ashare_ind
        file_name = "df_ashare_ind_" + date_q + "_" + date_q_pre +".csv" 

        obj_data["df_ashare_ind"].to_csv( path_export+file_name, index=False )

        ### 个股权重和分析指标信息 df_ashare_portfolio ；df_ashare_portfolio包括了 df_ashare_ana
        # 上一次
        file_name = "df_ashare_portfolio_" + obj_data["dict"]["date_ann"] +".csv"
        # obj_data["df_ashare_portfolio"] = data_io_1.del_columns( obj_data["df_ashare_portfolio"],col_str)
        obj_data["df_ashare_portfolio"].to_csv( path_export+file_name, index=False )

        ### 分组区间收益 df_ret_all
        
        file_name = "df_ret_all_" + obj_data["dict"]["date_ann_pre"] + "_" + obj_data["dict"]["date_ann"] +".csv"
        obj_data["df_ret_all"].to_csv( path_export+file_name, index=False )

        ### PMS 文件
        ### 保存至 PMS权重文件
        # obj_financial["df_pms"] = pd.DataFrame( columns=["证券代码","持仓权重","成本价格","调整日期","证券类型"] )
        temp_df = obj_data["df_ashare_portfolio"][ obj_data["df_ashare_portfolio"]["weight_raw"]>0.003 ] 
        temp_df = temp_df.loc[:,["S_INFO_WINDCODE","weight_raw"] ]
        temp_df["weight_raw"] = temp_df["weight_raw"] 
        temp_df.columns=[ "证券代码","持仓权重" ]
        ### 下一个交易日调仓
        temp_df["成本价格" ] = ""
        temp_df["调整日期" ] = obj_data["dict"]["date_ann"] 
        temp_df["证券类型" ] = "股票"

        if obj_financial["count_pms"]  == 0 :
            obj_financial["df_pms"] =temp_df 
            obj_financial["count_pms"]  = 1 
        else :
            obj_financial["df_pms"] = obj_financial["df_pms"].append(temp_df,ignore_index=True)

        ### save df_pms to file 
        file_pms="pms_"+ obj_data["dict"]["date_ann"] +".csv"
        temp_df.to_csv( obj_data["dict"]["path_export"] +file_pms, index=False, encoding="gbk" )
        file_pms="pms.csv"
        obj_financial["df_pms"].to_csv( obj_data["dict"]["path_export"] +file_pms, index=False, encoding="gbk" )

        ### save return for next period 
        file_ret_next = "df_ret_next_"+obj_data["dict"]["date_ann"] +"_" + obj_data["dict"]["date_adj_port_next"]  +".csv"
        obj_financial["df_ret_next"].to_csv( obj_data["dict"]["path_export"] +file_ret_next, index=False  )

        return obj_data,obj_financial
