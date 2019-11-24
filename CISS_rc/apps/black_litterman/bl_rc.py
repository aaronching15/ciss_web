# -*- encoding:"utf-8"-*-
__author__=" ruoyu.Cheng"

'''
实现BL模型的主要功能

MENU :
### BL三层次 :1个观点计算BL
    todo：BL1：行业内个股
    1，prior：要计算行业内股票的月收益率相关性cov，需要历史月收益率，选2年
        3，obj func：sum[ret,x]
        4, s.t. sum[weight] <= 1;  波动率预测<基准波动率
        5，output：w_mkt,ret_mkt,
        6, Find delta/δ
        7, sigma是资产历史收益率的协方差 
        8, PI,pie,∏ : vector of equilibrium asset returns 
    
    2，post：build Omega：??
        1, P:1*n,就1个观点，或者按股票数量n形成n个观点 
        2，Q
        3，Omega：diag( P*sigma*P ) 
        4, tau
        5,?C is proportional to sigma 

    3, 计算w_bl, ret_bl, cov_bl,

### BL模型类型：canonical还是alternative

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
class bl_rc():
    ### 类的初始化操作 
    def __init__(self, Sys_name ):
        self.Sys_name =Sys_name

    def print_info(self) :
        ### Print infomation for all modules  
        
        print() 
        ###  

        return 1 

    def optimize_bl_stock(self,temp_i,code_ind,temp_date,code_index ) :
        ### 计算BL模型第一层的股票
        '''
        todo：BL1：行业内个股
        1，prior：要计算行业内股票的月收益率相关性cov，需要历史月收益率，选2年
            3，obj func：sum[ret,x]
            4, s.t. sum[weight] <= 1;  波动率预测<基准波动率
            5，output：w_mkt,ret_mkt,
            6, Find delta/δ
            7, sigma是资产历史收益率的协方差 
            8, PI,pie,∏ : vector of equilibrium asset returns 
        
        2，post：build Omega：??
            1, P:1*n,就1个观点，或者按股票数量n形成n个观点 
            2，Q
            3，Omega：diag( P*sigma*P ) 
            4, tau
            5,?C is proportional to sigma 

        3, 计算w_bl, ret_bl, cov_bl,

        '''
        import json
        import pandas as pd 
        import numpy as np 
        import math
        ###################################################
        ### 载入市场指数， col_name from symbol to name_CN

        file_name = "in_stockbm_ind_ret_m_"+code_index+"_"+temp_date+".csv"
        file_path = "D:\\CISS_db\\db_bl\\data\\"
        file_path_input = "D:\\CISS_db\\db_bl\\data\\input\\"
        temp_df = pd.read_csv(file_path+file_name,encoding = "gbk")
        ret_benchmark_df = temp_df[code_ind]
        ###
        mu_bench = np.mean( ret_benchmark_df )
        var_bench = np.var( ret_benchmark_df )
        print("平均值，协方差  ",mu_bench ,var_bench  )

        len_date = len( ret_benchmark_df.index )
        print("length of date for benchmark is ", len_date )

        ###################################################
        ### 载入csi800成分股的月收益率指数
        file_name = "in_stock_ret_m_"+code_index+"_"+temp_date+".csv"
        # 000906.SH_20140301.csv"
        file_path = "D:\\CISS_db\\db_bl\\data\\"

        temp_df = pd.read_csv(file_path+file_name,encoding = "gbk")
        ### ret_asset_df 资产过去2年月收益率
        # print("temp_df ",temp_df.head()  )
        ret_asset_df = temp_df.drop("Date",axis=1)
        # No use ||  ret_asset_df = temp_df.dropna(axis=0)
        # 有的股票当时还未上市，需要把na值替代为0
        ret_asset_df =ret_asset_df.fillna(value=0.0)
        # print("len ret_asset_df ", len( ret_asset_df.index  ))
        # print( ret_asset_df.head() )

        ### 选取属于行业 code_ind = "化工"的股票
        ### notes:这里会出错是因为 basic_info.csv 里需要把所有回测周期内的股票都填进去
        ### 注意，"basic_info.csv" 里边数据不对，没用
        file_path_consti = "D:\\CISS_db\\db_bl\\" +"index_consti\\"
        file_name_consti = code_index+"_"+temp_date +".csv" # "000906.SH_20150301.csv"
        temp_df_basic = pd.read_csv(file_path_consti+file_name_consti,encoding = "gbk")
        code_list_ind1 = temp_df_basic[ temp_df_basic["ind"]== code_ind  ]["code"].values
        ret_asset_df = ret_asset_df.loc[:, code_list_ind1]

        ### code_list_ind1  是按代码的升序排列
        # print("code_list_ind1 \n",code_list_ind1   )

        ### 平均值，协方差、相关系数
        mu_asset_df = ret_asset_df.mean() 
        cov_asset_df = ret_asset_df.cov() 

        corr_asset_df = ret_asset_df.corr() 
        # notes: scipy.linalg 中需要np.array 格式，不能直接用dataframe
        # datafrmae to np.arrays for Optimization calculation
        ret_asset_np = ret_asset_df.values
        cov_asset_np = cov_asset_df.values
        # print("平均值，协方差、相关系数 ",mu_asset_df.head() ,cov_asset_df.head() ,corr_asset_df.head() )

        ###################################################
        ### method 3 optimize.minimize_scalar, minimize with "SLSQP" method
        from scipy import optimize

        ### 设置最优化参数
        ### notes:有时候index对应代码，有时候columns对应代码
        len_assets = len( ret_asset_df.columns )
        w_max = 1/len_assets  #  0.6
        # example bnds: ((0,1),(0,1),(0,1),(0,1),(0,1))
        bnds = [(0, w_max )] * len_assets
        # w_init = [1, 0,0,0,0]
        w_init = [0] * len_assets
        w_init[0] = 0.95

        ###debug
        # print("Debug========================")
        # x= w_init
        # OK | print("len x",len(x)  )
        # OK | print( np.cumprod( np.matmul( ret_asset_np,x).sum()+1 )[-1] ) 
        # print( type(cov_asset_np) )
        # print( np.matmul(cov_asset_np,x) )
        # print( np.matmul( x,np.matmul(cov_asset_np,x) )  ) 
        # print("Debug========================")

        ##############################################################
        ### 定义最优化方程
        obj_fun = lambda x: -1* (np.cumprod( np.matmul( ret_asset_np,x).sum()+1 )[-1]-1)    
        cons = ({'type': 'ineq', 'fun': lambda x: -1*np.sum(x) +0.95 },
                {'type': 'ineq', 'fun': lambda x:  -1*np.matmul( x,np.matmul(cov_asset_np,x) )+ var_bench  } ) 
        # cons = ({'type': 'ineq', 'fun': lambda x: -1*x[0] -1*x[1] -1*x[2] -1*x[3] -1*x[4]+1 },
        #         {'type': 'ineq', 'fun': lambda x:  -1*np.matmul( x,np.matmul(cov_asset_df,x) )+ var_bench  } ) 

        res = optimize.minimize( obj_fun , w_init, method='SLSQP', bounds=bnds,constraints=cons)

        print("result")
        print(res.success,res.message)
        ### w_mkt:方案一
        w_mkt = res.x
        print("weights of market")
        print( np.round(w_mkt,4 ) )
        print("portfolio return ", np.round( np.matmul( ret_asset_np,w_mkt).mean() ,4 ))

        print("portfolio variance ", np.round(np.matmul( w_mkt,np.matmul(cov_asset_np,w_mkt) ),8 ),  np.round(var_bench ,8 ) )

        print("Benchmark return ", np.round(  np.array(ret_benchmark_df).mean() ,4 ))
        j=0
        for i in w_mkt :
            if i> 0.001 :
                print(  ret_asset_df.columns[j])
            j=j+1
        
        #############################################################
        ### VIP 由于先验隐含组合业绩太差，w_mkt改为对应指数成分


        '''
        如果用对于行业内持仓寻找历史最忧配置比例，则对于 材料，201202~201402，会选到
        000688.SZ  201304长期停牌后复牌暴涨，之后下跌，雷
        000831.SZ  201302复牌，同上，
        000975.SZ  无超额收益
        002203.SZ  上涨较多，后市走势良好
        002440.SZ  上涨较多，后市走势良好
        002450.SZ  上涨较多，后市走势良好
        600392.SH  涨幅一般，后市走势良好
        601216.SH  涨幅一般，后市走势良好
        总结，也可以，但是缺乏经济学逻辑，对停牌等经济风险抵抗地较小。
        建议市场先验组合要么选 行业最忧配置，要么选指数成分
        '''
        ##############################################################
        # todo：BL1：行业内个股
        # 1，prior：要计算行业内股票的月收益率相关性cov，需要历史月收益率，选2年
        ##############################################################
        ### 3，obj func：sum[ret,x]
        #     4, s.t. sum[weight] <= 1;  波动率预测<基准波动率
        #     5，output：w_mkt,ret_mkt,
        #     6, Find delta/δ
        #     7, sigma是资产历史收益率的协方差 
        #     8, PI,pie,∏ : vector of equilibrium asset returns 
        # 12 months per year
        date_freq = 12
        miu_mkt = np.mean( np.matmul( ret_asset_np,w_mkt)  ) * date_freq 
        std_mkt = np.std(  np.matmul( ret_asset_np,w_mkt) ) *math.sqrt(date_freq )
        sharp_ratio = miu_mkt/std_mkt  
        ### notice：delta/δ的计算
        ###例： sharp= 0.2 , std=0.013, 如果std日波动率不乘100转化成百分比的单位，delta算出来的值15.35 
        # sharp_ratio  0.2005808082542644
        # value of delta is  0.1535597734348625
        delta = sharp_ratio/ (std_mkt )
        print("miu_mkt ", miu_mkt )
        print("std_mkt ", std_mkt )
        print("sharp_ratio ", sharp_ratio )
        print("value of delta is ", delta )

        ###################################################
        ### sigma是资产历史收益率的协方差 
        ### 1.4,pie = delta*Sigma*w_mkt ;
        # sigma/Σ is the covariance of the historical asset returns.
            # sigma = cov( historical return matrix )
        sigma = cov_asset_np

        ###################################################
        ### PI,pie,∏ : vector of equilibrium asset returns 
        # value of PI is  [0.13085351 0.1314748  0.12337079 0.11374284 0.03569067]
        # pie is in annual form, we want it to be daily baisi

        pie = delta * np.matmul(sigma , w_mkt)
        ### pie 值对于第三个和第五个asset收益较小的原因是市场组合的均衡配置比例都是0.0
        # value of PI is  [3.49090662e-03 4.62168365e-03 5.58691073e-03 4.09842457e-03 3.49421596e-08]
        print("value of PI is \n", pie )
        print("value of annual PI is \n", np.round(pie*date_freq ,4) )


        ##############################################################
        # 2，post：build Omega：

        ##############################################################
        ### P:1*n,就1个观点，或者按股票数量n形成n个观点 
        ### 本行业对应的观点配置 
        '''
        in_stock_all_views_000906.SH_20140901
        D:\CISS_db\db_bl\data\input
        文件 "in_stock_all_views.csv"里包括了P,Q,tau
        code w_mkt ind ret	sigma
        code_ind,temp_date
        '''
        # in_stock_all_views_000906.SH_20140901
        file_name = "in_stock_all_views_"+code_index +"_"+temp_date  +".csv"
        temp_df = pd.read_csv(file_path_input +file_name,encoding = "gbk")
        # code	ind	w_mkt	ret	w_view	sigma
        print("file_name views", file_name  ) 

        temp_view = temp_df[ temp_df["code"].isin( code_list_ind1) ]
        temp_view = temp_view.sort_values(by="code" )
        print("Debug=====",temp_date  ,temp_view.head() )
        w_view = temp_view["w_view"].values
        print("temp_view ", temp_view  )

        ### w_mkt:方案二
        w_view = temp_view["w_mkt"].values

        p_views = [w_view]

        ### tau ：信心程度比较高，因为我们认为观点相对于纯粹基于价格成交量的市场先验组合有效性
        tau = 0.5 # 1/len_date #  0.02
        # tau_str = input("Type in value for tau...")
        # tau = float( tau_str )

        ### Q 指的是对未来1个季度的预测收益率
        ret_q  = np.matmul( w_view, temp_view["ret"].values)
        ### Omega：diag( P*sigma*P ) 
        print("Debug========")
        ### notes:这里会报错是因为 basic_info.csv 里需要把所有回测周期内的股票都填进去
        print( code_ind,temp_date,code_index  )
        print( type(p_views),len(p_views) ,p_views  )
        print( type(sigma), len(sigma) ,sigma[0]  )
        ### Omega计算：方案一 ：
        omega_pre =  np.matmul( np.matmul(p_views,sigma), np.array(p_views ).T) 
        (P_nouse, omega, Q_nouse) = np.linalg.svd(omega_pre, full_matrices=False)
        ### Omega计算：方案二 todo
        
        omega = tau*np.diag(omega)

        ### Define C to be proportional to sigma
        C = tau*sigma 


        ##############################################################
        # 3, 计算w_bl, ret_bl, cov_bl,

        ###################################################
        ### Method 无限制条件下计算BL
        ### step 1: tau*sigma*P
        temp_p1 = tau* np.matmul( sigma, np.transpose(p_views) )
        ### notice that np.array() 矩阵可以直接乘常数，但是python-list格式变量乘常数会变成复制list
        # print( "temp_part1 \n", temp_p1 )

        ### step 2: tau*P*sigma*P +omega
        temp_p2 = np.matmul( p_views, temp_p1 ) + omega
        # print( "temp_part2 \n", temp_p2 )

        ### step 3: [tau*P*sigma*P +omega]^(-1)
        temp_p2 = np.linalg.inv( temp_p2 )
        # print( "temp_part2 \n", temp_p2 )

        ### df to np.array ,mu_asset_df column for assets 
        # mu_asset_np = np.array(mu_asset_df.T )

        ### step 4: (tau*sigma*P)*[tau*P*sigma*P +omega]^(-1)*[Q-P*PI]
        temp_p3 = ret_q - np.matmul( p_views ,  pie )
        # print( "temp_part3 \n", temp_p3 )

        mu_bl_2 =  np.matmul( temp_p1,temp_p2)
        ### posterior estimate returns 
        mu_bl_2 =  np.matmul( mu_bl_2,temp_p3)

        ### step 5: PI+ (tau*sigma*P)*[tau*P*sigma*P +omega]^(-1)*[Q-P*PI]
        mu_bl = pie +mu_bl_2 

        print( "return in daily basis " )
        print( "pie vs mu_bl_2 \n", pd.DataFrame([pie, mu_bl_2 ],index=["pie","mu_bl_2"]   ) )  
        print( "mu_bl vs miu return of_asset  \n", pd.DataFrame([mu_bl, mu_asset_df],index=["mu_bl","mu_asset_df"]   ) )
        # 确实反映了观点！
        ### Calculate covariance， method 2:reference:eq.(30)
        temp_c1 = tau*sigma
        temp_c2 = np.matmul( tau*sigma , np.transpose(p_views) ) 

        temp_c3 = tau* np.matmul(  np.matmul( p_views,sigma ) ,  np.transpose(p_views)  ) + omega
        temp_c3 = np.linalg.inv( temp_c3 )
        # source: Bl in detail, eq.(26)
        temp_c4 = tau* np.matmul( p_views,sigma )
        M_post_m2 = temp_c1 - np.matmul( np.matmul( temp_c2,temp_c3), temp_c4 )
        M_post = M_post_m2
        cov_bl2 = sigma + M_post

        print("M_post \n", M_post )
        print( "cov_bl2 \n" , cov_bl2 )
        print("666 ", delta )

        print("rank  ",  np.linalg.matrix_rank( cov_bl2 )   )
        print("Method: Optimization")

        if np.linalg.matrix_rank( cov_bl2 ) ==len(mu_asset_df) :
            ### method 1  ， unconstraint conditions
            w_bl = np.matmul(mu_bl , np.linalg.inv( cov_bl2 ) ) /delta
            # 无限制条件下算出来的数值非常不合理。 
            print( "Optimal portfolio weight on unconstraint efficient frontier by BL \n"  )
            print("weights of optimal portfolio by BL matrix calculation")
            print( np.round(w_bl,4 ) )  
            # 50 weeks per year with trading 
            print("portfolio return ,weekly :ALL hist.  vs rolling_window\n") 
            print(np.round(  np.matmul( ret_asset_df.mean() ,w_bl )*12  ,4 ), np.round(  np.matmul( mu_asset_df ,w_bl )*12  ,4 ) )

        ###########################################################################
        ### method 2  限制条件下计算BL

        ### notice 这里的预期收益率和协方差都要用新的！！ PI_hat and sigma_bl
        # obj_fun2 = lambda x: x[0] +x[1] +x[2] +x[3] +x[4] 
        # 求组合的每日收益率，加1后连乘，取最后一个值即区间净值，减去1后是组合的区间收益率，求最小值
        # max  ret_port/std_port  equals to min -1*ret_port/std_port
        
        # obj_fun2 = lambda x: -1* ( math.pow( (np.matmul( mu_bl,x)+1),len_date) -1) 
        import math
        obj_fun2 = lambda x: -1* ( math.pow( (np.matmul( mu_bl,x)+1),len_date) -1) + 0.3*math.sqrt( np.matmul( x,np.matmul(cov_bl2 ,x) ) ) 
        #### 20191115 因为结果太差，试一下 最大收益最小波动的情况
        cons2 = ({'type': 'eq', 'fun': lambda x: -1*np.sum(x) +1 },
                {'type': 'eq', 'fun': lambda x:  -1*np.matmul( x,np.matmul(cov_bl2 ,x) )+ var_bench  } ) 

        len_assets = len( ret_asset_df.columns )

        # example bnds: ((0,1),(0,1),(0,1),(0,1),(0,1))
        bnds2  = [(0, w_max)] * len_assets
        # w_init = [1, 0,0,0,0]
        w_init2 = [0] * len_assets
        w_init2[0] = 1

        res2 = optimize.minimize( obj_fun2 , w_init2, method='SLSQP', bounds=bnds2,constraints=cons2)

        print("result2")
        print(res.success,res.message)

        w_bl = res2.x
        print("Method: Optimization")
        print("weights of optimal portfolio by BL")
        print( np.round(w_bl,4 ) )

        ### 这里是计算得到的对下一期的资产配置，如果20140228，则需要20140301-20140601的配置
        ### 把权重和股票代码保存到csv文件，用于下一个周期的计算

        ### 计算后验配置比例和先验的区别。 
        ### Calculate squared Mahalanobis distance
        miu_diff = w_bl - w_mkt
        print("Debug================")
        print("miu_diff", len(miu_diff) )
        print("sigma ",sigma)
        ### Notes:20181201 计算时出现以下的报错，是不是行业内股票数量太多了，M_q是某种距离的计算，不要也罢
        # numpy.linalg.linalg.LinAlgError: Singular matrix
        # temp_m1 = np.matmul( miu_diff, np.linalg.inv(tau*sigma)  )
        # M_q = np.matmul( temp_m1 , miu_diff) 
        # print("M_q \n ")
        # print( M_q )


        ### generate a df with ["code","date","ind","w_bl"]
        # temp_view.columnss=temp_view   code  w_mkt ind   ret   sigma
        temp_view["w_bl"] = w_bl
        output= temp_view  

        dir_output ="191115update"
        file_path2 = "D:\\CISS_db\\db_bl\\data\\output\\"+dir_output + "\\"
        output.to_csv(file_path2+"w_"+temp_date+"_"+str(temp_i)+".csv",encoding="gbk")
        
        return output

    def optimize_bl_industry(self,input  ) :
        ### 计算BL模型第一层的股票






        output = 1 
        return output

    def optimize_bl_market(self,input  ) :
        ### 计算BL模型第一层的股票



        output = 1 
        return output