# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
需求：
实现black litterman模型

last 190529 || since 190529

Function:
功能：

todo:

Notes:
##############################################
变量：
Blended ecpected return miu
    miu = [P'Omega^(-1)P+C^(-1) ]^(-1)[P'Omega^(-1)q+C^(-1)pie]

Uncertainty of estimation cov(miu)
    cov(miu) = [P'Omega^(-1)P+C^(-1) ]

w_market:
    市场组合下的各个资产权重

delta/δ
    delta = sharp_ratio /( w_mkt*sigma*w_mkt )

PI,pie,∏ : vector of equilibrium asset returns
    根据市场组合下的各个资产权重计算的市场组合收益率、单资产收益率

C uncertainty in prior brief 

P views on asset {注意，views不应该是狭隘的不变的对资产的配置，而是可以代表任何策略、短期观点等}
    q =P∗μ + ε,   ε~N(0, Ω), Ω=diag(ω1,ω2,...ωv),v is total number of views
    v views and k assets, P is a v-by-k matrix, q is a v-by-1 vector, and 

omega/Ω is a v-by-v diagonal matrix (representing the independent uncertainty in the views).

Q,q :观点的(长期)收益率 || 文献中时间频率是年化的，要除以252 变成天的。
    例子：P =[[CSI300,601398,600030,000002,cash] [0.95,0,0,0.0.05],[0, 0.35,0.35,0.3,0],[0,0, 0.45,0.45,0.1] ]
        对5种资产有3个views，每个views对应了所有的资产的配置比例，默认是0
    Omega = [0, 0.001, 0.002] v个views，每个对应一个uncertainty level 
    q =【0.04，0.053,0.068  】

sigma/Σ is the covariance of the historical asset returns.
    sigma = cov( historical return matrix )

C ：makes the assumption that the structure of C is proportional to the covariance Σ. 
    Therefore, C=τΣ, where τ is a small constant. A smaller 
tau/τ indicates a higher confidence in the prior belief of μ. 
    The work of He and Litterman uses a value of 0.025. Other authors suggest using 1/n where n is the number of data points
    例子：tau = 0.025 or 1/n n是资产数量乘时间数量
    C = tau*sigma
##############################################
Steps：
1，导入证券/指数/资产的股价数据（日频率），定义benchmark，risk-free rate=0或逆回购/货币基金收益，
    1.1,价格转为收益率百分比，计算协方差 sigma = cov( return )
    1.3，Find delta/δ
        Multiply both sides of π=δΣω_mtk with ω_mtk,to output δ=SharpeRatio/sigma_mkt
        sharp_ratio = miu(r_benchmark)/std(r_benchmark)
        delta = sharp_ratio /( w_mkt*sigma*w_mkt )
    1.4,pie = delta*Sigma*w_mkt ;
    
2, 定义观点P，omega,q,sigma,tau,C：Views of market、 inhouse opinions、 portfolio related strategy
    show table 【view matrix， view_return， view_uncertainty】

3, 计计算市场组合权重W/weight_ij，要么选给定权重，要么用线性求解方程求解权重 Aeq*W=Beq， 
    Aeq: n-by-m,指数成分股或可投证券空间的资产权重矩阵，值都为1
    Beq: n-by-1,线性方程组右侧均为1，带表组合权重之和
    LB/lower bound: 变量的取值下沿

4，求解BL模型的最优组合预期收益和方差
    Use the P, q , Ω , π , and C inputs to compute the blended asset return and variance using the Black-Litterman model.
    mu_bl = (P'*(Omega/P) + inv(C)) / ( C/pie + P'*(Omega/q));
    cov_mu = inv(P'*(Omega/P) + inv(C));
    例子： assetNames', pie*252, mu_bl*252, 
    Asset_Name    Prior_Belief_of_Expected_Return    Black_Litterman_Blended_Expected_Return 
      "AA"        0.19143                            0.19012    
      "AIG"       0.14432                            0.13303   

5,Optimization, 以风险水平或收益率目标、sharp ratio之一为最大化目标方程
    标准的优化求解设置：Least Squares with Linear Constraints and Bounds 
    find the efficient portfolio for a given risk or return level, and you can also maximize the Sharpe ratio.
    5.1，计算传统mean-variance 的配置结果
    5.2，计算 mean-var-BL 的配置结果
    5.3，constraints， all w_ij >=0 and sum of w_ij =1 | 没有空头头寸的情况


source Appendix F{steps to implement the BL model} , The Black-Litterman Model in detail.pdf
source https://ww2.mathworks.cn/help/finance/examples/black-litterman-portfolio-optimization.html

todo，用 scipy.optimize 进行优化求解


===============================================
'''
import json
import pandas as pd 
import numpy as np 
import math
# import sys
# sys.path.append("..") 

###################################################
### step 1 导入证券/指数/资产的股价数据（日频率），定义benchmark，risk-free rate=0或逆回购/货币基金收益，
# 1.1,价格转为收益率百分比，计算协方差 sigma = cov( return )

###################################################
### Import Price data 

###################################################
### Get Return data 
name_benchmark = "equity_CN"
ret_benchmark = [ 0.001, -0.003, 0.014, 0.019, -0.0179 ,-0.0032  ,0.001, -0.003, 0.014, 0.019, -0.0179 ,-0.0032  ]
name_asset1 = "A50"
ret_asset1  = [ 0.0011, -0.0031, 0.0142, 0.0192, -0.01792 ,-0.0022 ,0.0011, -0.0031, 0.0142, 0.0192, -0.01792 ,-0.0022 ]
name_asset2 = "csi300"
ret_asset2  = [ 0.0013, -0.0039, 0.024, 0.023, -0.0219,-0.0067 ,0.0013, -0.0039, 0.024, 0.023, -0.0219,-0.0067 ]
name_asset3 = "csi500"
ret_asset3  = [ -0.0013, -0.019, 0.034, 0.023, -0.0219 ,-0.0192 ,-0.0013, -0.019, 0.034, 0.023, -0.0219 ,-0.0192]
name_asset4 = "csi1000"
ret_asset4  = [ -0.0033, 0.019, 0.024, 0.021, -0.0193 ,-0.0212 ,-0.0033, 0.019, 0.024, 0.021, -0.0193  ,-0.0212]
name_asset5 = "cash_tool"
ret_asset5  = [ 0.00001,0.000011,0.00001,0.000011,0.00001,0.00001  ,0.00001,0.000011,0.00001,0.000011,0.00001,0.00001]

len_date = len( ret_benchmark )
print("length of date for benchmark is ", len_date )
ret_benchmark_df = pd.DataFrame([ret_benchmark] )
ret_benchmark_df =ret_benchmark_df.T
ret_benchmark_df.columns=[name_benchmark]
ret_asset_df = pd.DataFrame([ret_asset1 ,ret_asset2, ret_asset3, ret_asset4, ret_asset5] )
ret_asset_df =ret_asset_df.T
ret_asset_df.columns=[name_asset1,name_asset2,name_asset3,name_asset4,name_asset5] 

print("ret_benchmark_df")
print( ret_benchmark_df )
print("ret_asset_df")
print(ret_asset_df)

###################################################
### Get covariance matrix, 计算协方差 sigma = cov( return )
# Notice that all cov(cash, ~) is NaN
# 使用过去5个时间频率的数据
rolling_window = 8
# 5*1 array 
mu_asset_df = ret_asset_df.loc[-1*rolling_window :,:].mean() 

### cov_asset_df  算错了！！！ 要的是variance，不是correlation！！！！ 
### notice np.cov(x) 计算的是x方差的无偏估计，分母是n-1, np.var(x) 对应的分母是n
temp_df = ret_asset_df.loc[-1*rolling_window :,:]
cov_asset_df = ret_asset_df.loc[-1*rolling_window :,:].cov() 
cov_asset_df = cov_asset_df.fillna(0.0)

print("ret_asset_mean")
print( mu_asset_df )

print("ret_asset_ covariance")
print( cov_asset_df )
# var_asset  [1.77172000e-04 3.20958667e-04 5.78542667e-04 4.73122667e-04  2.66666667e-13]
var_asset = np.diag( cov_asset_df )
print("var_asset ", var_asset )

var_bench = np.var( ret_benchmark )
print("var_benchmark ", var_bench )

# print( np.var( temp_df["A50"] ) )
# print( np.mean( temp_df["A50"] ) )

###################################################
### 3 计算市场组合权重W/weight_ij，要么选给定权重，要么用线性求解方程求解权重 Aeq*W=Beq， 
#     Aeq: n-by-m,指数成分股或可投证券空间的资产权重矩阵，值都为1
#     Beq: n-by-1,线性方程组右侧均为1，带表组合权重之和
#     LB/lower bound: 变量的取值下沿
#     output： 

# notes: scipy.linalg 中需要np.array 格式，不能直接用dataframe
# datafrmae to np.arrays
ret_asset_np = ret_asset_df.values
print("ret_asset_np  \n")
print( ret_asset_np )

### no use 
# numpy求解矩阵的逆：矩阵A为原始矩阵，矩阵Ainv为矩阵A的逆
# ret_inv = np.linalg.inv(ret_asset_np)
# 矩阵A乘矩阵A的逆
# result = np.matmul(ret_asset_np, ret_inv  )
# print("Inverse of matrix")
# print( result )

###################################################
### method 1 矩阵求解Ax=b，会出现负权重，应该放松为最优化求解，如 Ax>=b
# from scipy import linalg
# x = linalg.solve( ret_asset_np, ret_benchmark  )
# # print( x )
# w_mkt = x 


###################################################
### method 2 use root() 
# from scipy.optimize import root,fsolve

# def f1(x):
#     # Ax-b
#     return np.array(
#         [ret_asset_np[0][0]*x[0]+ret_asset_np[0][1]*x[1]+ret_asset_np[0][2]*x[2]-ret_benchmark[0],
#         ret_asset_np[1][0]*x[0]+ret_asset_np[1][1]*x[1]+ret_asset_np[1][2]*x[2]-ret_benchmark[1],
#         ret_asset_np[2][0]*x[0]+ret_asset_np[2][1]*x[1]+ret_asset_np[2][2]*x[2]-ret_benchmark[2], ])

# ### 求解Ax-b=0 的值， sol_root 包括了全部的结果， 
# ### Qs 只用前3 个资产时converged，但用5个资产时无法converge！ 
# ### Qs fsolve 好像无法设置上下界bounds
# # [1,0,0,0,0] 是初始猜测值
# sol_root = root( f1,[1,0,0] )
# print("sol_root" )
# print(sol_root.x ) # equals to sol_fsolve = fsolve(f1, [1,0,0 ] )
# print(sol_root.message ) # The solution converged.
# print(sol_root.success ) # True

###################################################
### method 3 optimize.minimize_scalar, minimize with "SLSQP" method
from scipy import optimize

# ## Qs：line 206， IndexError: invalid index to scalar variable.
# def f2(x):
#     # Ax-b
#     return np.array(
#         [ret_asset_np[0][0]*x[0]+ret_asset_np[0][1]*x[1]+ret_asset_np[0][2]*x[2]-ret_benchmark[0],
#         ret_asset_np[1][0]*x[0]+ret_asset_np[1][1]*x[1]+ret_asset_np[1][2]*x[2]-ret_benchmark[1],
#         ret_asset_np[2][0]*x[0]+ret_asset_np[2][1]*x[1]+ret_asset_np[2][2]*x[2]-ret_benchmark[2], 
#         ret_asset_np[3][0]*x[0]+ret_asset_np[3][1]*x[1]+ret_asset_np[3][2]*x[2]-ret_benchmark[3],
#         ret_asset_np[4][0]*x[0]+ret_asset_np[4][1]*x[1]+ret_asset_np[4][2]*x[2]-ret_benchmark[4] ])
# bnds = (0,1)
# a = optimize.minimize_scalar(f2, bounds= bnds , method='bounded')


### 不同objective function
### method 1 最大最新一期超额收益  obj_fun =
# ret_benchmark[4]-ret_asset_np[4][0]*x[0] -ret_asset_np[4][1]*x[1] -ret_asset_np[4][2]*x[2] -ret_asset_np[4][3]*x[3] -ret_asset_np[4][4]*x[4] 
# x: array([9.63894154e-01, 1.94452827e-18, 1.94452827e-18, 1.82678484e-02, 3.29603631e-05])
# weight1 = 96.4%, 0,0,1.83%,0

### method 2 最小权重使用 || obj_fun =  x[0] +x[1] +x[2] +x[3] +x[4] 
# x: array([2.27465479e-01, 6.17405969e-01, 8.54656116e-15, 1.60120573e-02,0.00000000e+00])
# weight2 =  22.7%,61.7%,0,1.6%,0
# obj_fun2 = lambda x: x[0] +x[1] +x[2] +x[3] +x[4] 

### method 3 最大化区间收益率/区间波动率
# obj_fun = lambda x: x[0] +x[1] +x[2] +x[3] +x[4] 
# 求组合的每日收益率，加1后连乘，取最后一个值即区间净值，减去1后是组合的区间收益率，求最小值
# max  ret_port/std_port  equals to min -1*ret_port/std_port
#  [0.1394 0.6885 0.     0.0177 1.    ]， sum=1.8xx  > 100% 
# 加入权重之和不超过1后： [0.215  0.6275 0.     0.0162 0.1413] ，sum = 1 

# obj_fun = lambda x: -1* (np.cumprod( np.matmul( ret_asset_np,x).sum()+1 )[-1]-1) /(np.sqrt(  np.matmul( x,np.matmul(cov_asset_df,x) ) ) )
# cons = ({'type': 'ineq', 'fun': lambda x: ret_asset_np[0][0]*x[0]+ret_asset_np[0][1]*x[1]+ret_asset_np[0][2]*x[2]+ret_asset_np[0][3]*x[3]+ret_asset_np[0][4]*x[4]-ret_benchmark[0] },
#     {'type': 'ineq', 'fun': lambda x: ret_asset_np[1][0]*x[0]+ret_asset_np[1][1]*x[1]+ret_asset_np[1][2]*x[2]+ret_asset_np[1][3]*x[3]+ret_asset_np[1][4]*x[4]-ret_benchmark[1] },
#     {'type': 'ineq', 'fun': lambda x: ret_asset_np[2][0]*x[0]+ret_asset_np[2][1]*x[1]+ret_asset_np[2][2]*x[2]+ret_asset_np[2][3]*x[3]+ret_asset_np[2][4]*x[4]-ret_benchmark[2] }, 
#     {'type': 'ineq', 'fun': lambda x: ret_asset_np[3][0]*x[0]+ret_asset_np[3][1]*x[1]+ret_asset_np[3][2]*x[2]+ret_asset_np[3][3]*x[3]+ret_asset_np[3][4]*x[4]-ret_benchmark[3] },
#     {'type': 'ineq', 'fun': lambda x: ret_asset_np[4][0]*x[0]+ret_asset_np[4][1]*x[1]+ret_asset_np[4][2]*x[2]+ret_asset_np[4][3]*x[3]+ret_asset_np[4][4]*x[4]-ret_benchmark[4] },
#     {'type': 'ineq', 'fun': lambda x: -1*x[0] -1*x[1] -1*x[2] -1*x[3] -1*x[4]+1 } ) 

obj_fun = lambda x: -1* (np.cumprod( np.matmul( ret_asset_np,x).sum()+1 )[-1]-1)    
cons = ({'type': 'ineq', 'fun': lambda x: -1*x[0] -1*x[1] -1*x[2] -1*x[3] -1*x[4]+1 },
        {'type': 'ineq', 'fun': lambda x:  -1*np.matmul( x,np.matmul(cov_asset_df,x) )+ var_bench  } ) 

#  'type':'ineq' # >= 
#  'type':'eq'   # == 

bnds = ((0,1),(0,1),(0,1),(0,1),(0,1))
w_init = [1, 0,0,0,0]
res = optimize.minimize( obj_fun , w_init, method='SLSQP', bounds=bnds,constraints=cons)

print("result")
print(res.success,res.message)

w_mkt = res.x
print("weights of market")
print( np.round(w_mkt,4 ) )

print("portfolio return ", np.round(np.cumprod( np.matmul( ret_asset_np,w_mkt).sum()+1 )[-1]-1 ,4 ))
print("portfolio variance ", np.round(np.matmul( w_mkt,np.matmul(cov_asset_df,w_mkt) ),8 ),  np.round(var_bench ,8 ) )

print("Benchmark return ", np.round(np.cumprod( np.array(ret_benchmark) +1 )[-1]-1 ,4 ))
w_temp = [1,0,0,0,0]
print("portfolio return a1 ", np.round(np.cumprod( np.matmul( ret_asset_np,w_temp ).sum()+1 )[-1]-1 ,4 ))
w_temp = [0,1,0,0,0]
print("portfolio return a2", np.round(np.cumprod( np.matmul( ret_asset_np,w_temp ).sum()+1 )[-1]-1 ,4 ))
w_temp = [0,0,1,0,0]
print("portfolio return a3", np.round(np.cumprod( np.matmul( ret_asset_np,w_temp ).sum()+1 )[-1]-1 ,4 ))
w_temp = [0,0,0,1,0]
print("portfolio return a4", np.round(np.cumprod( np.matmul( ret_asset_np,w_temp ).sum()+1 )[-1]-1 ,4 ))
w_temp = [0,0,0,0,1]
print("portfolio return a5", np.round(np.cumprod( np.matmul( ret_asset_np,w_temp ).sum()+1 )[-1]-1 ,4 ))
# weights of market [0.     0.     0.     0.5572 0.0042]
# portfolio return  0.0148
# portfolio variance  0.00014689 0.00014689
# Benchmark return  0.0095
# portfolio return a1  0.0113
# portfolio return a2 0.0158
# portfolio return a3 -0.0044
# portfolio return a4 0.0266
# portfolio return a5 0.0001


# cons=每一期收益都大于市场组合 
# weights of market [0.215  0.6275 0.     0.0162 0.1413]
# 去掉cons后，由于波动率的限制，会把全部资产配置在现金工具上，所以最大化 miu/sigma 不够。如果有了组合方差等于市场方差，那么最大化miu即可。

###################################################
### 1.3，Find delta/δ
#     Multiply both sides of π=δΣω_mtk with ω_mtk,to output δ=SharpeRatio/sigma_mkt
#     sharp_ratio = miu(r_benchmark)/std(r_benchmark)
#     delta = sharp_ratio /( w_mkt*sigma*w_mkt )


### 怎么感觉sharp和delta的计算不对呢，应该用求出来的最优组合计算吧
miu_mkt = np.mean( np.matmul( ret_asset_np,w_mkt)  ) * 252 
std_mkt = np.std(  np.matmul( ret_asset_np,w_mkt) ) *math.sqrt(252)
sharp_ratio = miu_mkt/std_mkt  
### notice：delta/δ的计算
###例： sharp= 0.2 , std=0.013, 如果std日波动率不乘100转化成百分比的单位，delta算出来的值15.35 
# sharp_ratio  0.2005808082542644
# value of delta is  0.1535597734348625
delta = sharp_ratio/ (std_mkt  )

print("miu_mkt ", miu_mkt )
print("std_mkt ", std_mkt )
print("sharp_ratio ", sharp_ratio )
print("value of delta is ", delta )


###################################################
### sigma是资产历史收益率的协方差 
### 1.4,pie = delta*Sigma*w_mkt ;
# sigma/Σ is the covariance of the historical asset returns.
    # sigma = cov( historical return matrix )
sigma = cov_asset_df 

###################################################
### PI,pie,∏ : vector of equilibrium asset returns 
# value of PI is  [0.13085351 0.1314748  0.12337079 0.11374284 0.03569067]
# pie is in annual form, we want it to be daily baisi

pie = delta * np.matmul(sigma , w_mkt)
### pie 值对于第三个和第五个asset收益较小的原因是市场组合的均衡配置比例都是0.0
# value of PI is  [3.49090662e-03 4.62168365e-03 5.58691073e-03 4.09842457e-03 3.49421596e-08]
print("value of PI is \n", pie )
print("value of annual PI is \n", np.round(pie*252,4) )


###################################################
### 2, 定义观点P，Q=q , Omega.|  tau,C：Views of market、 inhouse opinions、 portfolio related strategy
# show table 【view matrix， view_return， view_uncertainty】

### Q是主观观点的判断收益率，如果P是n*k matrix，则Q是n*1 vector。 即n个观点的每一个观点权重乘未知的收益率后得到该观点(策略)的预期收益率。
p_views = [[1,0,0,0,0],[0,1,0,0,0],[0,0, 0.3,0.5,0.2],[0,0,0,0,1],[1,0,-1,0,0] ]

###################################################
### q is k*1 vector，因为q的收益率对应的是资产的回测区间内总收益，因此不能只计算每日的观点收益率，还需要计算累计收益率。
### ret_q is return for given rolling windows

ret_asset_accu = []
### ret_asset_df: each column for one asset 
for temp_col in ret_asset_df.columns :
    temp_r = 1 
    for temp_i in ret_asset_df.index :     
        
        temp_r = temp_r *(1+ ret_asset_df.loc[temp_i,temp_col ]  )
    ret_asset_accu = ret_asset_accu +[ temp_r-1 ]

print("Accumulated return of 5 assets ")
print( np.round(ret_asset_accu,4) )
# [0.013115466179756119, 0.02194142883508099, 0.013639412773122528, 0.04790597879378922, 5.200108101144352e-05]

### we want all return variables based on daily 
ret_q = np.matmul( p_views,ret_asset_accu  ) /rolling_window 
print( "Q/ q as return of view " ) 
print( np.round(ret_q*252 ,4) ) 
# [1.31154662e-02 2.19414288e-02 2.80552134e-02 5.20010810e-05]


#     Omega = [0, 0.001, 0.002] v个views，每个对应一个uncertainty level 
# example, q =【0.04，0.053,0.068  】
# first transpose list to np.array(), then use np.diag(  )
tau = 0.5
# tau =0.5 | [0.5        0.5        1.47615916 0.        ]
# tau =  1 | [0.5        0.5        1.47615916 0.        ]
# notes 两个矩阵不能直接相乘，#  result = np.matmul(A1,A2  )
# get diagonal metrix 
omega_pre =  np.matmul( np.matmul(p_views,sigma), np.array(p_views ).T) 
### 矩阵分解 np.linalg.svd | 注意1，numpy's SVD computes X = PDQ, so the Q is already transposed.
# # X = np.random.normal(size=[20,18])
# # P, D, Q = np.linalg.svd(X, full_matrices=False)
# # X_a = np.matmul(np.matmul(P, np.diag(D)), Q) 
# # D = np.linalg.diag(D) notice that D is 1*k array, need to transpose to diagonal matrix
# P, D, Q = np.linalg.svd(omega, full_matrices=False)
# print( np.linalg.svd(omega) )
# print( np.linalg.inv(omega) )
(P_nouse, omega, Q_nouse) = np.linalg.svd(omega_pre, full_matrices=False)

### notice np.diag()只是把array 变成对角线矩阵，而不是分解矩阵求eigin value
omega = tau*np.diag(omega)
print( "Omega " ) 
print( omega  ) 

# BEFORE
# omega = tau*  np.matmul( np.matmul(p_views,sigma), np.array(p_views ).T) 

### notes： 直接 np.cumprod(unit_q) 会吧 m*n矩阵变成1*(mn).

###################################################
### what is C ？ method 1：define C to be proportional to sigma
C = tau*sigma

print("C is ")
print( C )



###################################################
### 4，求解BL模型的最优组合预期收益和方差
# Use the P, q , Ω , π , and C inputs to compute the blended asset return and variance using the Black-Litterman model.
# mu_bl = (P'*(Omega/P) + inv(C)) / ( C/pie + P'*(Omega/q));
# cov_mu = inv(P'*(Omega/P) + inv(C));
# 例子： assetNames', pie*252, mu_bl*252, 
# Asset_Name    Prior_Belief_of_Expected_Return    Black_Litterman_Blended_Expected_Return 
#   "AA"        0.19143                            0.19012    
#   "AIG"       0.14432                            0.13303   

###################################################
### Method 1 mu_bl目前计算结果是 n*n，有问题。
# temp_part1 = np.matmul( np.matmul(np.transpose(p_views) ,np.linalg.inv(omega) ), p_views ) + np.linalg.inv(C) 
# temp_part1 = np.linalg.inv( temp_part1 )
# temp_part2 = np.matmul( np.matmul(np.transpose(p_views) ,np.linalg.inv(omega) ), ret_q  ) + np.matmul( np.linalg.inv(C) , pie )

# mu_bl =  np.matmul( temp_part1 ,temp_part2 )
# cov_mu = temp_part1

###################################################
### Method 2 mu_bl 参考BL in detail page19
### mu_bl = E(miu) + tau*sigma*P_trans*[ tau*P*sigma*P_trans + omega ]^(-1)*[Q-P*E(miu) ]
### cov_bl= [(tau*sigma)^-1 +P_trans*omega^-1 *P ]^-1 
# notice that Q = ret_q

temp_p1 = tau* np.matmul( sigma, np.transpose(p_views) )
### notice that np.array() 矩阵可以直接乘常数，但是python-list格式变量乘常数会变成复制list
# print( "temp_part1 \n", temp_p1 )

temp_p2 =  tau* np.matmul( p_views, temp_p1 ) + omega
# print( "temp_part2 \n", temp_p2 )

temp_p2 = np.linalg.inv( temp_p2 )
# print( "temp_part2 \n", temp_p2 )

### df to np.array ,mu_asset_df column for assets 
# mu_asset_np = np.array(mu_asset_df.T )

temp_p3 = ret_q - np.matmul( p_views ,  pie )
# print( "temp_part3 \n", temp_p3 )

mu_bl_2 =  np.matmul( temp_p1,temp_p2)


### posterior estimate returns 
mu_bl_2 =  np.matmul( mu_bl_2,temp_p3)
###Qs 预期收益率的数据比较不合理。
### notice: PI is not mean return of assets pie != mu_asset_df
### mu_asset_df is on daily base but ret_q is on rolling windows(5 days )
#                  0         1         2         3             4
# pie      0.002123  0.002888  0.003630  0.002895  2.681732e-08
# mu_bl_2 -0.000260 -0.002426 -0.009958  0.001233  3.044851e-07
mu_bl = pie +mu_bl_2 

print( "return in daily basis " )
print( "pie vs mu_bl_2 \n", pd.DataFrame([pie, mu_bl_2 ],index=["pie","mu_bl_2"]   ) )  
print( "mu_bl vs miu return of_asset  \n", pd.DataFrame([mu_bl, mu_asset_df],index=["mu_bl","mu_asset_df"]   ) )
# 确实反映了观点！
#                  0         1         2         3             4
# pie      0.001478  0.002075  0.002495  0.002646  3.915312e-08
# mu_bl_2  0.002770  0.000030 -0.012667  0.005801  5.820561e-07

### mu_asset_df is on daily base but ret_q is on rolling windows(5 days )
#                    A50   csi300    csi500   csi1000  cash_tool
# mu_bl        0.052661  0.05795  0.057309  0.053994   0.000094
# mu_asset_df  0.002696  0.00450  0.002960  0.009560   0.000010
###Qs 预期收益率的数据比较不合理。
# Ana: matlab官网上的算法，mu_bl是daily1的，需要乘252；但我们的算法来自pdf，
# Ana:把 ret_q 除以rolling_window，转成基于daily的日收益率
# Ans: 把 ret_q 改成daily basis后，
#                    A50    csi300    csi500   csi1000  cash_tool
# mu_bl        0.002551  0.004347  0.002805  0.009438    0.00001
# mu_asset_df  0.002696  0.004500  0.002960  0.009560    0.00001
# mu_bl的预期收益率基本都比资产历史先验prior收益率要低。

################################################################
### Calculate covariance， method 1 
### Qs: 计算出来的variance都是负值，有问题。
### cov_bl= [(tau*sigma)^-1 +P_trans*omega^-1 *P ]^-1 
temp_c1 = np.linalg.inv( tau*sigma )
temp_c2 = np.matmul( np.transpose(p_views), omega  )
temp_c2 =  np.matmul( temp_c2, p_views) 
cov_bl = np.linalg.inv( temp_c1 +temp_c2 )
print( "cov_bl \n" , cov_bl )

### todo 当前 cov_bl计算是否OK？
### sigma_bl = cov_asset_df + M ?
################################################################
### Calculate covariance， method 1 
temp_c1 = tau*sigma
temp_c2 = np.matmul( tau*sigma , np.transpose(p_views) ) 

temp_c3 = np.matmul(  np.matmul( p_views,sigma ) ,  np.transpose(p_views)  ) + omega
temp_c3 = np.linalg.inv( temp_c3 )

temp_c4 = tau* np.matmul( p_views,sigma )
M_post = temp_c1 - np.matmul( np.matmul( temp_c2,temp_c3), temp_c4 )
print("M_post \n", M_post )
cov_bl2 = sigma + M_post
print( "cov_bl2 \n" , cov_bl2 )
# Qs ：感觉covariance 差异很小
print( "cov_bl2 diff\n" , cov_bl2-cov_asset_df   )


################################################################
### Get portfolio weights
print("666 ", delta )

### method 1  ， unconstraint conditions
w_bl = np.matmul(mu_bl , np.linalg.inv( cov_bl2 ) ) /delta
# 无限制条件下算出来的数值非常不合理。
#  [ 5.47 -2.34 -1.856  1.43 -6401]
#  [ 5.47018260e+00 -2.34436397e+00 -1.85595481e+00  1.43266687e+00 -6.40167655e+03]
print( "Optimal portfolio weight on unconstraint efficient frontier by BL \n" , w_bl )

# tau=0.5 ||  [ 6.92739029  1.51746758 -6.05542994  1.17607531  1.83571727]

### method 2  ，constrainted conditions
### notice 这里的预期收益率和协方差都要用新的！！ PI_hat and sigma_bl
# obj_fun2 = lambda x: x[0] +x[1] +x[2] +x[3] +x[4] 
# 求组合的每日收益率，加1后连乘，取最后一个值即区间净值，减去1后是组合的区间收益率，求最小值
# max  ret_port/std_port  equals to min -1*ret_port/std_port

obj_fun2 = lambda x: -1* ( math.pow( (np.matmul( mu_bl,x)+1),len_date) -1) 
cons2 = ({'type': 'ineq', 'fun': lambda x: -1*x[0] -1*x[1] -1*x[2] -1*x[3] -1*x[4]+1 },
        {'type': 'ineq', 'fun': lambda x:  -1*np.matmul( x,np.matmul(cov_bl2 ,x) )+ var_bench  } ) 

# obj_fun2 = lambda x: -1* (np.cumprod( np.matmul( ret_asset_np,x).sum()+1 )[-1]-1) /(np.sqrt( np.matmul( x,np.matmul(cov_asset_df,x) ) ) ) 
# cons2 = ({'type': 'ineq', 'fun': lambda x: ret_asset_np[0][0]*x[0]+ret_asset_np[0][1]*x[1]+ret_asset_np[0][2]*x[2]+ret_asset_np[0][3]*x[3]+ret_asset_np[0][4]*x[4]-mu_bl[0] },
#     {'type': 'ineq', 'fun': lambda x: ret_asset_np[1][0]*x[0]+ret_asset_np[1][1]*x[1]+ret_asset_np[1][2]*x[2]+ret_asset_np[1][3]*x[3]+ret_asset_np[1][4]*x[4]-mu_bl[1] },
#     {'type': 'ineq', 'fun': lambda x: ret_asset_np[2][0]*x[0]+ret_asset_np[2][1]*x[1]+ret_asset_np[2][2]*x[2]+ret_asset_np[2][3]*x[3]+ret_asset_np[2][4]*x[4]-mu_bl[2] }, 
#     {'type': 'ineq', 'fun': lambda x: ret_asset_np[3][0]*x[0]+ret_asset_np[3][1]*x[1]+ret_asset_np[3][2]*x[2]+ret_asset_np[3][3]*x[3]+ret_asset_np[3][4]*x[4]-mu_bl[3] },
#     {'type': 'ineq', 'fun': lambda x: ret_asset_np[4][0]*x[0]+ret_asset_np[4][1]*x[1]+ret_asset_np[4][2]*x[2]+ret_asset_np[4][3]*x[3]+ret_asset_np[4][4]*x[4]-mu_bl[4] },
#     {'type': 'ineq', 'fun': lambda x: -1*x[0] -1*x[1] -1*x[2] -1*x[3] -1*x[4]+1 } ) 

bnds2 = ((0,1),(0,1),(0,1),(0,1),(0,1))
w_init2 = [1, 0,0,0,0]
res2 = optimize.minimize( obj_fun2 , w_init2, method='SLSQP', bounds=bnds2,constraints=cons2)

print("result2")
print(res.success,res.message)

w_bl = res.x
print("weights of optimal portfolio by BL")
print( np.round(w_bl,4 ) )
print("weights of market")
print( np.round(w_mkt,4 ) )
# [2.518e-01 2.000e-04 0.000e+00 4.828e-01 7.800e-03]
# [2.518e-01 2.000e-04 0.000e+00 4.828e-01 7.800e-03]
# [25.18, 0.0, 0.0, 48.28, 0.78%  ]

###Qs market 和BL方式算出来的值是一样的，初步怀疑可能是资产收益率数据量太小的原因，也有可能是计算过程有问题，也有可能是最优化模型有问题。
# Ana：加了一行资产收益率数据，对结果没有影响
# 该结果只使用了86.08%的仓位，似乎不能体现最大化风险收益比的问题
# [0.2275 0.6174 0.     0.016  0.    ]

#Ana，改进了最优化目标方程后，权重之和变成1，但现金工具权重从0增加至14.13%，说明非现金部分的权重基本不变，且上证50权重下降1.6%，沪深300权重增加1%。
# weights of optimal portfolio by BL
# [0.215  0.6275 0.     0.0162 0.1413]
# weights of market
# [0.215  0.6275 0.     0.0162 0.1413]






###################################################
### 5,Optimization, 以风险水平或收益率目标、sharp ratio之一为最大化目标方程
# 标准的优化求解设置：Least Squares with Linear Constraints and Bounds 
# find the efficient portfolio for a given risk or return level, and you can also maximize the Sharpe ratio.
# 5.1，计算传统mean-variance 的配置结果
# 5.2，计算 mean-var-BL 的配置结果
# 5.3，constraints， all w_ij >=0 and sum of w_ij =1 | 没有空头头寸的情况


































#### 比较市场组合和 BL组合的权重
# port = Portfolio('NumAssets', numAssets, 'lb', 0, 'budget', 1, 'Name', 'Mean Variance');
# port = setAssetMoments(port, mean(assetRetns.Variables), Sigma);
# wts = estimateMaxSharpeRatio(port);

# portBL = Portfolio('NumAssets', numAssets, 'lb', 0, 'budget', 1, 'Name', 'Mean Variance with Black-Litterman');
# portBL = setAssetMoments(portBL, mu_bl, Sigma + cov_mu);  
# wtsBL = estimateMaxSharpeRatio(portBL);





# 5,Optimization, 以风险水平或收益率目标、sharp ratio之一为最大化目标方程
#     标准的优化求解设置：Least Squares with Linear Constraints and Bounds 
#     find the efficient portfolio for a given risk or return level, and you can also maximize the Sharpe ratio.
#     5.1，计算传统mean-variance 的配置结果
#     5.2，计算 mean-var-BL 的配置结果
#     5.3，constraints， all w_ij >=0 and sum of w_ij =1 | 没有空头头寸的情况



