# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
===============================================
Function:
功能：momemtum strategy script 
last update 181114 | since 181114
Menu : 

todo:


Notes:


===============================================
'''
















#####################################################################
### todo, VIP: 策略开发模块： create strategy_suites{data-in, indicator,strategy or function, algorithm or optimization, signals or estimation }

# get analytical module and strategy modules for single stock
from db.analysis_indicators import indicator_momentum  
for temp_code in stockpool_df['code'] :
    code = temp_code    
    # todo：需要下载前推至少100天的市场数据！~ 
    (code_head,code_df)=data_wind_0.load_quotes(config_IO_0,code,date_start,date_end,quote_type)
    print("666=============")
    print( code_df.info() )
    # indicators： get best(MA) type indicator 
    
    # get analytical moudle
    
    indi_mom = indicator_momentum('')
    code_df_ana = indi_mom.indi_mom_ma_all(code_head,code_df)
    print('code_df_ana')
    print( code_df_ana.info() )
    # Output analyzing DataFrame

    # get strategy module for single sotck 
    # strategy group：get best strategy 
