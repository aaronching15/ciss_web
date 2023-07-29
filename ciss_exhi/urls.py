# -*- encoding:"utf-8"-*-
__author__ = " ruoyu.Cheng"

'''
=============================================== 
todo: 
功能：
last 20220913 | since 20220209
Notes: 
refernce:  views.py 
Django网页变量显示的逻辑，
1，html网页代码里Request引用变量core_stra_list_cs；
2，urls.py文件里用path(html,views.xxx,name="" )连接html和views里的变量；urls.py也可以再include包括别的urls_sub.py文件；除了views.py，也可以是veiws_sub.py。
3，views.py文件里的function，从数据库模型提取数据入core_stra_list_cs，保存到输出的context。
===============================================
''' 

##################################################################
### Initialization
from django.urls import path

from ciss_exhi import views_strategy_dev,views_fund_analysis,views_fund_allocation
from . import views,views_quick
from . import views_pms_manage
from . import views_ciss_db
from . import views_data_windapi
from . import views_strategy_dev
from . import views_ppt
from . import views_strategy_allocation
from . import views_test
from django.urls import include

##################################################################
### 添加搜索路由 haystack | 

urlpatterns = [
    ################################################################## 
    ### ciss_exhi后无参数时，调用页面； http://127.0.0.1:8000/ciss_exhi/
    path('', views.index),
    ####################################################################################################################################
    ### Index 首页
    path('index.html', views.index, name='index'),

    #################################################################################################################################### 
    ### 基本面事件和观点|Fundamental Events and View
    path('event/index_event_view.html', views_ciss_db.event_view, name='event_view'),

    ####################################################################################################################################
    ##################################################################
    ### 基金分析 || 对应脚本：views_ciss_db.py 
    ### 旧版基金分析，202303开始，fund_analysis_old 功能都迁移至 fund_data_manage
    # path('index_fund_fof.html', views_fund_analysis.fund_analysis_old ,name='fund_analysis_old' ),  
    
    ################################################
    ### 基金打分和调研数据表管理 | notes: test/ 已经转移过来了
    path('fund_analysis/', views_fund_analysis.fund_analysis,name='fund_analysis' ),
    ### 设置stra_index.html 内的子页面，用于表格数据的增删查改 
    path('fund_analysis/getdata', views_fund_analysis.get_data ),
    path('fund_analysis/adddata', views_fund_analysis.add_data ),
    path('fund_analysis/deldata', views_fund_analysis.del_data ),
    path('fund_analysis/editdata', views_fund_analysis.edit_data ),
    
    ################################################
    ### 基金净值数据维护
    path('fund_data_manage/', views_fund_analysis.fund_data_manage,name='fund_data_manage' ),

    ################################################
    ### 基金分析指标和基金入池模板等
    path('fund_indi_template/', views_fund_analysis.fund_indi_template,name='fund_indi_template' ),
    
    ################################################
    ### 专用测试网页，test 目前没用了。
    path('test/', views_test.test,name='test' ), 
    
    ################################################# 
    ### sub 基金股债配置测算  
    path('fund_allocation/', views_fund_allocation.fund_allocation,name='fund_allocation' ),
       
    ####################################################################################################################################
    ##################################################################
    ### Strategy list | 策略应该包括数据分析、指标和信号、算法、策略组合、策略评估
    path('strategy/stra_index.html', views_strategy_dev.stra_index,name='stra_index' ),
    
    ### "" 表示strategy后无参数时，调用stra_index页面
    ### 设置stra_index.html 内的子页面，用于表格数据的增删查改 
    path('strategy/stra_weight/stra_weight_getdata', views_strategy_dev.stra_weight_getdata,name='stra_weight_getdata' ),



    ##################################################################
    ### stra_weight/ 策略权重管理stra_weight 
    path('strategy/stra_weight/', views_strategy_dev.stra_weight,name='stra_weight' ),
    ### 设置stra_weight 内的子页面，用于表格数据的增删查改 
    path('strategy/stra_weight/getdata', views_strategy_dev.get_data ),
    path('strategy/stra_weight/adddata', views_strategy_dev.add_data ),
    path('strategy/stra_weight/deldata', views_strategy_dev.del_data ),
    path('strategy/stra_weight/editdata', views_strategy_dev.edit_data ),
    
    ##################################################################
    ### 股债配置策略首页 views_strategy_allocation
    path('strategy/index_stra_allocation.html', views_strategy_allocation.stra_allocation,name='stra_allocation' ),

    ####################################################################################################################################
    ### 快速功能 对应脚本：views_quick.py
    path('quick.html', views_quick.quick ,name='quick' ), 

    #################################################################################################################################### 
    ### 量化策略和组合管理；    对应脚本： views_pms_manage.py等
    ##################################################################
    ### wind PMS组合管理    
    path('pms_manage.html', views_pms_manage.pms_manage ,name='pms_manage' ), 

    ##################################################################
    ### 数据库和各类数据文件管理，数据管理|Data Manage
    path('data/index_data.html', views_ciss_db.data_manage,name="data_manage" ),

    ##################################################################
    ### monitor_market_data 市场数据跟踪| 链接放在首页。
    path('monitor_market_data.html', views_data_windapi.monitor_market_data, name='monitor_market_data'),


    ##################################################################
    ### TEST Strategy；stra_index2.html是临时测试layui功能用的。
    path('ppt.html', views_ppt.ppt,name='ppt' ),
    































    ##################################################################
    ### BEFORE 2021
    ##################################################################
    ### Index of AI,machine learning,deep learning,factors
    ### 人工智能模型和相关研究
    path('ai/ai_index.html', views.ai_index,name='ai_index' ), 
    ##################################################################
    ### 注册开发日志地址，### For app ciss_exhibition
    # path('ciss_exhi', include("ciss_exhi.urls_log") ),
    path('index_log.html', views.log_index,name='log_index' ),

    ##################################################################
    ### Index of industry research list 
    ### 行业产业链研究 
    path('industry/industry_index.html', views.industry_index,name='industry_index' ),
    
    ##################################################################
    ### json file for index | 好像也没什么用了。
    path('json/tree_stra_s.json', views.json_index, name='json_index'),
    
      
    ################################################################## 
    ### Level 3 
    ### Strategy at csfunds
    ### cs-Feng,三个指数增强组合，csi300，csi500，中证中国1000，since 201911
    path('strategy/stra_cs_index_enhance3.html', views.stra_cs_index_enhance3,name="stra_cs_index_enhance3" ),
    ### 保险资金重仓股跟踪和制作模拟组合
    path('strategy/stra_cs_institute_insurance_holdings_1911.html', views.stra_cs_institute_insurance_holdings,name="institute_insurance_holdings" ),
    ### 
    path('strategy/stra_ashare_bm_index_replicate.html', views.stra_ashare_bm_index_replicate,name="stra_ashare_bm_index_replicate" ),

    ### Strategy for keti2018 and test of ciss_web
    path('strategy/stra_single.html', views.stra_single,name="stra_single" ),
    path('strategy/stra_abm_rc.html', views.stra_abm_rc,name="stra_abm_rc" ),
    ### working on personal strategy 

    # path('strategy/stra_abm.html', views.stra_abm,name="stra_abm" ),
    path('strategy/stra_bond_jny.html', views.stra_bond_jny,name="stra_bond_jny" ),
    # path('strategy/stra_multi_cryjny.html', views.stra_multi_cryjny,name="stra_multi_cryjny" ),

    ##################################################################
    ##################################################################
    ### working urls of portfolio list
    path('portfolio/index_port.html', views.port_index ),
    ### working urls of single portfolio 
    path('portfolio/port_single.html', views.port_single,name="port_single" ),

    ##################################################################
    ##################################################################
    ### working urls of data
    path('data/data_log.html', views.data_log,name="data_log" ),
    path('data/data_wind.html', views.data_wind,name="data_wind" ),
    path('data/data_ciss_web.html', views.data_ciss_web,name="data_ciss_web" ),
    path('data/data_ciss_db.html', views.data_ciss_db,name="data_ciss_db" ),
    path('data/data_rc_report.html', views.data_rc_report,name="data_rc_report" ),
    path('data/data_touyan.html', views.data_touyan,name="data_touyan" ), 
    
    # 数据库学习和知识
    path('data/knowledge/data_study_postgresql.html', views.data_study_postgresql,name="data_study_postgresql" ),
    
    ### 具体数据库 db_wind_wds:windn,Oracle,PLSQL：
    path('data/db_wind_wds/log_191118.html', views.data_db_wind_wds,name="data_db_wind_wds" ),
    
     ##################################################################
    ### working urls of event
    # last | since 191119
    path('event/index_event.html', views.event_index,name="event_index" ),

    ##################################################################
    ##################################################################
    ### working urls of docs 
    ### url-names , target place , name of this path 
    path('docs/index_ciss.html', views.docs_index ),
    
    path('docs/5min_ciss.html', views.docs_5min ),
    path('docs/data_manage.html', views.docs_data ),
    path('docs/esse_func.html', views.docs_esse ),
    path('docs/multi_asset.html', views.docs_multi ),
    path('docs/port_simu.html', views.docs_port ),
    path('docs/stra_ana.html', views.docs_stra_ana ),
    path('docs/stra_eval.html', views.docs_stra_eval ),
    path('docs/web_plat.html', views.docs_web_plat ),
    path('docs/update_coop_opensource.html', views.docs_update ),

    ##################################################################
    ### working urls for testing 
    # path('test/test.html', views.test_index ),

    ##################################################################
    ### working ETF files
    path('etf/etf_data.html', views.etf_data ,name="etf_data"),


    ##################################################################
    ##################################################################
    ### 行业研究
    ### 行业个股模型首页 
    path('industry/model_stock_index.html', views.model_stock_index,name='model_stock_index' ),

    ### 临时行业研究信息
    path('industry/temp.html', views.temp_ind,name='temp_ind' ),
    ##################################################################
    ### industry_software.html">软件产业链
    path('industry/industry_software.html', views.industry_software,name='industry_software' ), 
    ### industry_media 传媒产业链
    path('industry/industry_media.html', views.industry_media,name='industry_media' ), 
    ### industry_finance_new 新金融产业链
    path('industry/industry_finance_new.html', views.industry_finance_new,name='industry_finance_new' ), 
    ### industry_biopharmaceutical.html">生物科技和医药产业链
    path('industry/industry_biopharmaceutical.html', views.industry_biopharmaceutical,name='industry_biopharmaceutical' ), 
    ### industry_manufacturing.html 新材料和装备制造
    path('industry/industry_manufacturing.html', views.industry_manufacturing,name='industry_manufacturing' ), 

    ### industry_comsumer_staples.html 必须消费，=日常消费
    path('industry/industry_comsumer_staples.html', views.industry_comsumer_staples,name='industry_comsumer_staples' ), 
    ### industry_comsumer_discretionary.html 可选消费，=耐用消费
    path('industry/industry_comsumer_discretionary.html', views.industry_comsumer_discretionary,name='industry_comsumer_discretionary' ), 

    ### industry_huawei.html">华为产业链
    path('industry/industry_huawei.html', views.industry_huawei,name='industry_huawei' ), 
    ### industry_apple.html">苹果产业链
    path('industry/industry_apple.html', views.industry_apple,name='industry_apple' ), 
    ### industry_tesla.html">特斯拉产业链
    path('industry/industry_tesla.html', views.industry_tesla,name='industry_tesla' ), 

    ### industry_semiconductor.html">半导体产业链
    path('industry/industry_semiconductor.html', views.industry_semiconductor,name='industry_semiconductor' ), 
    ### industry_solar.html">光伏产业链
    path('industry/industry_solar.html', views.industry_solar,name='industry_solar' ), 
    ### industry_alibaba.html">阿里巴巴产业链
    path('industry/industry_alibaba.html', views.industry_alibaba,name='industry_alibaba' ), 
    ### industry_alibaba_ecommerce_mcn.html">阿里-电商和MCN产业链
    path('industry/industry_alibaba_ecommerce_mcn.html', views.industry_alibaba_ecommerce_mcn,name='industry_alibaba_ecommerce_mcn' ), 
    ### industry_alibaba_internet_finance.html">阿里-互联网金融产业链
    path('industry/industry_alibaba_internet_finance.html', views.industry_alibaba_internet_finance,name='industry_alibaba_internet_finance' ), 
    ### industry_alibaba_cloud_computing.html">阿里-云计算产业链
    path('industry/industry_alibaba_cloud_computing.html', views.industry_alibaba_cloud_computing,name='industry_alibaba_cloud_computing' ), 
    ### industry_tencent.html">腾讯产业链
    path('industry/industry_tencent.html', views.industry_tencent,name='industry_tencent' ), 
    ### industry_tencent_gaming.html">腾讯-游戏产业链
    path('industry/industry_tencent_gaming.html', views.industry_tencent_gaming,name='industry_tencent_gaming' ), 
    ### industry_tencent_online_video.html">腾讯-影视传媒产业链
    path('industry/industry_tencent_online_video.html', views.industry_tencent_online_video,name='industry_tencent_online_video' ), 
    ### industry_locallife_food_delivery.html">本地生活和外卖产业链
    path('industry/industry_locallife_food_delivery.html', views.industry_locallife_food_delivery,name='industry_locallife_food_delivery' ), 

     
    ##################################################################
    ### 添加搜索路由 haystack | 
    # path(r'search/$', SearchView(), name='haystack_search') 


]