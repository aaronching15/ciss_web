##################################################################
### Initialization
from django.urls import path
from . import views

##################################################################
### 添加搜索路由 haystack | 
from haystack.views import SearchView

urlpatterns = [
    path('', views.index, name='index'),
    path('index.html', views.index, name='index'),
    ### json file for index
    path('json/tree_stra_s.json', views.json_index, name='json_index'),

    ##################################################################
    ### working urls of strategy list
    path('strategy/index_stra.html', views.stra_index ),
    ### Strategy at csfunds
    ### cs-Feng,三个指数增强组合，csi300，csi500，中证中国1000，since 201911
    path('strategy/stra_cs_index_enhance3.html', views.stra_cs_index_enhance3,name="stra_cs_index_enhance3" ),
    ### 保险资金重仓股跟踪和制作模拟组合
    path('strategy/stra_cs_institute_insurance_holdings_1911.html', views.stra_cs_institute_insurance_holdings,name="institute_insurance_holdings" ),
    
    
    ### Strategy for keti2018 and test of ciss_web
    path('strategy/stra_single.html', views.stra_single,name="stra_single" ),
    path('strategy/stra_abm_rc.html', views.stra_abm_rc,name="stra_abm_rc" ),
    ### working on personal strategy 
    # path('strategy/stra_abm.html', views.stra_abm,name="stra_abm" ),
    path('strategy/stra_bond_jny.html', views.stra_bond_jny,name="stra_bond_jny" ),
    # path('strategy/stra_multi_cryjny.html', views.stra_multi_cryjny,name="stra_multi_cryjny" ),




    ##################################################################
    ### working urls of portfolio list
    path('portfolio/index_port.html', views.port_index ),
    ### working urls of single portfolio 
    path('portfolio/port_single.html', views.port_single,name="port_single" ),


    ##################################################################
    ### working urls of data
    path('data/index_data.html', views.data_index,name="data_index" ),
    path('data/data_log.html', views.data_log,name="data_log" ),
    # 数据库学习和知识
    path('data/knowledge/data_study_postgresql.html', views.data_study_postgresql,name="data_study_postgresql" ),
    
    ### 具体数据库 db_wind_wds:windn,Oracle,PLSQL：
    path('data/db_wind_wds/log_191118.html', views.data_db_wind_wds,name="data_db_wind_wds" ),
    
     ##################################################################
    ### working urls of event
    # last | since 191119
    path('event/index_event.html', views.event_index,name="event_index" ),


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
    ### 添加搜索路由 haystack | 
    path(r'search/$', SearchView(), name='haystack_search')

]