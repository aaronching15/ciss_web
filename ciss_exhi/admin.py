from django.contrib import admin

# Register your models here.
#################################################################################
### Import ciss_exhi app from 
from .models import User_ciss,Asset,Stock,Bond,Index,Strategy,Portfolio,Stra_Port_links,Multi_asset,DB

admin.site.register(User_ciss)
admin.site.register(Asset)
admin.site.register(Stock)
admin.site.register(Bond)
admin.site.register(Index)
# admin.site.register(Strategy)
# admin.site.register(Portfolio)
admin.site.register(Stra_Port_links)
    
admin.site.register(Multi_asset)
admin.site.register(DB)

#####################################################################################
### test 
from .models import Person,Group,Membership

admin.site.register(Person)
admin.site.register(Group)
admin.site.register(Membership)



#####################################################################################
### Strategy display admin | 190114
# source https://www.cnblogs.com/wumingxiaoyao/p/6928297.html
# 模型管理器

class Stra_Admin(admin.ModelAdmin):
    list_display=('stra_name', 'stra_code', 'stra_hier_1', 'stra_hier_2', 'stra_hier_3', 'stra_hier_4','stra_author',"stra_supervisor","stra_date_last","stra_link" )
     
#在admin中注册绑定
admin.site.register(Strategy, Stra_Admin)

#####################################################################################
### Portfolio display admin | 190114
class Port_Admin(admin.ModelAdmin):
    list_display=('port_name', 'port_type', 'port_author', 'port_supervisor', 'port_client', 'port_date_last',"port_path" )
     
#在admin中注册绑定
admin.site.register(Portfolio, Port_Admin)