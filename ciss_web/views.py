###########################################################################
'''
list of def in this file 
def index(request): 
'''

from django.shortcuts import render
from django.http import HttpResponse

### to exhibit static html file 
from django.shortcuts import render_to_response
###  you must use csrf_protect on any views that use the csrf_token template tag, as well as those that accept the POST data.
# source https://docs.djangoproject.com/en/2.1/ref/csrf/
# source https://blog.csdn.net/weixin_40612082/article/details/80686472
from django.views.decorators.csrf import csrf_protect,requires_csrf_token,csrf_exempt

###########################################################################
### Index and template page | 网站首页
def index(request):
    ###  网站首页
    return render_to_response("ciss_exhi/index_ciss.html")

