"""ciss_web URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls')) 
"""
from django.contrib import admin
from django.urls import path

##################################################################################
### include for app ciss_exhi
from django.urls import include
from . import views

urlpatterns = [ 
    ### Define index from ciss_exhi 
    path('index.html', views.index, name='index'),
    
    
    ### admin page 
    path('admin/', admin.site.urls),
    ### urls.py from ciss_exhi
    path('ciss_exhi/', include("ciss_exhi.urls") ),
    path('ciss_exhi/', include("ciss_exhi.urls_log") ),

]


### 从以前的django_01/django_01里学的
from django.conf.urls.static import static
from django.conf import settings
urlpatterns += static(settings.STATIC_URL, document_root = settings.STATIC_ROOT )




##################################################################################
### learning django-rest-framework | source http://hao.jobbole.com/django-rest-framework/

# from django.conf.urls import url, include
# from django.contrib.auth.models import User
# from rest_framework import serializers, viewsets, routers
 
# # Serializers define the API representation.
# class UserSerializer(serializers.HyperlinkedModelSerializer):
#     class Meta:
#         model = User
#         fields = ('url', 'username', 'email', 'is_staff')
 
 
# # ViewSets define the view behavior.
# class UserViewSet(viewsets.ModelViewSet):
#     queryset = User.objects.all()
#     serializer_class = UserSerializer
 
 
# # Routers provide a way of automatically determining the URL conf.
# router = routers.DefaultRouter()
# router.register(r'users', UserViewSet)
 
 
# # Wire up our API using automatic URL routing.
# # Additionally, we include login URLs for the browsable API.
# urlpatterns = [
#     url(r'^', include(router.urls)),
#     url(r'^api-auth/', include('rest_framework.urls', namespace='rest_framework'))
# ]

##################################################################################
###   
