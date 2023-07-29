"""
Django settings for ciss_web project.

Generated by 'django-admin startproject' using Django 2.0.

For more information on this file, see
https://docs.djangoproject.com/en/2.0/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/2.0/ref/settings/
"""

import os

########################################################################
### Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/2.0/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = ')ub9ce*jaq#49i#nm=5026tm)ge25uvsj1z6m1_+3@*js8)%00'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True
# sub 克隆静态文件到根目录
# reference website：https://github.com/newpanjing/simpleui/blob/master/QUICK.md

ALLOWED_HOSTS = []


# Application definition

##################################################################################
### learning django-rest-framework | source http://hao.jobbole.com/django-rest-framework/

INSTALLED_APPS = [
    #######################################################
    ###  https://github.com/newpanjing/simpleui
    "simpleui",
    ### 旧版的admin美化插件
    #'django_admin_bootstrapped',
    #######################################################
    ### Original part
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',

    #######################################################
    # 注册静态文件
    'django.contrib.staticfiles',
    # Make sure to include the default installed apps here.
    # 'rest_framework'
    #######################################################
    ### Include app ciss_exhi | 190107 rc
    'ciss_exhi.apps.CissExhiConfig',
    #######################################################
    ### todo items 
    # haystack要放在应用的上面
    'haystack',
    
]

# REST_FRAMEWORK = {
#     # Use Django's standard `django.contrib.auth` permissions,
#     # or allow read-only access for unauthenticated users.
#     'DEFAULT_PERMISSION_CLASSES': [
#         'rest_framework.permissions.DjangoModelPermissionsOrAnonReadOnly'
#     ]
# }

##################################################################################
### 
DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'

##################################################################################
### 

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    # to avoid Forbidden(403) CSRF verification failed.Request aborted错误
    # 'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'ciss_web.urls'

########################################################################
### 
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        ### 设置模版路径 os.path.join(BASE_DIR, 'static/templates')
        'DIRS': [ os.path.join(BASE_DIR, 'static/templates') ],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'ciss_web.wsgi.application'


########################################################################
###  Database
# https://docs.djangoproject.com/en/2.0/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
    },
    ############################################
    ### Add a second db: 
    ### 加db要做的事：CMD里运行：step1：python manage.py migrate --database=ciss_db
    ### step2：python manage.py makemigrations
    ### step2：python manage.py migrate
    "ciss_db":{
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(BASE_DIR, 'db_funda.sqlite3'),
    },
    ############################################
    ### Add db: Postgresql
    # "ciss_db":{
    #     'ENGINE':'django.db.backends.postgresql_psycopg2',
    #     'NAME':'ciss_db',# 数据库名字
    #     'USER':'ciss_rc',# 登录用户名
    #     'PASSWORD':'ciss_rc',
    #     'HOST':'127.0.0.1',# 随便设的,127.10.0.1
    #     'PORT':'5432', # 随便设的，ideal 178
    # },
    ### mysql 
    # 'local': {
    #     'ENGINE': 'django.db.backends.mysql',
    #     'NAME': 'test2',
    #     'USER': 'root',
    #     'PASSWORD': '12345',
    #     'HOST': '127.0.0.1',
    #     'PORT': '3306'
    # }

}

########################################################################
### ciss_web: django项目名称(project_name)
# database_router: 定义路由规则database_router.py 文件名称, 这个文件名可以自己定义
# DatabaseAppsRouter: 路由规则的类名称，这个类是在database_router.py 文件中定义
# 原文链接：https://blog.csdn.net/aa2528877987/article/details/122786813
DATABASE_ROUTERS = [ "ciss_web.database_router.DatabaseAppsRouter" ]

### 设置APP对应的数据库路由表；数据库路由规则文件=database_router.py，需要自己创建
# 在项目工程根路径下(与 settings.py 文件一级）创建 database_router.py 文件
# 为了使django自己的表也创建到你自己定义的数据库中，你可以指定 :admin, auth, contenttypes, sessions 到设定的数据库中，如果不指定则会自动创建到默认（default）的数据库中

DATABASE_APPS_MAPPING = {
	# 'app_name':'database_name',
    ### 管理默认功能对应地数据库
	'admin': 'default',
	'auth': 'default',
	'contenttypes': 'default',
	'sessions': 'default',
    ### 应用app对应地数据库
	'ciss_exhi': 'ciss_db', 

    ### 
}



########################################################################
###  Password validation
# https://docs.djangoproject.com/en/2.0/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


########################################################################
###  Internationalization
# https://docs.djangoproject.com/en/2.0/topics/i18n/
# LANGUAGE_CODE = 'en-us'
# 新版本的django包版本中只有zh_Hans目录，没有zh_CN
LANGUAGE_CODE = 'zh-Hans'

# TIME_ZONE = 'UTC'
TIME_ZONE = 'Asia/Shanghai'

USE_I18N = True

USE_L10N = True

USE_TZ = True

###########################################################################
### STATIC_URL的意思是将静态文件的http可访问路径设为/static/，而STATICFILES_DIRS则是真正存储静态文件的目录，你可以通过STATICFILES_DIRS添加多个静态文件存储目录。
# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/2.0/howto/static-files/

STATIC_URL = '/static/'

###########################################################################
### 设置静态文件目录
### 在自己的app下面创建2个目录 static 和 templates，static下存放静态文件，templates下存放网页模板文件

STATICFILES_DIRS = (
    os.path.join(os.path.join(BASE_DIR, 'static')),
    ### 增加系统静态图片目录,todo
    
)

###########################################################################
### 3rd party modules | since 191122
###########################################################################
### 安装搜索插件 haystack | 
HAYSTACK_CONNECTIONS = {
    'default': {
        'ENGINE': 'haystack.backends.whoosh_backend.WhooshEngine',
        'PATH': os.path.join(os.path.dirname(__file__), 'whoosh_index'),
    },
    
}



















