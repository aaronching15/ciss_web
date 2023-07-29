### 数据库路由规则文件=database_router.py，需要自己创建
# 在项目工程根路径下(与 settings.py 文件一级）创建 database_router.py 文件
# 匹配  settings.py 中配置 DATABASE_ROUTERS
# last | since 220823
# DatabaseAppsRouter: 路由规则的类名称，这个类是在database_router.py 文件中定义


from django.conf import settings
DATABASE_MAPPING = settings.DATABASE_APPS_MAPPING

class DatabaseAppsRouter(object):
    ### notes:这里好像没有需要自己定义的变量
	"""
	A router to control all database operations on models for different
	databases.
	In case an app is not set in settings.DATABASE_APPS_MAPPING, the router
	will fallback to the `default` database.
	Settings example:
	DATABASE_APPS_MAPPING = {'app1': 'db1', 'app2': 'db2'}
	"""
	def db_for_read(self, model, **hints):
		""""Point all read operations to the specific database."""
		if model._meta.app_label in DATABASE_MAPPING:
			return DATABASE_MAPPING[model._meta.app_label]
		return None

	def db_for_write(self, model, **hints):
		"""Point all write operations to the specific database."""
		if model._meta.app_label in DATABASE_MAPPING:
			return DATABASE_MAPPING[model._meta.app_label]
		return None

	def allow_relation(self, obj1, obj2, **hints):
		"""Allow any relation between apps that use the same database."""
		db_obj1 = DATABASE_MAPPING.get(obj1._meta.app_label)
		db_obj2 = DATABASE_MAPPING.get(obj2._meta.app_label)
		if db_obj1 and db_obj2:
			if db_obj1 == db_obj2:
				return True
			else:
				return False
				
		return None

	def allow_syncdb(self, db, model):
		"""Make sure that apps only appear in the related database."""
		if db in DATABASE_MAPPING.values():
			return DATABASE_MAPPING.get(model._meta.app_label) == db
		elif model._meta.app_label in DATABASE_MAPPING:
			return False
		return None

	def allow_migrate(self, db, app_label, model=None, **hints):
		"""
		Make sure the auth app only appears in the 'auth_db' database.
		"""
		if db in DATABASE_MAPPING.values():
			return DATABASE_MAPPING.get(app_label) == db
		elif app_label in DATABASE_MAPPING:
			return False
		return None

    ########################################################################
    ### 








