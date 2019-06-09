# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey has `on_delete` set to the desired behavior.
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class AuthGroup(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    name = models.CharField(unique=True, max_length=80)

    class Meta:
        managed = False
        db_table = 'auth_group'


class AuthGroupPermissions(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING)
    permission = models.ForeignKey('AuthPermission', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_group_permissions'
        unique_together = (('group', 'permission'),)


class AuthPermission(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    content_type = models.ForeignKey('DjangoContentType', models.DO_NOTHING)
    codename = models.CharField(max_length=100)
    name = models.CharField(max_length=255)

    class Meta:
        managed = False
        db_table = 'auth_permission'
        unique_together = (('content_type', 'codename'),)


class AuthUser(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    password = models.CharField(max_length=128)
    last_login = models.DateTimeField(blank=True, null=True)
    is_superuser = models.BooleanField()
    username = models.CharField(unique=True, max_length=150)
    first_name = models.CharField(max_length=30)
    email = models.CharField(max_length=254)
    is_staff = models.BooleanField()
    is_active = models.BooleanField()
    date_joined = models.DateTimeField()
    last_name = models.CharField(max_length=150)

    class Meta:
        managed = False
        db_table = 'auth_user'


class AuthUserGroups(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_user_groups'
        unique_together = (('user', 'group'),)


class AuthUserUserPermissions(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)
    permission = models.ForeignKey(AuthPermission, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_user_user_permissions'
        unique_together = (('user', 'permission'),)


class CissExhiAsset(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    asset_industry = models.CharField(max_length=200)
    asset_type = models.CharField(max_length=200)
    asset_market = models.CharField(max_length=200)
    asset_country = models.CharField(max_length=200)

    class Meta:
        managed = False
        db_table = 'ciss_exhi_asset'


class CissExhiBasics(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    industry = models.CharField(max_length=200)
    type_asset = models.CharField(max_length=200)
    date_published = models.DateTimeField()
    date_last = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'ciss_exhi_basics'


class CissExhiBond(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    bond_asset = models.ForeignKey(CissExhiAsset, models.DO_NOTHING)
    bond_port = models.ForeignKey('CissExhiPortfolio', models.DO_NOTHING)
    bond_stra = models.ForeignKey('CissExhiStrategy', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'ciss_exhi_bond'


class CissExhiBondDerivative(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    deriv_type = models.CharField(max_length=200)
    deriv_bond = models.ForeignKey(CissExhiBond, models.DO_NOTHING)
    deriv_stra = models.ForeignKey('CissExhiStrategy', models.DO_NOTHING)
    derive_port = models.ForeignKey('CissExhiPortfolio', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'ciss_exhi_bond_derivative'


class CissExhiCashTool(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    bond_asset = models.ForeignKey(CissExhiAsset, models.DO_NOTHING)
    bond_port = models.ForeignKey('CissExhiPortfolio', models.DO_NOTHING)
    bond_stra = models.ForeignKey('CissExhiStrategy', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'ciss_exhi_cash_tool'


class CissExhiDb(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    name_db = models.CharField(max_length=200)
    init_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'ciss_exhi_db'


class CissExhiIndex(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    bench_asset = models.ForeignKey(CissExhiAsset, models.DO_NOTHING)
    bench_port = models.ForeignKey('CissExhiPortfolio', models.DO_NOTHING)
    bench_stra = models.ForeignKey('CissExhiStrategy', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'ciss_exhi_index'


class CissExhiIndexDerivative(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    deriv_type = models.CharField(max_length=200)
    deriv_index = models.ForeignKey(CissExhiIndex, models.DO_NOTHING, db_column='deriv_Index_id')  # Field name made lowercase.
    deriv_stra = models.ForeignKey('CissExhiStrategy', models.DO_NOTHING)
    derive_port = models.ForeignKey('CissExhiPortfolio', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'ciss_exhi_index_derivative'


class CissExhiMultiAsset(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    asset_group = models.CharField(max_length=200)
    asset_author = models.CharField(max_length=200)
    asset_client = models.CharField(max_length=200)
    asset_date_pub = models.DateTimeField()
    asset_name = models.CharField(max_length=200)
    asset_supervisor = models.CharField(max_length=200)
    asset_type = models.CharField(max_length=200)

    class Meta:
        managed = False
        db_table = 'ciss_exhi_multi_asset'


class CissExhiPortfolio(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    port_name = models.CharField(max_length=200)
    port_date_pub = models.DateTimeField()
    port_author = models.CharField(max_length=200)
    port_client = models.CharField(max_length=200)
    port_supervisor = models.CharField(max_length=200)
    port_type = models.CharField(max_length=200)

    class Meta:
        managed = False
        db_table = 'ciss_exhi_portfolio'


class CissExhiStock(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    stock_asset = models.ForeignKey(CissExhiAsset, models.DO_NOTHING)
    stock_port = models.ForeignKey(CissExhiPortfolio, models.DO_NOTHING)
    stock_stra = models.ForeignKey('CissExhiStrategy', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'ciss_exhi_stock'


class CissExhiStockDerivative(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    deriv_type = models.CharField(max_length=200)
    deriv_stock = models.ForeignKey(CissExhiStock, models.DO_NOTHING)
    deriv_stra = models.ForeignKey('CissExhiStrategy', models.DO_NOTHING)
    derive_port = models.ForeignKey(CissExhiPortfolio, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'ciss_exhi_stock_derivative'


class CissExhiStrategy(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    stra_author = models.CharField(max_length=200)
    stra_date_pub = models.DateTimeField()
    stra_client = models.CharField(max_length=200)
    stra_date_last = models.DateTimeField()
    stra_name = models.CharField(max_length=200)
    stra_supervisor = models.CharField(max_length=200)
    stra_type = models.CharField(max_length=200)

    class Meta:
        managed = False
        db_table = 'ciss_exhi_strategy'


class CissExhiUserCiss(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    position = models.CharField(max_length=200)
    group_ciss = models.CharField(max_length=200)
    user_date_in = models.DateTimeField()
    user_date_last = models.DateTimeField()
    user_active = models.BooleanField()
    user_assett = models.ForeignKey(CissExhiAsset, models.DO_NOTHING)
    user_ciss = models.ForeignKey(AuthUser, models.DO_NOTHING)
    user_data = models.ForeignKey(CissExhiDb, models.DO_NOTHING)
    user_port = models.ForeignKey(CissExhiPortfolio, models.DO_NOTHING)
    user_stra = models.ForeignKey(CissExhiStrategy, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'ciss_exhi_user_ciss'


class DjangoAdminLog(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    action_time = models.DateTimeField()
    object_id = models.TextField(blank=True, null=True)
    object_repr = models.CharField(max_length=200)
    change_message = models.TextField()
    content_type = models.ForeignKey('DjangoContentType', models.DO_NOTHING, blank=True, null=True)
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)
    action_flag = models.PositiveSmallIntegerField()

    class Meta:
        managed = False
        db_table = 'django_admin_log'


class DjangoContentType(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    app_label = models.CharField(max_length=100)
    model = models.CharField(max_length=100)

    class Meta:
        managed = False
        db_table = 'django_content_type'
        unique_together = (('app_label', 'model'),)


class DjangoMigrations(models.Model):
    id = models.IntegerField(primary_key=True)  # AutoField?
    app = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    applied = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'django_migrations'


class DjangoSession(models.Model):
    session_key = models.CharField(primary_key=True, max_length=40)
    session_data = models.TextField()
    expire_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'django_session'
