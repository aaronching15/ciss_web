# Generated by Django 2.1.5 on 2019-01-08 06:44

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('ciss_exhi', '0002_auto_20190108_1434'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='portfolio',
            name='port_author',
        ),
        migrations.RemoveField(
            model_name='portfolio',
            name='port_client',
        ),
        migrations.RemoveField(
            model_name='portfolio',
            name='port_date_pub',
        ),
        migrations.RemoveField(
            model_name='portfolio',
            name='port_supervisor',
        ),
        migrations.RemoveField(
            model_name='portfolio',
            name='port_type',
        ),
        migrations.RemoveField(
            model_name='strategy',
            name='stra_author',
        ),
        migrations.RemoveField(
            model_name='strategy',
            name='stra_client',
        ),
        migrations.RemoveField(
            model_name='strategy',
            name='stra_date_last',
        ),
        migrations.RemoveField(
            model_name='strategy',
            name='stra_date_pub',
        ),
        migrations.RemoveField(
            model_name='strategy',
            name='stra_supervisor',
        ),
    ]