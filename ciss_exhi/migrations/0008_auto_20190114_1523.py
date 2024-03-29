# Generated by Django 2.1.5 on 2019-01-14 07:23

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ciss_exhi', '0007_portfolio_port_id'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='strategy',
            name='stra_type',
        ),
        migrations.AddField(
            model_name='strategy',
            name='stra_code',
            field=models.CharField(default='port_symbol_01', max_length=200),
        ),
        migrations.AddField(
            model_name='strategy',
            name='stra_hier_1',
            field=models.CharField(default='hier_1', max_length=200),
        ),
        migrations.AddField(
            model_name='strategy',
            name='stra_hier_2',
            field=models.CharField(default='hier_2', max_length=200),
        ),
        migrations.AddField(
            model_name='strategy',
            name='stra_hier_3',
            field=models.CharField(default='hier_3', max_length=200),
        ),
        migrations.AddField(
            model_name='strategy',
            name='stra_hier_4',
            field=models.CharField(default='hier_4', max_length=200),
        ),
        migrations.AddField(
            model_name='strategy',
            name='stra_intro',
            field=models.CharField(default='intro', max_length=200),
        ),
        migrations.AddField(
            model_name='strategy',
            name='stra_report_type',
            field=models.CharField(default='funda', max_length=200),
        ),
        migrations.AddField(
            model_name='strategy',
            name='stra_target',
            field=models.CharField(default='price_or_return', max_length=200),
        ),
        migrations.AlterField(
            model_name='strategy',
            name='stra_name',
            field=models.CharField(default='rc_abm_01', max_length=200),
        ),
    ]
