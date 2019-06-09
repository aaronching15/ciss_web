# Generated by Django 2.1.5 on 2019-01-08 06:53

from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('ciss_exhi', '0003_auto_20190108_1444'),
    ]

    operations = [
        migrations.CreateModel(
            name='Stra_Port_links',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('user_connect', models.CharField(default='rc', max_length=64)),
                ('date_connect', models.DateField()),
            ],
        ),
        migrations.AddField(
            model_name='portfolio',
            name='port_author',
            field=models.CharField(default='rc', max_length=200),
        ),
        migrations.AddField(
            model_name='portfolio',
            name='port_client',
            field=models.CharField(default='rc', max_length=200),
        ),
        migrations.AddField(
            model_name='portfolio',
            name='port_date_pub',
            field=models.DateTimeField(default=django.utils.timezone.now, verbose_name='date published'),
        ),
        migrations.AddField(
            model_name='portfolio',
            name='port_supervisor',
            field=models.CharField(default='rc', max_length=200),
        ),
        migrations.AddField(
            model_name='portfolio',
            name='port_type',
            field=models.CharField(default='market', max_length=200),
        ),
        migrations.AddField(
            model_name='strategy',
            name='stra_author',
            field=models.CharField(default='rc', max_length=200),
        ),
        migrations.AddField(
            model_name='strategy',
            name='stra_client',
            field=models.CharField(default='gy', max_length=200),
        ),
        migrations.AddField(
            model_name='strategy',
            name='stra_date_last',
            field=models.DateTimeField(default=django.utils.timezone.now, verbose_name='last update'),
        ),
        migrations.AddField(
            model_name='strategy',
            name='stra_date_pub',
            field=models.DateTimeField(default=django.utils.timezone.now, verbose_name='date published'),
        ),
        migrations.AddField(
            model_name='strategy',
            name='stra_supervisor',
            field=models.CharField(default='rc', max_length=200),
        ),
        migrations.AlterField(
            model_name='portfolio',
            name='port_name',
            field=models.CharField(default='rc_abm', max_length=200),
        ),
        migrations.AlterField(
            model_name='strategy',
            name='stra_name',
            field=models.CharField(default='rc_abm', max_length=200),
        ),
        migrations.AlterField(
            model_name='strategy',
            name='stra_type',
            field=models.CharField(default='funda', max_length=200),
        ),
        migrations.AddField(
            model_name='stra_port_links',
            name='portfolio',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='ciss_exhi.Portfolio'),
        ),
        migrations.AddField(
            model_name='stra_port_links',
            name='strategy',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='ciss_exhi.Strategy'),
        ),
        migrations.AddField(
            model_name='strategy',
            name='stra_port_list',
            field=models.ManyToManyField(through='ciss_exhi.Stra_Port_links', to='ciss_exhi.Portfolio'),
        ),
    ]