# Generated by Django 2.1.5 on 2019-01-09 02:41

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ciss_exhi', '0006_auto_20190109_0853'),
    ]

    operations = [
        migrations.AddField(
            model_name='portfolio',
            name='port_id',
            field=models.CharField(default='1544021284', max_length=200),
        ),
    ]
