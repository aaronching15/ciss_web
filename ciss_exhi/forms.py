from django import forms

##########################################################################
### create a default type form 
# source https://code.ziqiangxuetang.com/django/django-forms.html
# source django.pdf\using django\owrking with forms P230,236/1894

##########################################################################
### form for single portfolio 

# class AddForm(forms.Form):
class form_port(forms.Form):
    path_base = forms.CharField(label='path_base', max_length=300)
    port_name = forms.CharField(label='port_name', max_length=300)
    port_id = forms.CharField(label='port_id', max_length=300)
    date_last = forms.CharField(label='date_last', max_length=300)


