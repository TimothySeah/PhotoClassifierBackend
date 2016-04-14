from django.conf.urls import include, url
from django.contrib import admin

from inventory import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^meancov$', views.meancov, name='meancov'),
    url(r'^init$', views.init, name='init'),
    url(r'^init2$', views.init2, name='init2'),
    url(r'^testing$', views.testing, name='testing'),
    url(r'^admin/', include(admin.site.urls)),
]