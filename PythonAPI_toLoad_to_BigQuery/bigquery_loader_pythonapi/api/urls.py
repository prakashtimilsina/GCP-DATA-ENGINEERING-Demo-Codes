from django.urls import path
from api import views

urlpatterns = [
path('', views.home, name='home'),
path('api/load-json/', views.LoadJsonView.as_view(), name='load_json'),
]
