#api/views.py
from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response

def home(request):
    return render(request, 'api/home.html')

def load_json(request):
    return render(request, 'api/load_json.html')

