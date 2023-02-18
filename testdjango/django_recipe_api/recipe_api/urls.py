"""recipe_api URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include, re_path
from recipe.views import recipe_view_set
from recipe.views import status_view

from rest_framework import routers

# define the router
router = routers.DefaultRouter()
router.register(r'recipe', recipe_view_set) #the route tha will be used to access your API on the browser

urlpatterns = [
    path('admin/', admin.site.urls),
    re_path(r'v1/status', status_view, name='version'),
    path('', include(router.urls)),
    path('api-auth/', include('rest_framework.urls')) # Adds 'Login' link in the top right of the page

]
