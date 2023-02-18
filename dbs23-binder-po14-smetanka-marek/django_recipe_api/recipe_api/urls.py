from django.contrib import admin
from django.urls import path, include
from recipe.views import recipe_view_set
from rest_framework import routers

# define the router
router = routers.DefaultRouter()
router.register(r'recipe', recipe_view_set) #the route tha will be used to access your API on the browser

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include(router.urls)),
    path('api-auth/', include('rest_framework.urls')) # Adds 'Login' link in the top right of the page

]