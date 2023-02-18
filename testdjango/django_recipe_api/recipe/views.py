from rest_framework import viewsets
from .serializers import RecipeSerializer  # impor the serializer we just created
from .models import Recipe
from django.db import connection

cursor = connection.cursor()

class recipe_view_set(viewsets.ModelViewSet):
    # define queryset
    queryset = Recipe.objects.all()
    serializer_class = RecipeSerializer

class status_view(viewsets.ModelViewSet):
    # define queryset
    queryset = cursor.execute('SELECT version();')
    serializer_class = RecipeSerializer
