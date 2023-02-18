from rest_framework import viewsets
from .serializers import RecipeSerializer
from .models import Recipe
from django.db import connection
from django.db import connection, transaction
from django.http import JsonResponse

class recipe_view_set(viewsets.ModelViewSet):
    # define queryset
    queryset = Recipe.objects.all()
    serializer_class = RecipeSerializer

def status_view(request):
    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    row = cursor.fetchone()
    return JsonResponse({'version': row})
