from rest_framework import viewsets
from .serializers import RecipeSerializer  # impor the serializer we just created
from .models import Recipe


class recipe_view_set(viewsets.ModelViewSet):
    # define queryset
    queryset = Recipe.objects.all()
    serializer_class = RecipeSerializer