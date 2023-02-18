from dataclasses import fields
from rest_framework import serializers
from .models import Recipe


# create a serializer
class RecipeSerializer(serializers.Serializer):
    # initialize model and fields you want to serialize
    class Meta:
        model = Recipe
        fields = ('title', 'time_minutes', 'ingredients')