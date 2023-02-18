from dataclasses import fields
from rest_framework import serializers
from .models import dbs


class dbsSerializer(serializers.Serializer):
    # initialize model and fields you want to serialize
    class Meta:
        model = dbs
        fields = ('title')
