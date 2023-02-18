from rest_framework import viewsets
from .serializers import dbsSerializer
from django.db import connection
from django.db import connection, transaction
from django.http import JsonResponse

# class dbs_view_set(viewsets.ModelViewSet):
#     # define queryset
#     queryset = dbs.objects.all()
#     serializer_class = dbsSerializer

def status_view(request):
    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    row = cursor.fetchone()
    return JsonResponse({'version': row})
