from django.db import models

class Recipe(models.Model):
    """Recipe object"""
    title = models.CharField(max_length=255)
    time_minutes = models.IntegerField()
    ingredients = models.CharField(max_length=255)
    def __str__(self):
        return self.title