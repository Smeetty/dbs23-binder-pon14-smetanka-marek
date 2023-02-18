### Migrate data

sudo docker-compose run django_dbs_api sh -c "python manage.py makemigrations"

sudo docker-compose run django_dbs_api sh -c "python manage.py migrate"

###
sudo docker-compose build 

sudo docker-compose
