#!/bin/bash

# run from this directory
cd "${BASH_SOURCE%/*}" || exit

#django
docker stop dynamo_backend
docker rm dynamo_backend

docker rmi dynamo_backend_img
docker build -t dynamo_backend_img ../

docker create --name dynamo_backend --network geonet \
--network-alias na_dynamo_backend -t \
dynamo_backend_img
# -p 8005:8001 \

docker start dynamo_backend

# run default config precautions
docker exec dynamo_backend python manage.py makemigrations
docker exec dynamo_backend python manage.py migrate
docker exec dynamo_backend python manage.py collectstatic --noinput


# start supervisor service
docker exec dynamo_backend service supervisor start
docker exec dynamo_backend supervisorctl start all