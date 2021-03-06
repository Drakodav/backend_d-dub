#!/bin/bash

# run from this directory
cd "${BASH_SOURCE%/*}" || exit

#django
docker stop celery_broker
docker rm celery_broker

docker create --name celery_broker --network geonet \
--network-alias na_celery_broker -t \
-e RABBITMQ_DEFAULT_USER=admin \
-e RABBITMQ_DEFAULT_PASS=admin \
rabbitmq
# -p 5672:5672 \

docker start celery_broker