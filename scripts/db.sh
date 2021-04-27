#!/bin/bash

# run from this directory
cd "${BASH_SOURCE%/*}" || exit

# # pgadmin
docker stop pgadmin
docker rm pgadmin
docker create --name pgadmin --network geonet --network-alias na_pgadmin -t \
-v v_pgadmin:/var/lib/pgadmin \
-p 8082:80 \
-e 'PGADMIN_DEFAULT_EMAIL=admin@admin.ie' \
-e 'PGADMIN_DEFAULT_PASSWORD=admin' \
dpage/pgadmin4

docker start pgadmin

# #postgis
docker stop postgis
docker rm postgis
docker create --name postgis --network geonet \
--network-alias na_postgis -t -p 25432:5432 \
-v v_postgis:/var/lib/postgresql kartoza/postgis
docker start postgis