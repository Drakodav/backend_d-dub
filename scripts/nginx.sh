#!/bin/bash

# run from this directory
cd "${BASH_SOURCE%/*}" || exit

LOCAL=DESKTOP-9D122S4

if [ $HOSTNAME == $LOCAL ]
then
echo "Local Docker Version"
#remove container
docker stop nginx 
docker rm nginx

# remove image
docker image rm nginx_gis

docker build -t nginx_gis -f ./nginx/Dockerfile.local ./nginx/
echo "done building image"

echo "creating container"
docker create --name nginx \
--network geonet -t -p 8080:80 \
-v v_nginx:/usr/share/nginx/html nginx_gis

echo "starting..."
docker start nginx
else
echo "Live Docker Version"
#remove container
docker stop nginx_cert
docker rm nginx_cert

# remove image
docker image rm nginx_gis_cert

docker build -t nginx_gis_cert -f ./nginx/Dockerfile.live ./nginx/
echo "done building image"

echo "creating container"
docker create --name nginx_cert --network geonet --network-alias na_nginx \
-p 80:80 -p 443:443 -t \
-v wmap_web_data:/usr/share/nginx/html \
-v /etc/letsencrypt:/etc/letsencrypt \
-v /var/www/certbot \
-v html_data:/usr/share/nginx/html/static \
nginx_gis_cert
# -v $HOME/nginx_gis_cert/conf:/etc/nginx/conf.d \


echo "starting..."
docker start nginx_cert
fi