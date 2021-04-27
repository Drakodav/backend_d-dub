#!/bin/bash

# run from this directory
cd "${BASH_SOURCE%/*}" || exit


# shh in our server
echo "########### connecting to server and running commands in sequence ###########"
sudo ssh -i ../../web-mapping_key.pem azureuser@40.121.42.196 \
'
sudo service stop nginx
docker start $(docker ps -a -q)
docker start nginx_cert
docker exec dynamo_backend service supervisor start
docker exec dynamo_backend supervisorctl start all
'

echo "Finished"

