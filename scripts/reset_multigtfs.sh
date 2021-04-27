#!/bin/bash

# run from this directory
cd "${BASH_SOURCE%/*}" || exit


# shh in our server
echo "########### connecting to server and running commands in sequence ###########"
sudo ssh -i ../../web-mapping_key.pem azureuser@40.121.42.196 \
"

docker exec dynamo_backend python manage.py migrate multigtfs zero
docker exec dynamo_backend python manage.py migrate multigtfs
docker exec dynamo_backend rm gtfs.zip
docker exec dynamo_backend wget -O gtfs.zip https://www.transportforireland.ie/transitData/google_transit_combined.zip
docker exec dynamo_backend python manage.py importgtfs gtfs.zip

"

echo "Finished"

