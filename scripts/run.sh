#!/bin/bash

# run from this directory
cd "${BASH_SOURCE%/*}" || exit


# shh in our server
echo "########### connecting to server and run commands in sequence ###########"
sudo ssh -i ../../web-mapping_key.pem azureuser@40.121.42.196 \
'
git clone https://github.com/Drakodav/backend_d-dub.git;
'

# copy .env file to VM
sudo scp -i ../../web-mapping_key.pem \
../.env \
azureuser@40.121.42.196:/home/azureuser/backend_d-dub/

# # removed ML from production
# # copy machine learning files file to VM
# sudo scp -i ../../web-mapping_key.pem \
# ../ml/processing/output/gtfsr_historical_means.hdf5 \
# azureuser@40.121.42.196:/home/azureuser/backend_d-dub/ml/processing/output/

# sudo scp -i ../../web-mapping_key.pem \
# ../ml/processing/output/stop_time_data.hdf5 \
# azureuser@40.121.42.196:/home/azureuser/backend_d-dub/ml/processing/output/

# sudo scp -i ../../web-mapping_key.pem \
# ../ml/processing/output/gtfsr_model.json \
# azureuser@40.121.42.196:/home/azureuser/backend_d-dub/ml/processing/output/


# finish deploying django and cleaning up
echo "deploying backend"
sudo ssh -i ../../web-mapping_key.pem azureuser@40.121.42.196 \
'
sudo find backend_d-dub/ -type f -iname "*.sh" -exec chmod +x {} \;
cd backend_d-dub/scripts;
# sudo ./rabbitmq.sh
sudo ./django.sh;
# sudo ./nginx.sh;
cd ../.. ;
sudo rm -r backend_d-dub;
 
'

echo "Cleaning up..."
echo "Finished updating django to latest version"
