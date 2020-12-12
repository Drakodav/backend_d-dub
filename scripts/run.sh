#!/bin/bash

# shh in our server
echo "########### connecting to server and run commands in sequence ###########"
sudo ssh -i web-mapping_key.pem azureuser@40.121.42.196 \
'
git clone https://github.com/Drakodav/backend_d-dub.git;
sudo find backend_d-dub/ -type f -iname "*.sh" -exec chmod +x {} \;
cd backend_d-dub/scripts;
sudo ./django.sh;
sudo ./nginx.sh;
cd ../.. ;
sudo rm -r backend_d-dub;
 
'

echo "Cleaning up..."
echo "Finished updating django to latest version"
