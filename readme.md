* To load data
```
python manage.py shell
from world import load
load.run()
```

* The following are taken
```
/login/ [name='login']
/logout/ [name='logout']
/password_change/ [name='password_change']
/password_change/done/ [name='password_change_done']
/password_reset/ [name='password_reset']
/password_reset/done/ [name='password_reset_done']
/reset/<uidb64>/<token>/ [name='password_reset_confirm']
/reset/done/ [name='password_reset_complete']
```

* login 
```
sudo ssh -i web-mapping_key.pem azureuser@40.121.42.196
```

* pushing
```
docker tag geodjangoapp:latest vmedves/web-mapping:geodjangoapp

docker push vmedves/web-mapping:geodjangoapp
```

- server -> azure
- domain -> namecheap
- docker -> vmedves/web-mapping:geodjangoapp

install docker 
```
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"

sudo apt-get install docker-ce docker-ce-cli containerd.io

sudo groupadd docker
sudo usermod -aG docker $USER
newgrp docker
```

ssh script
```
git clone https://github.com/Drakodav/web-mapping.git

sudo find web-mapping/ -type f -iname "*.sh" -exec chmod +x {} \;

cd web-mapping

sudo ./run.sh
```

install certbot
```
sudo snap install core; sudo snap refresh core
sudo snap install --classic certbot
sudo ln -s /snap/bin/certbot /usr/bin/certbot

# make sure nginx binaries are installed
sudo apt-get install nginx

# here we create the cert
sudo certbot --nginx
```

reset github changes 
```
git reset --hard
git pull

sudo find . -type f -iname "*.sh" -exec chmod +x {} \;
```

bash into container
```
docker exec -it django_project bash
```