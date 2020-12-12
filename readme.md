- Install Backend

```
python 3.8 -m venv .denv
source ./.denv/bin.activate
pip install pipenv
pipenv install
python manage.py run
```

- import gtfs data

```
python manage.py importgtfs [--name name_of_feed] path/to/gtfsfeed.zip
```

- problems and fixes

  - on first install, postgresql is required for psycopg2
  - also sudo apt-get install libpq-dev python-dev
  - also sudp apt-get -y install build-essential python-cffi libcairo2 libpango-1.0-0 libpangocairo-1.0-0 libgdk-pixbuf2.0-0 libffi-dev shared-mime-info
  - or brew install gdal --HEAD && brew install gdal

- make all bash scripts executable

```
sudo find . -type f -iname "*.sh" -exec chmod +x {} \;
```

- importing gtfs data

```
docker exec -it dynamo_backend bash
rm gtfs.zip
wget -O gtfs.zip https://transitfeeds.com/p/transport-for-ireland/782/latest/download
python manage.py importgtfs gtfs.zip
```
