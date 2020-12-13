# Dynamo Dublin - Backend Api

Api - https://api.thev-lad.com/api/gtfs/  
Admin - https://api.thev-lad.com/api/admin/  
PgAdmin4 - https://api.thev-lad.com/pgadmin

## Brief

-   The main objective of the backend django application is to provide an api that a frontend can query.
-   The reason behind this architecture choice is to have a complete separation of concern of the two workflows.
-   From the links above you can browse the api.
-   GTFS (General Transit Feed Specification) data is being served.

-   For this part of the project I am most proud of my generic function that can serialize, display and filter all the models in the gtfs domain.

-   The current system has in place a very powerful and queryable api, the only thing left to do is to put in a few sprints of frontend development work in order to turn this into a usefule piece of technology.

*   Install Backend Django Rest FrameWork Api

```
python 3.8 -m venv .denv
source ./.denv/bin.activate
pip install pipenv
pipenv install
python manage.py run
```

-   import gtfs data

```
python manage.py importgtfs [--name name_of_feed] path/to/gtfsfeed.zip
```

-   problems and fixes

    -   on first install, postgresql is required for psycopg2
    -   also sudo apt-get install libpq-dev python-dev
    -   also sudp apt-get -y install build-essential python-cffi libcairo2 libpango-1.0-0 libpangocairo-1.0-0 libgdk-pixbuf2.0-0 libffi-dev shared-mime-info
    -   or brew install gdal --HEAD && brew install gdal

-   make all bash scripts executable

```
sudo find . -type f -iname "*.sh" -exec chmod +x {} \;
```

-   importing gtfs data

```
docker exec -it dynamo_backend bash
rm gtfs.zip
wget -O gtfs.zip https://transitfeeds.com/p/transport-for-ireland/782/latest/download
python manage.py importgtfs gtfs.zip
```
