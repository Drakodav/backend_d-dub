FROM python:3.8

# update and install gdal
RUN apt-get -y update && apt-get -y upgrade && apt-get -y install libgdal-dev

# Make a working directory in the image and set it as working dir
RUN mkdir -p /user/src/app
WORKDIR /usr/src/app
# make sure that pip is installed and to date
RUN pip install --upgrade pip setuptools wheel
RUN /usr/local/bin/python -m pip install --upgrade pip

# Get the following libraries. We can install them "globally" on 
# the image as it will contain only our project
RUN apt-get -y install build-essential python-cffi libcairo2 libpango-1.0-0 libpangocairo-1.0-0 libgdk-pixbuf2.0-0 libffi-dev shared-mime-info

# Now copy this to the image and install everything in it.
COPY requirements.txt /usr/src/app
RUN pip install -r requirements.txt

# copy this directory into image
COPY . /usr/src/app

# make sure static files are up to date and available 
RUN python manage.py collectstatic --no-input

# copy supervisor conf files 
COPY supervisor /etc/supervisor/conf.d

# make log files available
RUN mkdir -p /var/log/celery && touch /var/log/celery/worker.log
RUN mkdir -p /var/log/celery && touch /var/log/celery/beat.log

# Make supervisor aware of the new confs and start supervisor service
# RUN service supervisor start
# RUN supervisorctl reread
# RUN supervisorctl update
# RUN supervisorctl start all

# expose localhost 8002 on the image
# EXPOSE 8002

# run uwsgi 
CMD [ "uwsgi", "--ini", "uwsgi.ini" ]
