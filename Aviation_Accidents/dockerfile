# set the base image with specific tag/version
FROM python:3.9

# set up working directory inside the container
WORKDIR /app

# update all packages, install cron module and the text editors vim and nano 
RUN apt-get update && apt-get -y install cron 
RUN apt-get -y install vim nano

# set up working directory inside the container
WORKDIR /app

# copy and run the requirements.txt file to install the required packages.
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# copy files from  directory to the image's filesystem
COPY . .

RUN mkdir csv
# register a cron job to start the webscraper application
# it needs the absolute paths (runnning 'which python' inside the container might be useful)

# ---- CRON ----
# Run etl.py every day at 02:00
RUN echo "0 2 * * * python /app/etl.py >> /var/log/cron.log 2>&1" > /etc/cron.d/etl-cron

# Permissions + register cron
RUN chmod 0644 /etc/cron.d/etl-cron \
    && crontab /etc/cron.d/etl-cron

# Log file
RUN touch /var/log/cron.log

# Start cron in foreground
CMD ["cron", "-f"]