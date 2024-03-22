FROM python:3.11-slim-bookworm
WORKDIR /app
RUN apt-get update \
  && apt-get install -y \
    cron \
    procps \
    nano
COPY ./requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
COPY crontab /etc/cron.d/spillman_etl
RUN /usr/bin/crontab /etc/cron.d/spillman_etl
CMD tail -f /dev/null