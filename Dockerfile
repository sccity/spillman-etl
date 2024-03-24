FROM python:3.11-slim-bookworm
ENV USER=sccity
ENV GROUPNAME=$USER
ENV UID=1435
ENV GID=1435
WORKDIR /app
USER root
RUN addgroup \
    --gid "$GID" \
    "$GROUPNAME" \
&&  adduser \
    --disabled-password \
    --gecos "" \
    --home "/app" \
    --ingroup "$GROUPNAME" \
    --no-create-home \
    --uid "$UID" \
    $USER
RUN apt-get update \
  && apt-get install -y \
    cron \
    systemd \
    procps \
    git \
    nano
RUN mkdir -p /var/run && chmod 777 /var/run
COPY . /app
RUN chown -R sccity:sccity /app && chmod -R 775 /app
RUN pip install --no-cache-dir -r requirements.txt
COPY crontab /etc/cron.d/spillman_etl
RUN chmod 0644 /etc/cron.d/spillman_etl
RUN /usr/bin/crontab /etc/cron.d/spillman_etl
RUN touch /var/log/cron.log
CMD cron && tail -f /var/log/cron.log