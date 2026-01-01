FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    postgresql-client \
    cron \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY elt_script.py /app/elt_script.py
COPY start.sh /start.sh

RUN chmod +x /start.sh \
    && echo "0 3 * * * python /app/elt_script.py >> /var/log/cron.log 2>&1" \
    > /etc/cron.d/elt-cron \
    && chmod 0644 /etc/cron.d/elt-cron \
    && crontab /etc/cron.d/elt-cron

CMD ["/start.sh"]
