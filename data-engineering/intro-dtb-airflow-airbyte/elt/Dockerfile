# FROM python:3.9-slim

# RUN apt-get update && apt-get install -y \
#     postgresql-client \
#     cron \
#     && rm -rf /var/lib/apt/lists/*

# WORKDIR /app

# COPY elt_script.py /app/elt_script.py
# COPY start.sh /start.sh

# RUN chmod +x /start.sh \
#     && echo "0 3 * * * python /app/elt_script.py >> /var/log/cron.log 2>&1" \
#     > /etc/cron.d/elt-cron \
#     && chmod 0644 /etc/cron.d/elt-cron \
#     && crontab /etc/cron.d/elt-cron

# CMD ["/start.sh"]


FROM python:3.8-slim

# Install PostgreSQL command-line tools
RUN apt-get update && apt-get install -y postgresql-client

# Copy the ELT script 
COPY elt_script.py .

# Set the default command to run the ELT script
CMD ["python", "elt_script.py"]