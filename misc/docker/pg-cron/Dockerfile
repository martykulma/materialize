FROM postgres:18

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       postgresql-18-cron \
    && rm -rf /var/lib/apt/lists/*

RUN echo "shared_preload_libraries = 'pg_cron'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "cron.database_name = 'test'" >> /usr/share/postgresql/postgresql.conf.sample
