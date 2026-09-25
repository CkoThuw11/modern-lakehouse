FROM postgres:15.19

COPY configs/init.sql /docker-entrypoint-initdb.d/init.sql
COPY configs/postgresql.conf /etc/postgresql/postgresql.conf

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
