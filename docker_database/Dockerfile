FROM postgres::latest

ENV POSTGRES_PASSWORD docker
ENV POSTGRES_DB world

COPY ./nhl.sql /docker-entrypoint-initdb.d/
