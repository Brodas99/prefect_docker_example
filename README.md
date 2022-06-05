# Project Overview

This project will introduce and illustrate how to fire up a Postgres database with Docker, create & register a script with Prefect & insert data into that Database hosted by our Docker Container. To complete the project, you will need to create your own Docker File for the Postgres Database, Prefect Docker Agent & then create a script that is registered to Prefect Cloud to write to your Postgres DB. 

# Requirements

* Install [Python3.9](https://www.python.org/downloads/)
* Install [Docker](https://www.docker.com/)
* Install [Prefect](https://www.prefect.io/)


For this project, you'll be working with the NHL API

- NHL API: `https://statsapi.web.nhl.com/api/v1`



## PART 1 - Docker Postgres Database

1. Create a Dockerfile 

    ```
    FROM postgres
    ENV POSTGRES_PASSWORD docker
    ENV POSTGRES_DB world
    COPY ./nhl.sql /docker-entrypoint-initdb.d/
    ```


2. Build postgres db image with 
    `docker build -t my-postgres-db ./`


3. Start your container with 
    `docker run -d --name my-postgresdb-container -p 5435:5432 my-postgres-db`

    For my own purposes, I'm mapping the database to Port 5435 - you can use 5432:5432 if you see fit

4. You can now connect to the database by using your Creds created in your Dockerfile - I personally use Dbeaver or TablePlus as my GUI 
    
    ```
    Creds:
    User - Postgres
    Password - docker
    host = LocalHost or Docker IP ad
    db name = world```


5. SIDENOTE: If you have another container with scripts/code & want to connect to the Database you just configured with Docker - 
    make sure to adjust the Host = docker ip address (mines 172.17.0.2)
    run * docker network inspect bridge* in your terminal to find ip


## PART 2 - Docker with Prefect Cloud

1. Build the Dockerfile image (in Prefect_docker folder) with `docker build . -t test:latest`

2. Register the flow with (make sure to run prefect backend cloud & login with your prefect_api_key) `python nhl_schedule.py` 

3. Start your agent with `prefect agent docker start -l your-label_name`

4. Run the flow with a `Quick Run` from the UI

5. Check Database GUI - Data should be inserted into your Database setup with Docker
