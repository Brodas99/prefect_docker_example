

from helper_functions.functions import nhl_api, nhl_schedule_table_insert, db_host, db_name, db_password, db_port, db_user
import pandas as pd
from datetime import datetime, timedelta

import psycopg2
from psycopg2 import OperationalError

import prefect
from prefect import Flow, task
from prefect.schedules import IntervalSchedule
from prefect.run_configs import DockerRun
from prefect.storage import GitHub


@task
def get_schedule2122_url():
    return nhl_api().get_schedule2122_url()

@task
def transform_json(response_json):

    game_schedule =  []

    ## response_json['dates'] will provide multiple game days (if avaiable, could be length of 1)
    ## within each given date, there will be a number of games, ex) FLA v Lighting, Leafs vs Cancucks on 2021-10-14
    ##  given that there is 1, 2 or 3 games played on the same date, we need to loop through the number of games and grab each 
    for idx, game in enumerate(response_json['dates']):
        for num_games in range(response_json['dates'][idx]['totalGames']):
            
            try: 
                
                ## All-Star games will not have OT 
                if 'ot' in response_json['dates'][idx]['games'][num_games]['teams']['away']['leagueRecord'].keys():

                    data = {
                        
                        'home_team': response_json['dates'][idx]['games'][num_games]['teams']['home']['team']['name'], 
                        'home_wins': response_json['dates'][idx]['games'][num_games]['teams']['home']['leagueRecord']['wins'],
                        'home_losses': response_json['dates'][idx]['games'][num_games]['teams']['home']['leagueRecord']['losses'],
                        'home_ot': response_json['dates'][idx]['games'][num_games]['teams']['home']['leagueRecord']['ot'],
                        'away_team': response_json['dates'][idx]['games'][num_games]['teams']['away']['team']['name'],
                        'away_wins': response_json['dates'][idx]['games'][num_games]['teams']['away']['leagueRecord']['wins'],
                        'away_losses': response_json['dates'][idx]['games'][num_games]['teams']['away']['leagueRecord']['losses'],
                        'away_ot': response_json['dates'][idx]['games'][num_games]['teams']['away']['leagueRecord']['ot'],
                        'home_score': response_json['dates'][idx]['games'][num_games]['teams']['home']['score'],
                        'away_score': response_json['dates'][idx]['games'][num_games]['teams']['away']['score'],
                        'venue':response_json['dates'][idx]['games'][num_games]['venue']['name'],
                        'game_date':response_json['dates'][idx]['date'],
                        'season':'2021-2022',
                        'game_type': 'Regular Season',
                        'game_id': response_json['dates'][idx]['games'][num_games]['gamePk']

                    }
                
                    game_schedule.append(data)

                else: 
                    
                    print('no ot in key')
                    data = {
                        
                        'home_team': response_json['dates'][idx]['games'][num_games]['teams']['home']['team']['name'], 
                        'home_wins': response_json['dates'][idx]['games'][num_games]['teams']['home']['leagueRecord']['wins'],
                        'home_losses': response_json['dates'][idx]['games'][num_games]['teams']['home']['leagueRecord']['losses'],
                        'home_ot': -1,
                        'away_team': response_json['dates'][idx]['games'][num_games]['teams']['away']['team']['name'],
                        'away_wins': response_json['dates'][idx]['games'][num_games]['teams']['away']['leagueRecord']['wins'],
                        'away_losses': response_json['dates'][idx]['games'][num_games]['teams']['away']['leagueRecord']['losses'],
                        'away_ot': -1,
                        'home_score': response_json['dates'][idx]['games'][num_games]['teams']['home']['score'],
                        'away_score': response_json['dates'][idx]['games'][num_games]['teams']['away']['score'],
                        'venue':response_json['dates'][idx]['games'][num_games]['venue']['name'],
                        'game_date':response_json['dates'][idx]['date'],
                        'season':'2021-2022',
                        'game_type': 'All Star Weekend',
                        'game_id': response_json['dates'][idx]['games'][num_games]['gamePk']

                    }
                
                    game_schedule.append(data)
            
            except Exception as e:
                print(e)

    df = pd.DataFrame.from_dict(game_schedule)
    df['game_date'] = pd.to_datetime(df['game_date']).dt.date
    df['game_winner'] = df.apply(lambda x: x['home_team'] if x['home_score'] > x['away_score'] else x['away_team'], axis = 1)
    df['h_game_number'] = df['home_wins'] + df['home_losses'] + df['home_ot']
    df['a_game_number'] = df['away_wins'] + df['away_losses'] + df['away_ot']
    df['processed_date'] = datetime.now()

    logger = prefect.context.get("logger")
    logger.info(f"Test {df.shape}!")

    logger.info("Task has been completed.")
    return df


def execute_read_query(connection, query):
    connection.autocommit = None
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")

@task
def load_data(df, connection, cursor):

    # example to manage PostgreSQL transactions
    try:

        # Print PostgreSQL details
        print("PostgreSQL server information")
        print(connection.get_dsn_parameters(), "\n")


        # Executing a SQL query
        version_comment = "SELECT version();"
        record = execute_read_query(connection, version_comment)
        
        # # Fetch result
        # record = cursor.fetchone()
        print("You are connected to - ", record, "\n")

        # ______________________________________________________________________________________________

        for i, row in df.iterrows():

            logger = prefect.context.get("logger")
            logger.info(f"Test {list(row)}!")
            cursor.execute(nhl_schedule_table_insert, list(row))

        print("Transaction completed successfully ")
        connection.commit()


    # rollback if we have an issue
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error in transction Reverting all other operations of a transction ", error)
        connection.rollback()
        

    # closing database connection
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")



connection = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password, port = db_port)
cursor = connection.cursor()
connection.autocommit = False




## Prefect Flow Set Up
schedule=IntervalSchedule(interval=timedelta(hours=1))

with Flow(name = "docker_github_example", 
    storage=GitHub(repo="Brodas99/prefect_docker_example", path="prefect_docker/nhl_schedule.py", access_token_secret="your_github_token"), 
    schedule=schedule
) as flow:
    
    response_json = get_schedule2122_url()
    df = transform_json(response_json)
    load_data(df, connection, cursor)

flow.run_config = DockerRun(image="test:latest")

flow.register(
    project_name = "Prefect_tutorial",
    labels = ["docker_test"]
)

