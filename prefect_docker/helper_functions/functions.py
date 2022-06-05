import requests
import pandas as pd
import pandas_redshift as pr
from dataclasses import dataclass, field


@dataclass
class to_csv:

    df:dict = field(init = False)
    path_location:str = field(init = False)

    def df_to_csv(self, path_location, df):

        df_list = list(df)
        [df[name].to_csv(path_location + f"{name}.csv") for name in df_list]


@dataclass
class read_text:

    filepath: str = field(init = False)
    # open text file & read
    def read_text_file(self, filepath):

        filestring = filepath 
        with open(filestring, 'r') as file:
            file_content = file.read()

        return file_content


@dataclass 
class nhl_api:

    base_url: str = "https://statsapi.web.nhl.com/api/v1"
    team_id: str  = field(init = False)
    lot_dict : dict = field(default_factory = dict)


    def __post_init__(self):

        ## url's 

        ## Returns a list of data about all teams including their id, venue details, division, conference and franchise information.
        ## https://statsapi.web.nhl.com/api/v1/teams/ID Returns the same information as above just for a single team instead of the entire league.
        self.teams_url = f"{self.base_url}/teams"

        ## Returns a list of stat data about all teams including their games played, wins, losses, ot etc 
        ## https://statsapi.web.nhl.com/api/v1/teams/ID/stats Returns the same information as above just for a single team instead of the entire league.
        self.team_stats_url = f"{self.base_url}/teams"

        ## Returns a list of data about all teams rosters including their name, id, jersey number, team name & link to people url (stats).
        self.roster_url = f"{self.teams_url}"

        ## Returns a list of data about the schedule for a specified date range. If no date range is specified, returns results from the current day
        self.schedule_url = f"{self.base_url}/schedule"
        
        ## Returns a list of data about the schedule for a specified date range - this case, season 21-22
        self.schedule2122_url = f"{self.base_url}/schedule?startDate=2021-10-12&endDate=2022-05-01"
        
        ## Returns a list of data about the schedule for a specified date range - this case, play offs for season 21-22
        self.schedule_playoffs2122 = f"{self.base_url}/schedule?startDate=2022-05-02&endDate=2022-05-15"
        
        ## Returns a list of data about the schedule for a specified date range - this case, only the games for Florida Panthers (id) season 21-22
        self.florida_panthers2122_schedule = f"{self.base_url}/schedule?teamId=13&startDate=2021-10-12&endDate=2022-05-01"


    ## GET https://statsapi.web.nhl.com/api/v1/teams
    def get_teams(self):
        return (requests.get(url = self.teams_url)).json()

    ## GET https://statsapi.web.nhl.com/api/v1/teams/team_id/stats
    def get_team_stats(self, team_id):
        team_stats_url = f"{self.team_stats_url}/{team_id}/stats"
        return (requests.get(url = team_stats_url)).json()
    
    ## GET https://statsapi.web.nhl.com/api/v1/schedule
    def get_schedule2122_url(self):
        return (requests.get(url = self.schedule2122_url)).json()

    ## GET https://statsapi.web.nhl.com/api/v1/schedule
    def get_playoff_schedule_url(self):
        return (requests.get(url = self.schedule_playoffs2122)).json()

    ## GET https://statsapi.web.nhl.com/api/v1/teamID/roster/season=
    def nhl_roster_url(self, team_id):
        roster_url = f"{self.teams_url}/{team_id}/roster/season=20212022"
        return (requests.get(url = roster_url)).json()

    ## GET https://statsapi.web.nhl.com/api/v1/people/ID/stats?stats=statsSingleSeason&season=20212022
    def stats_url(self, player_id):
        stats_url = f"{self.base_url}/people/{player_id}/stats?stats=statsSingleSeason&season=20212022"
        return (requests.get(url = stats_url)).json()

    ## GET "https://statsapi.web.nhl.com/api/v1/schedule?teamId=13&startDate=2021-10-12&endDate=2022-05-01"
    def get_fla_schedule_url(self):
        return (requests.get(url = self.florida_panthers2122_schedule)).json()

    ## GET https://statsapi.web.nhl.com/api/v1/game/game_id/feed/live 
    def get_fla_live_data(self, game_id):
        live_data = f"{self.base_url}/game/{game_id}/feed/live"
        return (requests.get(url = live_data)).json()


def fill_na_by_type(df : pd.DataFrame()):

    for col in df:
        dt = df[col].dtype 
        if dt == int or dt == float:
            df[col] = df[col].fillna(0)
        else:
            df[col] = df[col].fillna("Other")

    return df 


def send_email(subject, body, receiver_email, cc_list):

    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    cc_list_string = ", ".join(cc_list)

    sender_email = ""
    password = ""

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = subject
    message['Cc'] = cc_list_string

    # Add body to email
    message.attach(MIMEText(body, "html"))
    text = message.as_string()

    toaddrs = [receiver_email] + cc_list
    
    with smtplib.SMTP("smtp-mail.outlook.com", 587) as server:
        server.starttls()
        server.login(sender_email, password)
        server.sendmail(sender_email, toaddrs, text)
        server.quit()
    
    return 0



def send_email_report(email_subject_script,final_time):


    email_receiver = ""
    email_subject = f"NHL_API_{email_subject_script}: "
    cc_list = [""]
    body = f"Hey Bryan, <br>  Final Time: {final_time}"

    send_email(email_subject, body, email_receiver, cc_list)


nhl__team_table_insert = ("""
    INSERT INTO nhl_teams 
    (team_name, short_name, team_abbrev, team_id, conference, division, city, venue, franchise_id, nhl_team_link, nhl_franchise_link, first_year)
    VALUES (%s,%s,%s,%s,%s,%s, %s, %s,%s,%s,%s,%s)
    ON CONFLICT DO NOTHING
    """)


nhl_schedule_table_insert = ("""
    INSERT INTO nhl_schedule
    (home_team, home_wins, home_losses, home_ot, away_team, away_wins, away_losses, away_ot, home_score, away_score, venue, game_date, season, game_type, game_id, game_winner, h_game_number, a_game_number, processed_date)
    VALUES (%s,%s,%s,%s,%s,%s, %s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s, %s)
    ON CONFLICT DO NOTHING
    """)


#db_host = '127.0.0.1'
db_host =  '172.17.0.2'
db_port  = 5432
db_user = 'postgres'
db_password = 'docker'
db_name = 'world'