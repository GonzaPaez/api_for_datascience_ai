import pytest
from swcpy import SWCClient
from swcpy import SWCConfig
from swcpy.schemas import League, Team, Player, Performance
from io import BytesIO, StringIO
import pyarrow.parquet as pq
import pandas as pd
import csv
import os

os.environ["SWC_API_BASE_URL"] = "http://0.0.0.0:8000"
config = SWCConfig(backoff=False)
client = SWCClient(config)

def test_environment_variable():
    import os
    # Retrieve the environment variable
    swc_base_url = os.getenv("SWC_API_BASE_URL")
    # Print the value of the environment variable
    print(f"API_BASE_URL: {swc_base_url}")
    # Check if the environment variable is set correctly
    assert(swc_base_url == "http://0.0.0.0:8000")

def test_health_check():
    """Test health check from SDK"""
    response = client.get_health_check()
    assert response.status_code == 200
    assert response.json() == {"message": "API health check successful"}

def test_list_league():
    """Test get leagues from SDK"""  
    leagues_response = client.list_leagues()
    # Assert the endpoint returned a list object
    assert isinstance(leagues_response, list)
    # Assert each item in the list is an instance of a Pydantic League object
    for league in leagues_response:
        assert isinstance(league, League)
    # Assert that 5 League object are returned
    assert len(leagues_response) == 5

def test_list_leagues_no_backoff():
    """Tests get leagues from SDK without backoff"""
    config = SWCConfig()
    client = SWCClient(config)    
    leagues_response = client.list_leagues()
    # Assert the list is not empty
    assert isinstance(leagues_response, list)
    # Assert each item in the list is an instance of League
    for league in leagues_response:
        assert isinstance(league, League)
    assert len(leagues_response) == 5

def test_get_league_by_id():
    """Tests get leagues from SDK"""
    league_response = client.get_league_by_id(5002)
    assert isinstance(league_response, League)
    assert len(league_response.teams) == 8

def test_get_league_with_filter():
    """Tests get leagues from SDK"""
    leagues_response = client.list_leagues(league_name='Pigskin Prodigal Fantasy League')
    # Assert the list is not empty
    assert isinstance(leagues_response, list)
    # Assert each item in the list is an instance of League
    for league in leagues_response:
        assert isinstance(league, League)
    assert len(leagues_response) == 1

def test_list_teams():
    """Test get teams from SDK"""
    teams_response = client.list_teams()
    # Assert the endpoint returned a list object
    assert isinstance(teams_response, list)
    # Assert each item in the list an instance of a Pydantic Team object
    for team in teams_response:
        assert isinstance(team, Team)
    # Assert that 20 Team objects are returned
    assert len(teams_response) == 20

def test_list_players():
    """Tests get players from SDK"""
    players_response = client.list_players(0, 1500)
    # Assert the list is not empty
    assert isinstance(players_response, list)
    # Assert each item in the list is an instance of League
    for player in players_response:
        assert isinstance(player, Player)
    assert len(players_response) == 1018

def test_list_players_by_name():
    """Tests that the count of players in the database is what is expected"""
    players_response = client.list_players(first_name="Bryce", last_name="Young")
    # Assert the list is not empty
    assert isinstance(players_response, list)
    # Assert each item in the list is an instance of League
    for player in players_response:
        assert isinstance(player, Player)
    assert len(players_response) == 1
    assert players_response[0].player_id == 2009

def test_get_player_by_id():
    """Tests get player by ID from SDK"""
    player_response = client.get_player_by_id(2009)
    assert isinstance(player_response, Player)
    assert player_response.first_name == "Bryce" 

def test_list_performance():
    """Test get performance from SDK"""
    performance_response = client.list_performance(limit=20000)
    # Assert the endpoint returned a list object
    assert isinstance(performance_response, list)
    # Assert each item in the list an instance of a Pydantic Performance object
    for performance in performance_response:
        assert isinstance(performance, Performance)
    # Assert that 17306 Performance objects are returned
    assert len(performance_response) == 17306

def test_list_performance_by_date():
    """Test get performance by date from SDK"""
    performances_response = client.list_performance(
        limit=3000,
        minimum_last_changed_date="2024-04-01"
    )
    # Assert the list is not empty
    assert isinstance(performances_response, list)
    # Assert each item in the list is an instance of League
    for performance in performances_response:
        assert isinstance(performance, Performance)
    assert len(performances_response) == 2711

# Test bulks download
def test_bulk_player_file_parquet():
    """Tests bulk player download through SDK - Parquet"""
    config = SWCConfig(bulk_file_format = "parquet")
    client = SWCClient(config)    
    player_file_parquet = client.get_bulk_player_file()
    # Assert the file has the correct number of records (including header)
    player_table = pq.read_table(BytesIO(player_file_parquet))
    player_df = player_table.to_pandas()
    assert len(player_df) == 1018

def test_bulk_player_file():
    """Test bulk player download through SDK"""
    config = SWCConfig()
    client = SWCClient(config)
    player_file = client.get_bulk_player_file()
    # Decode the byte content to a string to test contents
    player_file_str = player_file.decode("utf-8")
    player_file_s = StringIO(player_file_str)
    csv_reader = csv.reader(player_file_s)
    # Assert the file has the correct number of records (including header)
    rows = list(csv_reader)
    assert len(rows) == 1019
    
    # Ensure the first row is the header
    assert rows[0] == [
        "player_id", "gsis_id", "first_name",
        "last_name", "position", "last_changed_date"
    ]

def test_bulk_league_file():
    """Test bulk league download through SDK"""
    config = SWCConfig()
    client = SWCClient(config)
    league_file = client.get_bulk_league_file()
    # Decode the byte content to a string to test contents
    league_file_str = league_file.decode("utf-8")
    league_file_s = StringIO(league_file_str)
    csv_reader = csv.reader(league_file_s)
    # Assert the file has the correct number of records (including header)
    rows = list(csv_reader)
    assert len(rows) == 6
    
    #Ensure the first row is the header
    assert rows[0] == [
        "league_id", "league_name",
        "scoring_type", "last_changed_date" 
    ]

def test_bulk_performance_file():
    """Test bulk performance download through SDK"""
    config = SWCConfig()
    client = SWCClient(config)
    performance_file = client.get_bulk_performance_file()
    # Decode the byte content to a string to test contents
    performance_file_str = performance_file.decode("utf-8")
    performance_file_s = StringIO(performance_file_str)
    csv_reader = csv.reader(performance_file_s)
    # Assert the file has the correct number of records (including header)
    rows = list(csv_reader)
    assert len(rows) == 17307
    
    # Ensure the first row is the header
    assert rows[0] == [
        'performance_id','week_number','fantasy_points','player_id','last_changed_date'
    ]

def test_bulk_teams_file():
    """Test bulk teams download throug SDK"""
    config = SWCConfig()
    client = SWCClient(config)
    team_file = client.get_bulk_team_file()
    # Decode the byte content to a string to test contents
    team_file_str = team_file.decode("utf-8")
    team_file_s = StringIO(team_file_str)
    csv_reader = csv.reader(team_file_s)
    rows = list(csv_reader)
    # Assert the file has the correct number of records + 1 record (the header)
    assert len(rows) == 21
    # Ensure the first row is the header
    assert rows[0] == [
        "team_id", "team_name", "league_id", "last_changed_date"
    ]

def test_bulk_team_player_file():
    """Test bulk team_player download through SDK"""
    config = SWCConfig()
    client = SWCClient(config)
    team_player_file = client.get_bulk_team_player_file()
    # Decode the byte content to a string to test contents
    team_player_file_str = team_player_file.decode("utf-8-sig")
    team_player_file_s = StringIO(team_player_file_str)
    csv_reader = csv.reader(team_player_file_s)
    # Assert the file has the correct number of records
    rows = list(csv_reader)
    assert len(rows) == 141
    
    # Additional check: ensure the first row is the header
    assert rows[0] == ["team_id", "player_id", "last_changed_date"]