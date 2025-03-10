import os
from pathlib import Path

BASE_PATH = Path(os.getcwd()) / "data"
FIGHT_LINKS_PICKLE = BASE_PATH / "fight_links.pickle"
PAST_EVENT_LINKS_PICKLE = BASE_PATH / "past_event_links.pickle"
PAST_FIGHTER_LINKS_PICKLE = BASE_PATH / "past_fighter_links.pickle"
SCRAPED_FIGHTER_DATA_DICT_PICKLE = BASE_PATH / "scraped_fighter_data_dict.pickle"

EVENT_DATA_PATH = BASE_PATH / "event_data.csv"
NEW_FIGHTS_DATA_PATH = BASE_PATH / "new_fight_data.csv"
TOTAL_FIGHTS_DATA_PATH = BASE_PATH / "raw_total_fight_data.csv"
PREPROCESSED_DATA_PATH = BASE_PATH / "preprocessed_data.csv"
FIGHTER_DETAILS_PATH = BASE_PATH / "raw_fighter_details.csv"
UFC_DATA_PATH = BASE_PATH / "data.csv"

# Parameters for data schema and column names go here.
# Not needed if inferring column names from headers

# some ufc table headers have wrong names in source code (TD is often labelled TD% internally)
# which busts pd.read_html because it sees two TD% columns.
# praying for consistent schema and using these labels instead trying
# to parse headers will be faster and simpler than writing parsing code
# ...if it works

# FIGHT DETAILS
# e.g. http://ufcstats.com/fight-details/eaa885cf7ae31e0b
fight_cols = [
    "FIGHTER",
    "KD",
    "SIG STR",
    "SIG STR%",
    "TOTAL STR",
    "TD",
    "TD%",
    "REV",
    "CTRL",
]

strike_cols = [
    "FIGHTER",
    "SIG STR",
    "SIG STR%",
    "HEAD",
    "BODY",
    "LEG",
    "DISTANCE",
    "CLINCH",
    "GROUND",
]

event_cols = [
    "ID",
    "TITLE",
    "DATE",
    "LOCATION",
    "LINK",
    "FIGHT_LINKS_SCRAPED",
    "FIGHT_DATA_SCRAPED"
]
