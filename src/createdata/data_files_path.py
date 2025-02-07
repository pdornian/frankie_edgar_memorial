import os
from pathlib import Path

BASE_PATH = Path(os.getcwd()) / "data"
FIGHT_LINKS_PICKLE = BASE_PATH / "fight_links.pickle"
PAST_EVENT_LINKS_PICKLE = BASE_PATH / "past_event_links.pickle"
PAST_FIGHTER_LINKS_PICKLE = BASE_PATH / "past_fighter_links.pickle"
SCRAPED_FIGHTER_DATA_DICT_PICKLE = BASE_PATH / "scraped_fighter_data_dict.pickle"
NEW_FIGHTS_DATA_PATH = BASE_PATH / "new_fight_data.csv"
TOTAL_FIGHTS_DATA_PATH = BASE_PATH / "raw_total_fight_data.csv"
PREPROCESSED_DATA_PATH = BASE_PATH / "preprocessed_data.csv"
FIGHTER_DETAILS_PATH = BASE_PATH / "raw_fighter_details.csv"
UFC_DATA_PATH = BASE_PATH / "data.csv"
