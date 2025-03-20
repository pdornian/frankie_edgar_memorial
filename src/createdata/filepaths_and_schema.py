import os
from pathlib import Path

# from itertools import product

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
# _________
# FIGHT DETAILS columns as listed on UFC stats website
# e.g. http://ufcstats.com/fight-details/eaa885cf7ae31e0b
web_fight_cols = [
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

web_strike_cols = [
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
# _________

# cols of event data saved locally
event_cols = [
    "ID",
    "TITLE",
    "DATE",
    "LOCATION",
    "LINK",
    "FIGHT_LINKS_SCRAPED",
    "FIGHT_DATA_SCRAPED",
]

# column labels for processed fight data
# each fighter (R: red, B: blue)
# gets total stats (_TOT) suffix
# and round stats (_R#)
# everyone gets stats for 5 rounds to keep schema consistant
# doing this programatically to write less
# these will have dependencies (tots = sum of  rounds, percents, etc)
# that you might want to remove before shoving into an ML model
# but making columns for everything for granularity/readability

shared_cols = [
    "FIGHT_ID",
    "FIGHT_LINK",
    "TITLE_FIGHT",  # true/false
    "R_FIGHTER",
    "R_FIGHTER_ID",
    "L_FIGHTER",
    "L_FIGHTER_ID",
    "WINNER",
    "METHOD",
    "WIN_RND",
    "WIN_TIME",
    "FORMAT",  # 5 round or 3 round
    "DETAILS",  # could be judge scores or more details on finish, needs processing
    "REFEREE",
    "EVENT_ID",
    "EVENT_TITLE",
    "EVENT_DATE",
    "EVENT_LOC",
    "EVENT_BOUT_NUM",  # 1= headliner, 2= coheadliner, etc...]
]
# columns without percents
# aka stuff that doesn't need ATT, LND, and PCT suffixes
# these could have a better variable name
gen_stat_cols = ["KD", "SUB_ATT", "REV", "CTRL_TIME"]

# head/body/leg/distance/clinch/ground
# numbers are only for sig strikes --
# breakdowns not included for non-sig strikes
# omitting sig_str prefix for these for readability

pct_stat_cols = [
    "SIG_STR",
    "ALL_STR",
    "TD",
    "HEAD_STR",
    "BODY_STR",
    "LEG_STR",
    "DISTANCE_STR",
    "CLINCH_STR",
    "GROUND_STR",
]


def append_col_prefix_suffix(
    fighter_prefix=("B", "R"),
    pct_suffix=("LND", "ATT", "PCT"),
    rnd_suffix=("TOT", "R1", "R2", "R3", "R4", "R5"),
    shared_cols=shared_cols,
    gen_cols=gen_stat_cols,
    pct_cols=pct_stat_cols,
):
    gen_stat_cols = [
        f"{pre}_{stat}_{r_suf}"
        for pre in fighter_prefix
        for stat in gen_cols
        for r_suf in rnd_suffix
    ]
    pct_stat_cols = [
        f"{pre}_{stat}_{p_suf}_{r_suf}"
        for pre in fighter_prefix
        for stat in pct_cols
        for r_suf in rnd_suffix
        for p_suf in pct_suffix
    ]
    fight_cols = shared_cols + gen_stat_cols + pct_stat_cols
    return fight_cols


fight_cols = append_col_prefix_suffix()
