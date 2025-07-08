# functions that act on scraped data/scraper object to process data to be ML ready.
import pandas as pd

# from src.ufctools.scraping import FightDataScraper
from src.ufctools.filepaths_and_schema import (
    PROCESSED_FIGHTER_DATA_PATH,
    PROCESSED_FIGHT_DATA_PATH,
)


# process scraped fighter data
def process_fighter_data(
    fighter_df, save_local=True, save_dest=PROCESSED_FIGHTER_DATA_PATH
):
    # copy to not modify inplace -- leads to unexpected behaviour.
    raw_df = fighter_df.copy()
    # check for expected columns?

    # cols with percent strings
    pct_cols = ["STR_ACC", "STR_DEF", "TD_ACC", "TD_DEF"]

    raw_df["REACH"] = _parse_reach_col(raw_df["REACH"])
    raw_df["DOB"] = _parse_dob_col(raw_df["DOB"])
    raw_df["HEIGHT"] = _parse_height_col(raw_df["HEIGHT"])
    raw_df["WEIGHT"] = _parse_weight_col(raw_df["WEIGHT"])

    for col in pct_cols:
        raw_df[col] = _parse_pct_col(raw_df[col])

    if save_local:
        raw_df.to_csv(save_dest, sep=";")

    return raw_df


# convert height string to inches (int)
def _parse_height_str(height_str: str) -> int:

    # null handling -- empty string returns 0
    if height_str == "":
        return 0

    # heuristic:
    # kill the whitespace and the ""
    # split at the ' to get the integers
    height_str = height_str.replace(" ", "").replace('"', "")
    ft, inch = height_str.split("'")
    height = 12 * int(ft) + int(inch)
    return height


def _parse_height_col(col: pd.Series) -> pd.Series:
    col = col.str.replace("--", "")
    col = col.map(_parse_height_str).astype(int)

    return col


def _parse_weight_str(weight_str: str) -> int:
    if weight_str == "":
        return 0

    weight = weight_str.replace("LBS.", "").strip()

    return weight


def _parse_weight_col(col: pd.Series) -> pd.Series:
    col = col.str.replace("--", "")
    col = col.map(_parse_weight_str).astype(int)

    return col


def _parse_dob_col(col: pd.Series) -> pd.Series:
    col = col.str.replace("--", "")
    col = pd.to_datetime(col, format="%b %d, %Y")
    return col


def _parse_reach_col(col: pd.Series) -> pd.Series:
    col = col.str.replace("--", "0").str.replace('"', "")
    return col.astype(int)


def _parse_pct_col(col: pd.Series) -> pd.Series:
    col = col.str.replace("%", "").astype(float) / 100

    return col


############################################################################################
# parse/process fight data


def process_fight_data(
    fight_df, save_local=True, save_dest=PROCESSED_FIGHT_DATA_PATH
):
    # copy to not modify inplace -- leads to unexpected behaviour.
    raw_df = fight_df.copy()
    # check for expected columns?
    # time string to int
    raw_df["TIME"] = raw_df.map(_convert_timestr_to_sec)
    # some missing ref data (100/8200)-- you'd think this would be for old fights,
    # but is mostly in modern era

    raw_df['REFEREE'] = raw_df['REFEREE'].fillna('NO REF DATA')
    # details: not touching this right now
    # could extract submission details + ref scorecard eventually.

    if save_local:
        raw_df.to_csv(save_dest, sep=";")

    return raw_df


# convert "mm:ss" timestamp (for time of fight end) into elapsed seconds
def _convert_timestr_to_sec(time: str) -> int:
    min, sec = time.split(":")
    return 60 * int(min) + int(sec)
