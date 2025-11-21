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
        print(f"Saving to {PROCESSED_FIGHTER_DATA_PATH}")
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

land_att_cols = [
    "ALL_STR",
    "SIG_STR",
    "HEAD",
    "BODY",
    "LEG",
    "DISTANCE",
    "CLINCH",
    "GROUND",
]

# generate list of all column names holding "LANDED of ATTEMPTED" data
# avoid any cols including PCT as name

# these are:

match_pattern = f".*({'|'.join(land_att_cols)})(?!_PCT).*"


def process_fight_data(
    fight_df,
    land_att_regex=match_pattern,
    save_local=True,
    save_dest=PROCESSED_FIGHT_DATA_PATH,
):
    # copy to not modify inplace -- leads to unexpected behaviour.
    df = fight_df.copy()
    # check for expected columns?
    # time string to int
    df["TIME"] = df["TIME"].map(_convert_timestr_to_sec)
    # some missing ref data (100/8200)-- you'd think this would be for old fights,
    # but is mostly in modern era

    df["REFEREE"] = df["REFEREE"].fillna("NO REF DATA")
    # details: not touching this right now
    # could extract submission details + ref scorecard eventually.

    # fill string NA's with empty string
    # this might fuck up any non-string column that got typed as an object, care
    df.loc[:, df.select_dtypes("object").columns] = df.select_dtypes("object").fillna(
        ""
    )
    # generate land_att col_names from regex match and df cols before feeding into
    # column processing
    land_att_col_names = df.filter(regex=land_att_regex).columns

    df = _proc_land_attempt_cols(df, land_att_col_names)
    df = _proc_ctrl_time(df)

    if save_local:
        df.to_csv(save_dest, sep=";")

    return df


# convert "mm:ss" timestamp (for time of fight end and control time) into elapsed seconds
def _convert_timestr_to_sec(time: str) -> int:
    min, sec = time.split(":")
    return 60 * int(min) + int(sec)


# taking fight_df, select ctrl columns,
# replace "" and "--" with "0:-1" (this then converts to -1,
# distinguishing non-control from rounds not reached)
# then convert to seconds
def _proc_ctrl_time(df: pd.DataFrame) -> pd.DataFrame:
    ctrl_df = df.filter(regex="CTRL", axis=1)
    ctrl_df = ctrl_df.replace(["--", ""], "0:-1")
    ctrl_df = ctrl_df.map(_convert_timestr_to_sec)
    df.loc[:, ctrl_df.columns] = ctrl_df

    return df


# given a dataframe and column name (list of col names?) that stores data strings of LANDED of ATTEMPTED form
# return dataframe with new columns col_name_landed and col_name_attempted as integers
# also, drop original col (making this toggleable for troubleshooting)

# mapping empty vals in land/attempt to -1 by default to distenguish them from genuine 0's


def _expand_land_attempt_col(col=pd.Series, empty_str_map=-1) -> pd.DataFrame:
    col_name_land = col.name + "_LANDED"
    col_name_att = col.name + "_ATTEMPTED"
    expanded_df = (
        col.str.split(" of ", expand=True)
        .rename(columns={0: col_name_land, 1: col_name_att})
        # awkward case handling from splitting empty string
        # returning ("", None)
        .replace("", -1)
        .fillna(-1)
        .astype({col_name_land: int, col_name_att: int})
    )
    return expanded_df


def _proc_land_attempt_cols(
    df: pd.DataFrame, col_list: list, drop_original_cols: bool = True, copy_data=True
) -> pd.DataFrame:

    expanded_cols = [_expand_land_attempt_col(df[col_name]) for col_name in col_list]
    df_list = [df] + expanded_cols

    # just doing concat because index order shouldn't have changed
    output_df = pd.concat(df_list, axis=1, copy=copy_data)

    if drop_original_cols:
        output_df = output_df.drop(columns=col_list)
    return output_df
