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
]
