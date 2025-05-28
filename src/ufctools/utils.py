# not sure if this needs to be seperate, but keeping it for now.

import sys
import requests
from bs4 import BeautifulSoup


def make_soup(url: str) -> BeautifulSoup:
    source_code = requests.get(url, allow_redirects=False)
    plain_text = source_code.text.encode("ascii", "replace")
    return BeautifulSoup(plain_text, "html.parser")


def print_progress(
    iteration: int,
    total: int,
    prefix: str = "",
    suffix: str = "",
    decimals: int = 1,
    bar_length: int = 50,
) -> None:
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """
    percents = f"{100 * (iteration / float(total)):.2f}"
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = f'{"█" * filled_length}{"-" * (bar_length - filled_length)}'

    sys.stdout.write(f"\r{prefix} |{bar}| {percents}% {suffix}")

    if iteration == total:
        sys.stdout.write("\n")
    sys.stdout.flush()


# helper functions for adding prefixes/suffixes to dictionary labels.
# lots of stats can be scraped via same routine iterated over some html object
# but need different prefixes/suffixes (e.g. R or B for red or blue fighter, or R1-R5 for round)
def add_prefix_label(old_dict: dict[str, str], prefix: str) -> dict[str, str]:
    new_dict = {}

    for lbl, val in old_dict.items():
        new_lbl = f"{prefix}_{lbl}"
        new_dict[new_lbl] = val

    return new_dict


def add_suffix_label(old_dict: dict[str, str], suffix: str) -> dict[str, str]:
    new_dict = {}

    for lbl, val in old_dict.items():
        new_lbl = f"{lbl}_{suffix}"
        new_dict[new_lbl] = val

    return new_dict
