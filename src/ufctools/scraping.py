from io import StringIO
import os
import pickle
import re
from typing import Dict, List, Iterable

from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

# hardcoded headers/filepaths
from src.ufctools.filepaths_and_schema import (
    FIGHT_LINKS_PICKLE,
    NEW_FIGHTS_DATA_PATH,
    FIGHT_DATA_PATH,
    EVENT_DATA_PATH,
    web_fight_cols,
    web_strike_cols,
    event_cols,
)

from src.ufctools.utils import (
    make_soup,
    print_progress,
    add_prefix_label,
    add_suffix_label,
)


# should refactor these to not just be giant classes/move static methods out
class UFCLinks:
    def __init__(
        self, all_events_url="http://ufcstats.com/statistics/events/completed?page=all"
    ):
        self.all_events_url = all_events_url
        self.EVENT_DATA_PATH = EVENT_DATA_PATH
        self.EVENT_DATA = None
        self.FIGHT_LINKS_PICKLE_PATH = FIGHT_LINKS_PICKLE
        self.FIGHT_LINKS = None
        # pulls web data and compares to local data to prep class instance
        self._initiate_class()

    def get_fight_links(self, force_refresh=False):

        # if force_refresh is True, retrieves all fight links from events regardless
        # of FIGHT_LINKS_SCRAPED value (refresh also forced if fight link file doesnt exist)
        # otherwise, only scrapes links where FIGHT_LINKS_SCRAPED == False
        if force_refresh or not self.FIGHT_LINKS_PICKLE_PATH.exists():
            print(f"Scraping all fight links to {self.FIGHT_LINKS_PICKLE_PATH}")
            fight_link_dict = self._initiate_fight_links()
        else:
            print("Checking for new events to scrape")
            new_fight_links = self._get_unscraped_fight_links()
            fight_link_dict = self.FIGHT_LINKS.copy()
            fight_link_dict.update(new_fight_links)

        self.FIGHT_LINKS = fight_link_dict
        self._update_event_fight_link_scraped_status()
        self._write_fight_links()

        return fight_link_dict

    def _initiate_class(self):
        # get latest event data from web
        print(f"Pulling event data from {self.all_events_url}")
        web_event_df = self._scrape_all_events()
        web_event_ids = web_event_df.index

        if not self.EVENT_DATA_PATH.exists():
            # if no event data file, initate event data by writing this to csv
            # with no comparisons

            print(
                f"No existing event data, writing web data locally to {self.EVENT_DATA_PATH}"
            )
            self._write_event_data(web_event_df)
            # label for return data
            event_df = web_event_df
        else:
            # otherwise, event data file already exists.
            # compare with all_event_df by id and only write rows
            # that aren't present in existing file
            print(f"Reading local event data from {self.EVENT_DATA_PATH}")
            local_event_df = pd.read_csv(
                self.EVENT_DATA_PATH, sep=";", parse_dates=["DATE"], index_col="ID"
            )

            local_event_ids = local_event_df.index
            new_event_ids = web_event_ids.difference(local_event_ids)

            # return local data unless new events present in web data.

            if not new_event_ids.empty:
                # append  new events to beginning of DF and overwrite file
                # we could make it only write the new rows, but this file is small enough that i don't care
                # and sorting semantics are easier like this.
                print(f"{len(new_event_ids)} new event/s. Updating local event data.")
                # return  web_event_df, new_event_ids, local_event_df
                updated_df = pd.concat(
                    [web_event_df.loc[new_event_ids], local_event_df]
                )
                self._write_event_data(updated_df)
                # return updated event df if new events present in web
                event_df = updated_df
            else:
                # otherwise, no new events, local event data still valid.
                print("No new events, local data up to date")
                event_df = local_event_df

        # set event data property
        self.EVENT_DATA = event_df

        # load fight links if they already exist.
        if self.FIGHT_LINKS_PICKLE_PATH.exists():
            print(f"Loading local fight links from {self.FIGHT_LINKS_PICKLE_PATH}")
            # load prev events and links
            with open(self.FIGHT_LINKS_PICKLE_PATH, "rb") as event_fight_dict:
                prev_fight_links = pickle.load(event_fight_dict)
                self.FIGHT_LINKS = prev_fight_links

        return event_df

    def _scrape_all_events(self) -> pd.DataFrame:
        # reads all events from all_events_url column and
        # initiates event data table as dataframe.
        event_text = ";".join(event_cols)
        soup = make_soup(self.all_events_url)
        for row in soup.tbody.findAll("tr", {"class": "b-statistics__table-row"}):

            # case handling for blank row that exists at top of table.
            # text is just empty string/newline chars
            if row.text.strip() == "":
                continue

            link_elt = row.find("a")
            event_title = link_elt.text.strip().upper()
            event_link = link_elt.get("href")
            event_id = event_link.split("/")[-1]

            event_date = (
                row.find("span", {"class": "b-statistics__date"}).text.strip().upper()
            )

            # taking for granted that event location is last td element in row.
            event_location = row.findAll("td")[-1].text.strip().upper()

            event_text += "\n" + ";".join(
                [
                    event_id,
                    event_title,
                    event_date,
                    event_location,
                    event_link,
                    "False",
                    "False",
                ]
            )

        # pass through stringIO so this csv like text string can be plugged into pandas read_csv
        event_data = StringIO(event_text)
        event_df = pd.read_csv(event_data, sep=";")
        # reformat datetimes
        event_df["DATE"] = pd.to_datetime(event_df["DATE"], format="%B %d, %Y")
        # change ID to index
        event_df = event_df.set_index("ID")

        return event_df

    def _write_event_data(self, df):
        filepath = self.EVENT_DATA_PATH
        df.to_csv(filepath, sep=";")

        return df

    # given list of event links, gets all links to fights for that event and
    # stores in dictionary using event link as key
    def _make_link_dict(self, event_links: list[str]) -> dict[str, str]:

        num_events = len(event_links)
        event_fight_dict = {}
        print(f"Scraping fight links from {num_events} events: ")
        print_progress(0, num_events, prefix="Progress:", suffix="Complete")
        for index, link in enumerate(event_links):
            event_fights = []
            soup = make_soup(link)
            for row in soup.findAll(
                "tr",
                {
                    "class": "b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click"
                },
            ):
                href = row.get("data-link")
                event_fights.append(href)

            event_fight_dict[link] = event_fights

            print_progress(index + 1, num_events, prefix="Progress:", suffix="Complete")

        return event_fight_dict

    def _write_fight_links(self):
        # might not need this as a subfunction but i don't want to write it twice
        with open(self.FIGHT_LINKS_PICKLE_PATH, "wb") as f:
            pickle.dump(self.FIGHT_LINKS, f)

    def _initiate_fight_links(self):
        # to initiate, make dict from all event data links
        event_df = self.EVENT_DATA
        event_fight_link_dict = self._make_link_dict(event_df["LINK"])
        return event_fight_link_dict

    def _get_unscraped_fight_links(self):
        event_df = self.EVENT_DATA
        links_to_scrape = event_df[~event_df["FIGHT_LINKS_SCRAPED"]]["LINK"]
        if links_to_scrape.empty:
            print("No new event links to scrape.")
            new_fight_links = {}
        else:
            new_fight_links = self._make_link_dict(links_to_scrape)
        return new_fight_links

    def _update_event_fight_link_scraped_status(self):
        # get ids from event-fight dict keys.
        # assuming that if event is in there, fight links have been scraped.
        # it's not really airtight logic, but good enough for now
        scraped_ids = [id.split("/")[-1] for id in self.FIGHT_LINKS.keys()]
        event_df = self.EVENT_DATA
        event_df.loc[scraped_ids, "FIGHT_LINKS_SCRAPED"] = True
        self.EVENT_DATA = event_df
        self._write_event_data(event_df)
        return event_df


class FightDataScraper:
    def __init__(self):
        self.NEW_FIGHTS_DATA_PATH = NEW_FIGHTS_DATA_PATH
        self.FIGHT_DATA_PATH = FIGHT_DATA_PATH
        # when fight scraper initiated, update/load event links.
        self.events = UFCLinks()
        self.events.get_fight_links()
        #
        self.fight_data = self._load_local_fight_data()

    def _load_local_fight_data(self) -> None:
        if self.FIGHT_DATA_PATH.exists():
            print(f"Reading local fight data from {self.FIGHT_DATA_PATH}")
            local_fight_df = pd.read_csv(
                self.FIGHT_DATA_PATH,
                sep=";",
                parse_dates=["DATE"],
                index_col="FIGHT_ID",
            )
            return local_fight_df
        else:
            return None

    # master function for scraping all missing fight data

    def scrape_new_fights(self, force_refresh=False, itercap=1000) -> pd.DataFrame:

        events_df = self.events.EVENT_DATA

        if force_refresh:
            # reset data scraped status
            events_df["FIGHT_DATA_SCRAPED"] = False
            self.events._write_event_data(events_df)
            # delete fight data
            if self.FIGHT_DATA_PATH.exists():
                os.remove(self.FIGHT_DATA_PATH)

        # get links to all events with FIGHT_DATA_SCRAPED == FALSE
        unscraped_events = events_df[~events_df["FIGHT_DATA_SCRAPED"]]

        # multiprocessing goes here???
        # should this save as we go?

        new_fight_data = []
        print(f"Scraping fights from {unscraped_events.shape[0]} event/s.")

        x = 0
        for event_row in tqdm(unscraped_events.itertuples()):
            if x >= itercap:
                break
            # print(event_row)
            event_fights_df = self.scrape_event_fights(event_row.LINK)
            # update fight scraped status in events data
            # have to access by 'Index' --- seems to throwaway label of 'ID'
            events_df.at[event_row.Index, "FIGHT_DATA_SCRAPED"] = True

            new_fight_data.append(event_fights_df)
            x += 1

        # update local event saved data file
        self.events._write_event_data(events_df)

        new_fights_df = pd.concat(new_fight_data)
        new_fights_df.to_csv(self.NEW_FIGHTS_DATA_PATH)

        return new_fights_df

    # given an event link, scrape all fights to dataframe
    def scrape_event_fights(self, event_link: str) -> pd.DataFrame:
        event_fight_data = []
        event_fight_links = self.events.FIGHT_LINKS[event_link]
        for fight_link in event_fight_links:
            try:
                event_fight_data.append(self.get_fight_stats(fight_link))
            except Exception as e:
                print(f"error processing {fight_link}: {e}")

        event_fights_df = pd.DataFrame.from_records(event_fight_data, index="FIGHT_ID")
        return event_fights_df

    def get_fight_stats(self, fight_link: str) -> dict:

        fight_soup = make_soup(fight_link)

        # - 4 things to grab

        # - fighter details (name, result, fighter_link (or ID))

        # - fight attributes (winner, method, etc -- everything before the tables)

        # - general stats (kd, td, sub att, rev,  ctrl) - these are deceptively labeled TOTALS

        # (strike stats also have totals so i think totals is a bad name -- we're going to call them 'other stats')

        # - sig strike data.

        ##################

        # initiate fight_stats dict with LINK and ID.

        fight_id = fight_link.split("/")[-1]
        fight_stats = {"FIGHT_ID": fight_id, "FIGHT_LINK": fight_link}

        fight_fighters = self._get_fighters(fight_soup)

        fight_attr = self._get_fight_attr(fight_soup)
        ###########

        fight_other_stats = self._get_fight_table_stats(
            fight_soup,
            header_lbls=web_fight_cols,
            omit_lbls=["FIGHTER", "SIG_STR", "SIG_STR_PCT"],
            table_index=0,
        )

        fight_strike_stats = self._get_fight_table_stats(
            fight_soup,
            header_lbls=web_strike_cols,
            omit_lbls=["FIGHTER"],
            table_index=2,
        )

        fight_stats.update(fight_fighters)

        fight_stats.update(fight_attr)

        fight_stats.update(fight_other_stats)

        fight_stats.update(fight_strike_stats)

        return fight_stats

    #############
    # parsing fighter names/results here

    # this could be static method. this class could be a module. problems for later.
    # given single "b-fight-details__person" element, get name, link and result.
    def _get_fighter(self, fighter_raw: BeautifulSoup) -> dict:
        name = fighter_raw.a.text.strip().upper()
        link = fighter_raw.a.get("href")
        id = link.split("/")[-1]
        result = fighter_raw.i.text.strip().upper()

        fighter = {
            "FIGHTER": name,
            "FIGHTER_ID": id,
            "FIGHTER_LINK": link,
            "FIGHTER_RESULT": result,
        }

        return fighter

    # given soup of fight link, parse fighter data to dict
    def _get_fighters(self, fight_soup: BeautifulSoup) -> dict:
        fighters = {}

        r_raw, b_raw = fight_soup.find_all("div", {"class": "b-fight-details__person"})

        r = self._get_fighter(r_raw)
        b = self._get_fighter(b_raw)

        fighters = add_prefix_label(r, "R") | add_prefix_label(b, "B")

        return fighters

    #####

    ########
    # scraping fight attributes (everything in the the non-tabular box) and all associated routines HERE

    # fight name might say HEAVYWEIGHT BOUT, or UFC TITLE HEAVYWEIGHT BOUT
    # helper function picks out the word with 'weight' in it
    # expects string to already be stripped/capitalized/seperated by spaces
    @staticmethod
    def _parse_weightclass(fight_name):
        for word in fight_name.split(" "):
            if "WEIGHT" in word:
                return word
            else:
                continue
        # no word with "weight" in fight name
        return "WEIGHTCLASS PARSING ERROR"

    # couple of cases here because of changes in UFC methodology that i'm merging together.
    # current UFC awards FOTN and performance bonuses for best finishes
    # used to award KOTN and SOTN specficially and icons indicating these are still
    # in data. i'm gonna record them all as equivalent performance bonuses
    # because i don't feel like distinguishing between them
    # all of these are are just marked by embedded images so this check is super hardcoded
    # based on the name of the embedded images
    # static
    @staticmethod
    def _is_perf_bonus(attr_soup: BeautifulSoup) -> bool:
        for img in attr_soup.i.find_all("img"):
            src = img.get("src")
            if (
                ("ko.png" in src)
                or ("perf.png" in src)
                or ("fight.png" in src)
                or ("sub.png" in src)
            ):
                return True
        # otherwise false
        return False

    # static
    # this function might be useful other places, might generalize
    @staticmethod
    def _parse_attr(p_soup: BeautifulSoup) -> dict:
        # each top level i tag in this p block is one attr
        attr_dict = {}
        for i_raw in p_soup.findAll("i", recursive=False):
            # smash (like khamzat) together, then split at :
            attr = " ".join(i_raw.stripped_strings).upper().split(": ")
            attr_lbl = attr[0]

            # case to handle blank attributes
            # inspired by blank referee on this page that exploded this
            # http://ufcstats.com/fight-details/6fa2dae90bda4742

            attr_txt = "" if len(attr) == 1 else attr[1]

            attr_dict[attr_lbl] = attr_txt

        return attr_dict

    # given "b-fight-details__fight" html soup,
    # parses content ("b-fight-details__content") (method, round, etc)
    def _get_attr_content(self, attr_soup: BeautifulSoup) -> Dict:
        # logic for parsing attributes out of the two p blocks
        # is annoying because of inconsistency in i tag usage.

        # ASSUMING THAT THE FOLLOWING CALL ONLY FINDS TWO P TAGS (one with method/round/etc. and second with details:)
        # each has its own parsing
        p_attr, p_details = attr_soup.find(
            "div", {"class": "b-fight-details__content"}
        ).findAll("p")
        attr_dict = self._parse_attr(p_attr)

        # details value is special case
        attr_dict["DETAILS"] = (
            " ".join(p_details.stripped_strings).upper().split(": ")[-1]
        )

        return attr_dict

    def _get_fight_attr(self, fight_soup: BeautifulSoup) -> Dict:
        attr_raw = fight_soup.find("div", {"class": "b-fight-details__fight"})
        fight_name = attr_raw.i.text.strip().upper()
        weight = self._parse_weightclass(fight_name)

        # detecting title fights by the word "TITLE" in fight name
        # could also do this by looking for belt icon/css tag
        title_fight = "TITLE" in fight_name
        perf_bonus = self._is_perf_bonus(attr_raw)

        # initialize attr_dict with attr content then manually add
        # weight class, title fight and performance bonus flags
        attr_dict = self._get_attr_content(attr_raw)
        attr_dict["WEIGHT_CLASS"] = weight
        attr_dict["TITLE_FIGHT"] = title_fight
        attr_dict["PERF_BONUS"] = perf_bonus

        return attr_dict

    ###########################

    # helper function: each <tr> element of fight table contains two rows semantically.
    # data for each fighter is stacked on top of each other within the same cell
    # unsure if this was done intentionally to make it harder to scrape or if it's just
    # dubious formatting. but we need to unpack it.

    @staticmethod
    def _unpack_table_cell(td_soup: BeautifulSoup, lbl: str) -> Dict:

        # this really depends on exactly two elements
        # containing strings being present in the cell
        # and may explode otherwise
        r_stat, b_stat = td_soup.stripped_strings

        cell_dict = {f"R_{lbl}": r_stat, f"B_{lbl}": b_stat}

        return cell_dict

    def _unpack_table_row(
        self, tr_soup: BeautifulSoup, header_lbls: List, omit_lbls: Iterable = ()
    ) -> Dict:
        """
            Unpacks each cell of table row to dictionary with adjusted labels for each fighter
            e.g.

            KD
            1
            3

            becomes
            {"R_KD": 1, "B_KD": 3}


        Args:
            row_soup (BeautifulSoup): <tr> object
            header_lbls (List): labels for headers in web table
            omit_lbls (List): labels for cells/cols to drop (sometimes we want the whole row, sometimes we don't)

        Returns:
            Dict: Dictionary with label keys to stat.
        """

        # add corner prefixes to omit lbls
        lbls_to_drop = [f"R_{lbl}" for lbl in omit_lbls] + [
            f"B_{lbl}" for lbl in omit_lbls
        ]

        row_dict = {}
        # unpack cells with ordered header lbls
        for td_soup, lbl in zip(tr_soup.findAll("td"), header_lbls):

            row_dict.update(self._unpack_table_cell(td_soup, lbl))

        # drop any lbls specified

        for lbl in lbls_to_drop:
            del row_dict[lbl]

        return row_dict

    def _parse_round_table(
        self,
        round_tbody_soup: BeautifulSoup,
        header_lbls: List,
        omit_lbls: Iterable = (),
    ) -> Dict:
        # heuristic here is given the tbody of a round by round stat table:
        # - look for the text elements with the word "Round" in them
        # - parse the first tr element after each such text

        all_round_stats = {}
        round_text_elts = round_tbody_soup.findAll(string=re.compile("Round"))

        # counter for round number
        round_num = 0

        for elt in round_text_elts:
            round_num += 1
            round_stats_raw = elt.find_next("tr")
            round_stats_dict = self._unpack_table_row(
                round_stats_raw, header_lbls, omit_lbls
            )
            # add round number to labels
            round_stats_dict = add_suffix_label(round_stats_dict, f"R{round_num}")
            all_round_stats.update(round_stats_dict)

        return all_round_stats

    # smashing the rows of all fights together should induce all round labels
    # for all fights.
    # but too lazy.
    # using default arg to hardcode headers. could add header parsing logic.
    # table-index: what table to look for. set to 0 for other stats, 2 for strike stats
    # (parses initial index as total stats, then expects round by round table next)

    def _get_fight_table_stats(
        self,
        fight_soup: BeautifulSoup,
        header_lbls: list = web_fight_cols,
        omit_lbls: Iterable = (
            "FIGHTER",
            "SIG_STR",
            "SIG_STR_PCT",
        ),  # omit these by default, redundant
        table_index: int = 0,
    ) -> Dict:

        tables = fight_soup.findAll("tbody")

        tot_soup = tables[table_index]

        # initiate dict with total stats

        stats_dict = self._unpack_table_row(
            tot_soup.find("tr"), header_lbls=header_lbls, omit_lbls=omit_lbls
        )

        stats_dict = add_suffix_label(stats_dict, "TOT")

        # get per round stats (second table/tbody)

        round_soup = tables[table_index + 1]
        round_stats_dict = self._parse_round_table(
            round_soup, header_lbls, omit_lbls=omit_lbls
        )
        stats_dict.update(round_stats_dict)

        return stats_dict
