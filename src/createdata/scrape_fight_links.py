from io import StringIO

import pandas as pd
import pickle

from src.createdata.utils import make_soup, print_progress

from src.createdata.filepaths_and_schema import (  # isort:skip
    FIGHT_LINKS_PICKLE,
    EVENT_DATA_PATH,
    event_cols,
)


class UFCLinks:
    def __init__(
        self, all_events_url="http://ufcstats.com/statistics/events/completed?page=all"
    ):
        self.all_events_url = all_events_url
        self.EVENT_DATA_PATH = EVENT_DATA_PATH
        self.EVENT_DATA = None
        self.FIGHT_LINKS_PICKLE_PATH = FIGHT_LINKS_PICKLE
        self.FIGHT_LINKS = None
        self._initiate_class()

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
