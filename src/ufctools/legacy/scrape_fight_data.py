import os
import concurrent.futures
from typing import Dict, List

import pandas as pd
from bs4 import BeautifulSoup

from src.ufctools.legacy.scrape_fight_links import UFCLinks
from src.ufctools.utils import make_soup, print_progress

from src.ufctools.filepaths_and_schema import (  # isort:skip
    NEW_FIGHTS_DATA_PATH,
    TOTAL_FIGHTS_DATA_PATH,
)


class FightDataScraper:
    def __init__(self):
        self.HEADER: str = (
            "R_fighter;B_fighter;R_KD;B_KD;R_SIG_STR.;B_SIG_STR.\
;R_SIG_STR_pct;B_SIG_STR_pct;R_TOTAL_STR.;B_TOTAL_STR.;R_TD;B_TD;R_TD_pct\
;B_TD_pct;R_SUB_ATT;B_SUB_ATT;R_REV;B_REV;R_CTRL;B_CTRL;R_HEAD;B_HEAD;R_BODY\
;B_BODY;R_LEG;B_LEG;R_DISTANCE;B_DISTANCE;R_CLINCH;B_CLINCH;R_GROUND;B_GROUND\
;win_by;last_round;last_round_time;Format;Referee;date;location;Fight_type;Winner\n"
        )

        self.NEW_FIGHTS_DATA_PATH = NEW_FIGHTS_DATA_PATH
        self.TOTAL_FIGHTS_DATA_PATH = TOTAL_FIGHTS_DATA_PATH

    def create_fight_data_csv(self) -> None:
        print("Scraping links!")

        ufc_links = UFCLinks()
        new_fight_links, all_fight_links = ufc_links.get_fight_links()
        print("Successfully scraped and saved fight links!\n")
        print("Now, scraping fight data!\n")

        # are there new fight links to scrap data from?
        if not new_fight_links:
            # if there's no new fight links
            if self.TOTAL_FIGHTS_DATA_PATH.exists():
                # if fight data csv file exists.

                # assume fight data up to date
                # this is not actually necessarily true
                # but good enough for now
                print(
                    f"""No new fight data to scrape.
                        {self.TOTAL_EVENT_AND_FIGHTS_PATH} up to date."""
                )
                return None
            else:
                # if no data csv, scrape all fights and make it.
                self._scrape_raw_fight_data(
                    all_fight_links,
                    filepath=self.TOTAL_FIGHTS_PATH,
                )
        else:
            # scrape only fights from new events
            self._scrape_raw_fight_data(
                new_fight_links, filepath=self.NEW_EVENT_AND_FIGHTS_PATH
            )

            new_fights_data = pd.read_csv(self.NEW_FIGHTS_PATH)
            old_fights_data = pd.read_csv(self.TOTAL_FIGHTS_PATH)

            # verify same column count
            assert len(new_fights_data.columns) == len(old_fights_data.columns)

            # restricts new event cols to those with labels of old events/ensures same col order
            # feels like merging new/old fight data should be a seperate method
            new_fights_data = new_fights_data[list(old_fights_data.columns)]

            # might be worth verifying integrity here
            latest_total_fight_data = pd.concat(
                [new_fights_data, old_fights_data],
                axis=1,
                ignore_index=True,
            )

            latest_total_fight_data.to_csv(self.TOTAL_FIGHTS_DATA_PATH, index=None)
            print(f"Updated {self.TOTAL_FIGHTS_DATA_PATH} with new fight data")
            os.remove(self.NEW_EVENT_AND_FIGHTS_DATA_PATH)
            print("Removed temporary files.")

        print("Successfully scraped and saved UFC fight data!")

    def _scrape_raw_fight_data(
        self, event_and_fight_links: Dict[str, List[str]], filepath
    ):
        if filepath.exists():
            print(f"File {filepath} already exists, overwriting.")

        total_stats = self._get_total_fight_stats(event_and_fight_links)
        with open(filepath.as_posix(), "wb") as file:
            file.write(bytes(self.HEADER, encoding="ascii", errors="ignore"))
            file.write(bytes(total_stats, encoding="ascii", errors="ignore"))

    def _get_fight_stats_task(self, fight, event_info):
        total_fight_stats = ""
        try:
            fight_soup = make_soup(fight)
            fight_stats = self._get_fight_stats(fight_soup)
            fight_details = self._get_fight_details(fight_soup)
            result_data = self._get_fight_result_data(fight_soup)
            total_fight_stats = (
                fight_stats + ";" + fight_details + ";" + event_info + ";" + result_data
            )
        except Exception as e:
            print("Error getting fight stats, " + str(e))
            pass

        return total_fight_stats

    def _get_total_fight_stats(self, fight_links: Dict[str, List[str]]) -> str:
        total_stats = ""

        fight_count = len(fight_links)
        print(f"Scraping data for {fight_count} fights: ")
        print_progress(0, fight_count, prefix="Progress:", suffix="Complete")

        for index, (event, fights) in enumerate(fight_links.items()):
            event_soup = make_soup(event)
            event_info = self._get_event_info(event_soup)

            # Get data for each fight in the event in parallel.
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                futures = []
                for fight in fights:
                    futures.append(
                        executor.submit(
                            self._get_fight_stats_task,
                            fight=fight,
                            event_info=event_info,
                        )
                    )
                for future in concurrent.futures.as_completed(futures):
                    fight_stats = future.result()
                    if fight_stats != "":
                        if total_stats == "":
                            total_stats = fight_stats
                        else:
                            total_stats = total_stats + "\n" + fight_stats
                    print_progress(
                        index + 1, fight_count, prefix="Progress:", suffix="Complete"
                    )

        return total_stats

    def _get_fight_stats(self, fight_soup: BeautifulSoup) -> str:
        tables = fight_soup.findAll("tbody")
        # hard coded to grab totals and significant strike stats.
        # skips per round stats
        # i think we want per round stats.
        total_fight_data = [tables[0], tables[2]]
        fight_stats = []
        for table in total_fight_data:
            row = table.find("tr")
            stats = ""
            for data in row.findAll("td"):
                if stats == "":
                    stats = data.text
                else:
                    stats = stats + "," + data.text
            fight_stats.append(
                stats.replace("  ", "")
                .replace("\n\n", "")
                .replace("\n", ",")
                .replace(", ", ",")
                .replace(" ,", ",")
            )

        # hardcoded here to ignore first 3 cols of significant strikes table
        fight_stats[1] = ";".join(fight_stats[1].split(",")[6:])
        fight_stats[0] = ";".join(fight_stats[0].split(","))
        fight_stats = ";".join(fight_stats)
        return fight_stats

    def _get_fight_details(self, fight_soup: BeautifulSoup) -> str:
        columns = ""
        for div in fight_soup.findAll("div", {"class": "b-fight-details__content"}):
            for col in div.findAll("p", {"class": "b-fight-details__text"}):
                if columns == "":
                    columns = col.text
                else:
                    columns = columns + "," + (col.text)

        columns = (
            columns.replace("  ", "")
            .replace("\n\n\n\n", ",")
            .replace("\n", "")
            .replace(", ", ",")
            .replace(" ,", ",")
            .replace("Method: ", "")
            .replace("Round:", "")
            .replace("Time:", "")
            .replace("Time format:", "")
            .replace("Referee:", "")
        )

        fight_details = ";".join(columns.split(",")[:5])

        return fight_details

    def _get_event_info(self, event_soup: BeautifulSoup) -> str:
        event_info = ""
        for info in event_soup.findAll("li", {"class": "b-list__box-list-item"}):
            if event_info == "":
                event_info = info.text
            else:
                event_info = event_info + ";" + info.text

        event_info = ";".join(
            event_info.replace("Date:", "")
            .replace("Location:", "")
            .replace("Attendance:", "")
            .replace("\n", "")
            .replace("  ", "")
            .split(";")[:2]
        )

        return event_info

    def _get_fight_result_data(self, fight_soup: BeautifulSoup) -> str:
        winner = ""
        for div in fight_soup.findAll("div", {"class": "b-fight-details__person"}):
            if (
                div.find(
                    "i",
                    {
                        "class": "b-fight-details__person-status b-fight-details__person-status_style_green"
                    },
                )
                is not None
            ):
                winner = (
                    div.find("h3", {"class": "b-fight-details__person-name"})
                    .text.replace(" \n", "")
                    .replace("\n", "")
                )

        fight_type = (
            fight_soup.find("i", {"class": "b-fight-details__fight-title"})
            .text.replace("  ", "")
            .replace("\n", "")
        )

        return fight_type + ";" + winner
