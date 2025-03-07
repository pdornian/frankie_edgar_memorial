import pickle
from typing import Dict, List, Tuple

from src.createdata.utils import make_soup, print_progress

from src.createdata.data_files_path import (  # isort:skip
    FIGHT_LINKS_PICKLE,
    PAST_EVENT_LINKS_PICKLE,
)


class UFCLinks:
    def __init__(
        self, all_events_url="http://ufcstats.com/statistics/events/completed?page=all"
    ):
        self.all_events_url = all_events_url
        self.PAST_EVENT_LINKS_PICKLE_PATH = PAST_EVENT_LINKS_PICKLE
        self.FIGHT_LINKS_PICKLE_PATH = FIGHT_LINKS_PICKLE
        self.new_event_links, self.all_event_links = self._get_updated_event_links()

    def _get_updated_event_links(self) -> Tuple[List[str], List[str]]:
        all_event_links = []
        print("Getting all event URLs")
        soup = make_soup(self.all_events_url)
        # could pull title text too
        for link in soup.findAll("td", {"class": "b-statistics__table-col"}):
            for href in link.findAll("a"):
                foo = href.get("href")
                all_event_links.append(foo)

        if not self.PAST_EVENT_LINKS_PICKLE_PATH.exists():
            # if no past event links are present, set empty list
            past_event_links = []
        else:
            # get past event links
            with open(self.PAST_EVENT_LINKS_PICKLE_PATH.as_posix(), "rb") as pickle_in:
                past_event_links = pickle.load(pickle_in)

        # set new events to be all events not in past event link file.
        new_event_links = list(set(all_event_links) - set(past_event_links))

        # dump all_event_links as PAST_EVENT_LINKS
        with open(self.PAST_EVENT_LINKS_PICKLE_PATH.as_posix(), "wb") as f:
            pickle.dump(all_event_links, f)

        return new_event_links, all_event_links

    def get_fight_links(self) -> tuple[Dict, Dict]:
        def get_fight_links_from_events(event_links: List[str]) -> Dict[str, List[str]]:
            fight_links = {}

            num_events = len(event_links)
            print("Scraping fight links: ")
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
                fight_links[link] = event_fights

                print_progress(
                    index + 1, num_events, prefix="Progress:", suffix="Complete"
                )

            return fight_links

        new_fight_links = {}
        # if event/fight link pickle file exists.
        if self.FIGHT_LINKS_PICKLE_PATH.exists():
            print(
                f"Loading previous fight data URLs from {self.FIGHT_LINKS_PICKLE_PATH}"
            )
            # load prev events and links
            with open(self.FIGHT_LINKS_PICKLE_PATH.as_posix(), "rb") as pickle_in:
                prev_fight_links = pickle.load(pickle_in)

            # if no new event links
            if not self.new_event_links:
                print("No new event URLs.")
                # then prev events are all events
                all_fight_links = prev_fight_links
            else:
                # get new fight URLs
                print("Getting URLs to fights from new events.")
                new_fight_links = get_fight_links_from_events(self.new_event_links)
                # add to all events
                all_fight_links = new_fight_links | prev_fight_links
                # update file
                print(f"Updating {self.FIGHT_LINKS_PICKLE_PATH.as_posix()}")
                with open(self.FIGHT_LINKS_PICKLE_PATH.as_posix(), "wb") as f:
                    pickle.dump(all_fight_links, f)
        else:
            # no event and fight link file exists
            print("No fight data URLs saved. Retrieving all URLs.")
            all_fight_links = get_fight_links_from_events(self.all_event_links)
            # all events are new events
            new_fight_links = all_fight_links
            print(f"Writing fight URLs to {self.FIGHT_LINKS_PICKLE_PATH.as_posix()}")
            with open(self.FIGHT_LINKS_PICKLE_PATH.as_posix(), "wb") as f:
                pickle.dump(all_fight_links, f)

        return new_fight_links, all_fight_links
