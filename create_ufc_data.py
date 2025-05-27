#moved this to root, might make it explode, fix later.

import time
from src.ufctools.preprocess import Preprocessor
from src.ufctools.scrape_fight_data import FightDataScraper
from src.ufctools.scrape_fighter_details import FighterDetailsScraper

time_start = time.time()
print("Creating fight data \n")
fight_data_scraper = FightDataScraper()
fight_data_scraper.create_fight_data_csv()  # Scrapes raw ufc fight data from website
print(f'elapsed seconds = {(time.time() - time_start):.2f}')

time_start = time.time()
print("Creating fighter data \n")
fighter_details_scraper = FighterDetailsScraper()
fighter_details_scraper.create_fighter_data_csv()  # Scrapes raw ufc fighter data from website
print(f'elapsed seconds = {(time.time() - time_start):.2f}')

time_start = time.time()
print("Starting Preprocessing \n")
preprocessor = Preprocessor()
preprocessor.process_raw_data()  # Preprocesses the raw data and saves the csv files in data folder
print(f'elapsed seconds = {(time.time() - time_start):.3f}')
