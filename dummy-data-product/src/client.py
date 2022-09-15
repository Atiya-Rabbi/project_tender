import dotenv
import logging

from datetime import datetime

# Importing scraping and data processing modules
from dependencies.scraping.scraper import ProjectsTenders
from dependencies.cleaning.cleaning import CleanData
from dependencies.geocoding.geocoder import GeoCoder
from dependencies.standardization.standardizer import Standardizer

dotenv.load_dotenv(".env")
logging.basicConfig(level=logging.INFO)


# In each step create an object of the class, initialize the class with 
# required configuration and call the run method 
def step_1():
    ProjectsTenders().scrape_metadata()
    logging.info("Scraped Metadata")


def step_2():
    ProjectsTenders().run_scraper()
    logging.info("Scraped Main Data")


def step_3():
    CleanData().clean_csv_data()
    logging.info("Cleaned Main Data")


def step_4():
    GeoCoder().geolocate()
    logging.info("Geocoded Cleaned Data")


def step_5():
    Standardizer().standardize_data()
    logging.info("Standardized Geocoded Data")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--step", help="step to be choosen for execution")

    args = parser.parse_args()

    eval(f"step_{args.step}()")

    logging.info(
        {
            "last_executed": str(datetime.now()),
            "status": "Pipeline executed successfully",
        }
    )
