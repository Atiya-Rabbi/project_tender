import logging
logging.basicConfig(level=logging.INFO)
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import pandas as pd
import os,requests,time
from dependencies.utils.utils import remove_prevfiles, load_csv
from configurations.helper import read_config
from tqdm import tqdm
import csv


class GeoCoder:
    def __init__(self) -> None:
        config = read_config()
        self.clean_csv = config['CSVFiles']['clean']
        self.geocoder_data = config['CSVFiles']['geocoder']

    def geolocate(self):
        remove_prevfiles(file_name=self.geocoder_data)
        logging.info('Removed old GeoCoder csv data file')

        # opening the CSV file 
        with open(self.clean_csv, mode ='r')as file: 
            # reading the CSV file 
            csvFile = csv.DictReader(file)

            with open(self.geocoder_data, mode='w') as csv_file:
                counter = 0
                # displaying the contents of the CSV file 
                for data in csvFile: 
                    if counter==0:
                        fieldnames = {**data, **{'address':'', 'location':'', 'map_coordinates':'', 'country_name': ''}}
                        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                        writer.writeheader()
                    try:
                        data['address'] = data['buyer_city'] + ',' + data['buyer_country']
                    
                        geocode_result = self.get_nominatim(data['address'])
                        data['location'] = geocode_result['location']
                        data['map_coordinates'] = [geocode_result['latitude'],geocode_result['longitude']]
                        data['country_name'] = geocode_result['country']
                        writer.writerow(data)
                        logging.info("Geocoded: {}: {}".format(data['address'], geocode_result['status']))
                    except Exception as e:
                        logging.exception(e)
                        logging.error("Major error with {}".format(data['address']))
                        logging.error("Skipping!")

                    counter+=1
                    #limit set to 100 for trial task
                    if counter%100==0:
                        logging.info("Completed {} addresses".format(counter))
                        break
        
    def get_nominatim(self, address):
        output = {
            'status'   : 'Error',
            'location'  : None,
            'latitude' : None,
            'longitude': None,
            'country'  : None
        }
        geolocator = Nominatim(user_agent="geocoder")
        
        location = geolocator.geocode(address)
        
        if location:
            output['location']   =location.address
            output['latitude']  = location.latitude
            output['longitude'] = location.longitude
            output['country']   = location.raw['display_name'].split('/')[-1]
            output['status']    = 'OK'

        return output

    