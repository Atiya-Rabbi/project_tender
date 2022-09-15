import logging
logging.basicConfig(level=logging.INFO)
import pandas as pd
import os
from dependencies.utils.utils import remove_prevfiles, load_csv
from configurations.helper import read_config

class CleanData:
  def __init__(self) -> None:
    config = read_config()
    self.data_csv  = config['CSVFiles']['raw']
    self.clean_csv = config['CSVFiles']['clean']

  def clean_csv_data(self) -> None:
    try:
      remove_prevfiles(file_name=self.clean_csv)
      logging.info('Removed old clean csv data file')
      #read csv data
      csv_data =  load_csv(csv_filename=self.data_csv, seprator=';')
      logging.info('RAW CSV data--> {}'.format(self.data_csv))
      #Drop only if ALL columns are NaN
      csv_dropped = csv_data.dropna(how = 'all')
      logging.info('Dropped missing rows')
      #dropping the duplicates
      csv_dropped = csv_dropped.drop_duplicates()
      logging.info('Dropped duplicates')
      #save clean data
      csv_dropped.to_csv(self.clean_csv, index=False)
      logging.info('CLEAN CSV data--> {}'.format(self.clean_csv))
    except Exception as e:
      logging.exception(e)
