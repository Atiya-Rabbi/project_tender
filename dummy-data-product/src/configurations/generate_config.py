import configparser
import os

class Configuration:
    def generate(self):
        script_dir = os.getcwd()
        csv_path   = '../../data/'

        raw_data      = os.path.join(script_dir,csv_path)+'opentender.csv'
        meta_data     = os.path.join(script_dir,csv_path)+'meta_data.json'
        clean_data    = os.path.join(script_dir,csv_path)+'clean_opentender.csv'
        geocoder_data = os.path.join(script_dir,csv_path)+'geo_opentender.csv'
        stnd_data     = os.path.join(script_dir,csv_path)+'standard_opentender.csv'

        # CREATE OBJECT
        config_file = configparser.ConfigParser()

        # ADD SECTION
        config_file.add_section("CSVFiles")
        # ADD SETTINGS TO SECTION
        config_file.set("CSVFiles", "raw", raw_data)
        config_file.set("CSVFiles", "clean", clean_data)
        config_file.set("CSVFiles", "geocoder", geocoder_data)
        config_file.set("CSVFiles", "standard", stnd_data)


        config_file["JSONFiles"]={
                "meta_data":meta_data,
            }

        config_file["URLs"]={
                "scrape_url": 'https://opentender.eu/ie/search/tender',
            }

        config_file["GZip"]={
                "gzip_folder": script_dir,
            }
        # SAVE CONFIG FILE
        with open(r"configurations.ini", 'w') as configfileObj:
            config_file.write(configfileObj)
            configfileObj.flush()
            configfileObj.close()

        print("Config file 'configurations.ini'")

        # PRINT FILE CONTENT
        # read_file = open("configurations.ini", "r")
        # content = read_file.read()
        # print("Content of the config file are:\n")
        # print(content)
        # read_file.flush()
        # read_file.close()