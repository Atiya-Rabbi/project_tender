import os
import pandas as pd

def remove_prevfiles(zipfile=False, file_name=False) -> None:
    '''Remove any previous folder or files'''

    if file_name and os.path.exists(file_name):
        os.remove(file_name)
    
    if zipfile:
        for file in os.listdir(zipfile):
            if file.endswith(".gz"):
                dirs = os.path.join(zipfile, file)
                os.remove(dirs)
    
def load_csv(csv_filename=False, seprator = ','):
    '''Load CSV File return DataFrame'''

    if csv_filename and os.path.exists(csv_filename):
        df = pd.read_csv(csv_filename, sep=seprator, low_memory=False)
        return df
    return None
