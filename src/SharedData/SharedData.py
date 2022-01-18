from multiprocessing import Condition
from pathlib import Path
import pandas as pd
import logging

from SharedData.SharedDataFeeder import SharedDataFeeder
from SharedData.Metadata import Metadata

class SharedData:

    def __init__(self, config, database='Signals'):
        
        self.config = config
        self.logger = config.logger
    
        self.database = database

        # DATA DICTIONARY
        # SharedDataTimeSeries: data[feeder][period][tag] (date x symbols)
        # SharedDataFrame: data[feeder][period][date] (symbols x tags)
        self.data = {} 

        # Symbols collections metadata
        self.metadata = {}
        
        # DATASET
        path = Path(config.db_directory)
        datasetPath = path / database / 'DataSet.xlsx'
        if datasetPath.is_file():
            self.dataset = pd.read_excel(datasetPath, sheet_name='DATASET')
        else:
            self.logger.error('SharedData could not find dataset at %s' % (datasetPath))
            raise Exception('SharedData could not find dataset at %s' % (datasetPath))
        
    def __setitem__(self, feeder, value):
        self.data[feeder] = value
                
    def __getitem__(self, feeder):        
        if not feeder in self.data.keys():
            self.data[feeder] = SharedDataFeeder(self, feeder)
        return self.data[feeder]
                          
    def getSymbols(self, collection):        
        if not collection in self.metadata.keys():            
            md = Metadata(collection)
            self.metadata[collection] = md            
        return self.metadata[collection].static.index.values
    
    